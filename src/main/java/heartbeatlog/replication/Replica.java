package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.core.Role;
import heartbeatlog.simulation.Message;
import heartbeatlog.simulation.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One copy of the telemetry log - a leader or a follower, decided by
 * appointment. This is the replication half of the protocol: how writes
 * spread from the leader to followers, and how the cluster decides which
 * entries are safely "committed".
 *
 * <pre>
 *   ClientAppend("hr=61")
 *        │
 *        ▼
 *   ┌─────────┐  FetchRequest(fromOffset)   ┌───────────┐
 *   │ LEADER  │ ◀────────────────────────── │ FOLLOWER  │
 *   │ log:0..9│    "= I have everything     │ log: 0..6 │
 *   │  HW=7   │       below fromOffset"     │  HW = 6   │
 *   │         │ ──────────────────────────▶ │           │
 *   └─────────┘  FetchResponse(entries,     └───────────┘
 *                 leader HW)                 appends, adopts HW,
 *                                            immediately fetches again
 *
 *   High watermark (HW) = how many entries EVERY in-sync replica has.
 *   Only entries below the HW are "committed" - promised safe to clients.
 * </pre>
 *
 * ELI5: the leader keeps the class notebook; followers copy it page by
 * page, and each time a follower asks "what comes after page 6?" it is also
 * proving it HAS the first 6 pages. The teacher calls a page "safe" only
 * once every copier has it - that count is the high watermark.
 *
 * Followers pull continuously: each response triggers the next request.
 * When a follower has copied everything and knows the latest watermark,
 * there is nothing useful to tell it - so instead of replying "nothing new"
 * (which would just make it ask again, forever), the leader writes its name
 * on a waiting list and stays silent. The moment there IS news - a new
 * entry or a watermark move - everyone on the waiting list gets a response.
 * This is why an idle cluster goes quiet instead of ping-ponging empty
 * fetches. (Kafka does the same thing; it calls it long-polling.)
 *
 * Epoch discipline on every message (the fencing rules): older epoch =>
 * ignore it; newer epoch => adopt it, and step down if leading. Entries are
 * stamped with the epoch they were appended under - the raw material the
 * KIP-101 truncation fix will reason about in later commits.
 */
public final class Replica implements Node {

    private final int id;
    private final CommitLedger ledger;

    private Role role = Role.FOLLOWER;
    private long epoch = 0;
    private int leaderId = -1;
    private final List<Entry> log = new ArrayList<>();
    private long highWatermark = 0;

    // Leader-only bookkeeping. TreeMaps, not HashMaps: iteration order is
    // part of message-send order, and determinism-on-any-machine forbids
    // leaving that to hash-table internals.
    private final TreeMap<Integer, Long> followerLogEnds = new TreeMap<>();
    // The waiting list: followers that are fully caught up, remembered by
    // (follower id -> the offset they are waiting at) so the leader can
    // answer them the moment there is news instead of replying "nothing
    // new" in an endless loop.
    private final TreeMap<Integer, Long> waitingFollowers = new TreeMap<>();
    private List<Integer> replicaGroup = List.of();

    public Replica(int id, CommitLedger ledger) {
        this.id = id;
        this.ledger = ledger;
    }

    @Override
    public List<Message> step(long now, Object event) {
        return switch (event) {
            case LeaderAppointment appointment -> onAppointment(appointment);
            case ClientAppend append -> onClientAppend(append);
            case FetchRequest fetch -> onFetchRequest(fetch);
            case FetchResponse response -> onFetchResponse(response);
            default -> throw new IllegalArgumentException("replica " + id + ": unknown event " + event);
        };
    }

    /**
     * A new leadership decree arrived: adopt the new epoch and take the
     * assigned role. A fresh leader starts with empty bookkeeping (it will
     * relearn follower positions from their fetches); a fresh follower
     * immediately starts pulling the leader's log.
     */
    private List<Message> onAppointment(LeaderAppointment appointment) {
        // Fencing: a decree from an older (or repeated) leadership changes
        // nothing. Real appointments start at epoch 1; replicas begin at 0.
        if (appointment.epoch() <= epoch) {
            return List.of();
        }
        epoch = appointment.epoch();
        leaderId = appointment.leaderId();
        replicaGroup = appointment.replicas();
        followerLogEnds.clear();
        waitingFollowers.clear();

        if (leaderId == id) {
            role = Role.LEADER;
            return List.of();
        }
        role = Role.FOLLOWER;
        // Start the pull chain: ask for everything we don't have yet.
        return List.of(nextFetch());
    }

    /**
     * A client write arrived: the leader stamps it with the current epoch,
     * appends it to its own log, and answers every follower on the waiting
     * list - they were waiting precisely for new data like this.
     */
    private List<Message> onClientAppend(ClientAppend append) {
        if (role != Role.LEADER) {
            // A real system would redirect the client to the leader;
            // out of scope here - the schedule always aims at the leader.
            return List.of();
        }
        log.add(new Entry(epoch, log.size(), append.payload()));
        return answerWaitingFollowers();
    }

    /**
     * LEADER side: a follower is asking for entries from fromOffset onward.
     * Three jobs in order: (1) treat the request as the follower's ack and
     * note how much log it now holds, (2) recompute the high watermark with
     * that new knowledge, (3) either answer the follower right away - it is
     * missing entries or watermark news - or, if it is fully current, put it
     * on the waiting list until there is something new to say.
     */
    private List<Message> onFetchRequest(FetchRequest fetch) {
        if (fetch.epoch() < epoch || role != Role.LEADER) {
            // Stale leadership or we aren't the leader (yet) - a real system
            // answers with an error; here the failover schedule re-appoints
            // and restarts the pull chain, so silence is safe.
            return List.of();
        }
        // The fetch IS the ack: fromOffset proves the follower holds
        // everything below it.
        followerLogEnds.put(fetch.followerId(), fetch.fromOffset());

        List<Message> out = new ArrayList<>(advanceHighWatermark());
        if (fetch.fromOffset() < log.size() || fetch.followerHighWatermark() < highWatermark) {
            // The follower is missing something - entries, watermark news,
            // or both. The second clause closes a real race: a fetch that
            // crossed a watermark advance in flight arrives looking fully
            // caught up on entries, but its sender is still holding the old
            // watermark and would otherwise wait forever without hearing
            // the new one.
            out.add(fetchResponseFor(fetch.followerId(), fetch.fromOffset()));
        } else {
            // Truly current on both counts: goes on the waiting list until
            // there is data or watermark news.
            waitingFollowers.put(fetch.followerId(), fetch.fromOffset());
        }
        return out;
    }

    /**
     * FOLLOWER side: the leader answered our fetch. Store the new entries
     * (they must continue exactly where our log ends), adopt the leader's
     * high watermark (capped at what we actually hold - we can't consider
     * committed what we haven't stored), and immediately fetch again, which
     * also acks what we just stored.
     */
    private List<Message> onFetchResponse(FetchResponse response) {
        if (response.epoch() < epoch || role != Role.FOLLOWER) {
            return List.of();
        }
        if (response.fromOffset() != log.size()) {
            throw new IllegalStateException("replica " + id + ": batch starts at "
                    + response.fromOffset() + " but log ends at " + log.size());
        }
        log.addAll(response.entries());
        highWatermark = Math.min(response.leaderHighWatermark(), log.size());
        return List.of(nextFetch());
    }

    /**
     * HW = the smallest log end across the whole replica group (leader
     * included). When it advances, every newly covered entry becomes a
     * commitment - recorded in the ledger - and every follower on the
     * waiting list is told the news.
     */
    private List<Message> advanceHighWatermark() {
        long minimum = log.size();
        for (int member : replicaGroup) {
            if (member != id) {
                minimum = Math.min(minimum, followerLogEnds.getOrDefault(member, 0L));
            }
        }
        if (minimum <= highWatermark) {
            return List.of();
        }
        for (long offset = highWatermark; offset < minimum; offset++) {
            ledger.recordCommitted(log.get((int) offset));
        }
        highWatermark = minimum;
        return answerWaitingFollowers();
    }

    /**
     * Empties the waiting list: every follower that was waiting for news
     * gets a response carrying whatever is new since it started waiting -
     * fresh entries, a moved watermark, or both.
     */
    private List<Message> answerWaitingFollowers() {
        List<Message> out = new ArrayList<>();
        for (Map.Entry<Integer, Long> waiting : waitingFollowers.entrySet()) {
            out.add(fetchResponseFor(waiting.getKey(), waiting.getValue()));
        }
        waitingFollowers.clear();
        return out;
    }

    /**
     * Builds this follower's next pull: "send me everything from where my
     * log ends" plus the watermark it currently believes, so the leader can
     * tell whether it is fully current or owed news.
     */
    private Message nextFetch() {
        return new Message(id, leaderId, new FetchRequest(epoch, id, log.size(), highWatermark));
    }

    /**
     * Builds the leader's answer to one follower: all entries the leader
     * holds from fromOffset onward (possibly none) plus the current high
     * watermark.
     */
    private Message fetchResponseFor(int follower, long fromOffset) {
        List<Entry> batch = List.copyOf(log.subList((int) fromOffset, log.size()));
        return new Message(id, follower, new FetchResponse(epoch, fromOffset, batch, highWatermark));
    }

    // Test-visible state

    public List<Entry> log() {
        return List.copyOf(log);
    }

    public long highWatermark() {
        return highWatermark;
    }

    public Role role() {
        return role;
    }

    public long epoch() {
        return epoch;
    }
}

package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.core.Role;
import heartbeatlog.simulation.Message;
import heartbeatlog.simulation.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

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
 *   High watermark (HW) = how many entries every IN-SYNC replica has.
 *   Only entries below the HW are "committed" - promised safe to clients.
 * </pre>
 *
 * ELI5: the leader keeps the class notebook; followers copy it page by
 * page, and each time a follower asks "what comes after page 6?" it is also
 * proving it HAS the first 6 pages. The teacher calls a page "safe" only
 * once every active copier has it - that count is the high watermark.
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
 * THE IN-SYNC ROSTER: the leader cannot wait forever for a dead follower,
 * or the watermark would freeze and nothing would ever commit again. So it
 * keeps a roster of followers that are keeping up. A follower that hasn't
 * been caught up for {@code inSyncLagLimit} ticks is dropped from the
 * roster (the watermark then advances without it); a dropped follower that
 * catches back up to the watermark is re-admitted. Safety valve: if the
 * roster shrinks below {@link #MIN_IN_SYNC_REPLICAS}, the leader REFUSES
 * new writes - the cluster goes unavailable rather than making promises
 * only one machine holds. (Kafka's names for these three knobs:
 * replica.lag.time.max.ms, the ISR, and min.insync.replicas with acks=all.)
 *
 * CRASH AND RECOVERY: a crash wipes everything volatile (role, roster,
 * waiting list); the log survives on disk. On recovery the replica applies
 * its {@link TruncationRule} - under HIGH_WATERMARK it tears the log back
 * to the last watermark it personally knew, which can be older than what
 * the cluster committed. That gap is deliberate: it is the raw material of
 * the KIP-101 red test in the next commits.
 *
 * Epoch discipline on every message (the fencing rules): older epoch =>
 * ignore it; newer epoch => adopt it, and step down if leading. Entries are
 * stamped with the epoch they were appended under.
 */
public final class Replica implements Node {

    /**
     * The durability floor: a write is only accepted while at least this
     * many replicas (leader included) are in sync, so every commitment is
     * held by at least 2 of 3 machines. Below the floor the cluster chooses
     * unavailability over risky promises.
     */
    public static final int MIN_IN_SYNC_REPLICAS = 2;

    private final int id;
    private final CommitLedger ledger;
    private final TruncationRule truncationRule;
    private final long inSyncLagLimit;

    // Durable state - survives a crash (simulated stable storage).
    private final List<Entry> log = new ArrayList<>();
    private long epoch = 0;
    private long highWatermark = 0;
    private int leaderId = -1;

    // Volatile state - wiped by a crash.
    private Role role = Role.FOLLOWER;
    private boolean crashed = false;
    private int refusedAppends = 0;

    // Leader-only bookkeeping (volatile). TreeMaps/TreeSets, not hash
    // variants: iteration order is part of message-send order, and
    // determinism-on-any-machine forbids leaving that to hash internals.
    private final TreeMap<Integer, Long> followerLogEnds = new TreeMap<>();
    // The waiting list: followers that are fully caught up, remembered by
    // (follower id -> the offset they are waiting at) so the leader can
    // answer them the moment there is news instead of replying "nothing
    // new" in an endless loop.
    private final TreeMap<Integer, Long> waitingFollowers = new TreeMap<>();
    // The in-sync roster, plus when each member was last fully caught up.
    private final TreeSet<Integer> inSyncReplicas = new TreeSet<>();
    private final TreeMap<Integer, Long> lastCaughtUpAt = new TreeMap<>();
    // Where the log ended (and when) at each follower's PREVIOUS fetch -
    // needed to recognize a healthy follower chasing a moving target: with
    // a continuous write stream there is always one entry in flight, so
    // "fromOffset >= log end right now" alone would never trigger and the
    // roster would evict followers that are keeping up fine. (Kafka's
    // Replica.updateFetchState has this same two-part rule.)
    private final TreeMap<Integer, Long> previousFetchLogEnd = new TreeMap<>();
    private final TreeMap<Integer, Long> previousFetchAt = new TreeMap<>();
    private List<Integer> replicaGroup = List.of();

    public Replica(int id, CommitLedger ledger, TruncationRule truncationRule, long inSyncLagLimit) {
        this.id = id;
        this.ledger = ledger;
        this.truncationRule = truncationRule;
        this.inSyncLagLimit = inSyncLagLimit;
    }

    @Override
    public List<Message> step(long now, Object event) {
        if (crashed) {
            // Powered off: deaf to everything except the power switch.
            return event instanceof Recover ? onRecover() : List.of();
        }
        return switch (event) {
            case LeaderAppointment appointment -> onAppointment(now, appointment);
            case ClientAppend append -> onClientAppend(now, append);
            case FetchRequest fetch -> onFetchRequest(now, fetch);
            case FetchResponse response -> onFetchResponse(response);
            case EpochEndRequest request -> onEpochEndRequest(request);
            case EpochEndResponse response -> onEpochEndResponse(response);
            case Crash crash -> onCrash();
            case Recover recover -> List.of(); // not crashed - nothing to recover from
            default -> throw new IllegalArgumentException("replica " + id + ": unknown event " + event);
        };
    }

    /**
     * A new leadership decree arrived: adopt the new epoch and take the
     * assigned role. A fresh leader starts everyone on its roster with a
     * full grace period (it will learn real positions from their fetches);
     * a fresh follower immediately starts pulling the leader's log.
     */
    private List<Message> onAppointment(long now, LeaderAppointment appointment) {
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
        inSyncReplicas.clear();
        lastCaughtUpAt.clear();
        previousFetchLogEnd.clear();
        previousFetchAt.clear();

        if (leaderId == id) {
            role = Role.LEADER;
            for (int member : replicaGroup) {
                inSyncReplicas.add(member);
                lastCaughtUpAt.put(member, now);
            }
            return List.of();
        }
        role = Role.FOLLOWER;
        if (truncationRule == TruncationRule.HIGH_WATERMARK && log.size() > highWatermark) {
            // BUGGY PRE-KIP-101 RULE, leadership-change half: before
            // following a new leader, tear the log back to our own
            // watermark. The torn entries may be COMMITTED (our watermark
            // knowledge lags the cluster's by up to one round trip) - this
            // tear plus a fast leadership bounce is the KIP-101 data-loss
            // recipe the red test stages.
            log.subList((int) highWatermark, log.size()).clear();
        }
        if (truncationRule == TruncationRule.EPOCH_BOUNDARY && !log.isEmpty()) {
            // THE FIX: no guessing. Ask the new leader where our last
            // epoch ends in ITS log and truncate exactly there. Committed
            // entries survive; only divergent scribble is discarded.
            return List.of(reconcileWithLeader());
        }
        // Start the pull chain: ask for everything we don't have yet.
        return List.of(nextFetch());
    }

    /**
     * A client write arrived. If enough replicas are in sync, the leader
     * stamps it with the current epoch, appends it, and answers everyone on
     * the waiting list. If not, the write is REFUSED - unavailability over
     * promises only one machine would hold.
     */
    private List<Message> onClientAppend(long now, ClientAppend append) {
        if (role != Role.LEADER) {
            // A real system would redirect the client to the leader;
            // out of scope here - the schedule always aims at the leader.
            return List.of();
        }
        List<Message> out = new ArrayList<>(reviewInSyncRoster(now));
        if (inSyncReplicas.size() < MIN_IN_SYNC_REPLICAS) {
            refusedAppends++;
            return out;
        }
        log.add(new Entry(epoch, log.size(), append.payload()));
        out.addAll(answerWaitingFollowers(now));
        return out;
    }

    /**
     * LEADER side: a follower is asking for entries from fromOffset onward.
     * Jobs in order: (1) treat the request as the follower's ack and note
     * how much log it now holds, (2) update the in-sync roster - this fetch
     * may prove a dropped follower has caught back up, (3) recompute the
     * high watermark with that new knowledge, (4) either answer the
     * follower right away - it is missing entries or watermark news - or,
     * if it is fully current, put it on the waiting list until there is
     * something new to say.
     */
    private List<Message> onFetchRequest(long now, FetchRequest fetch) {
        if (fetch.epoch() < epoch) {
            // Stale leadership: drop. (Fencing rule 1.)
            return List.of();
        }
        if (fetch.epoch() > epoch || role != Role.LEADER) {
            // Either the requester knows a NEWER leadership than we do, or
            // it believes we lead one we haven't heard of yet - in both
            // cases the decree is still in flight somewhere. Serving with a
            // stale epoch would get the response (rightly) dropped. Requeue
            // the request to ourselves through the network (1-4 ticks) and
            // re-handle it once the decree has had a chance to land.
            // Bounded: the controller's decree always arrives in these
            // schedules; without it the runaway backstop would flag the
            // loop loudly rather than losing the request silently.
            return List.of(new Message(id, id, fetch));
        }
        // The fetch IS the ack: fromOffset proves the follower holds
        // everything below it.
        int follower = fetch.followerId();
        followerLogEnds.put(follower, fetch.fromOffset());
        if (fetch.fromOffset() >= log.size()) {
            // Fully caught up right now.
            lastCaughtUpAt.put(follower, now);
        } else if (fetch.fromOffset() >= previousFetchLogEnd.getOrDefault(follower, Long.MAX_VALUE)) {
            // Caught up to where the log ended when it LAST asked - it is
            // keeping pace with a moving target, which counts as of then.
            // max(): this credit dates from the previous fetch and must
            // never rewind a FRESHER stamp (e.g. from time spent on the
            // waiting list, which counts as caught up continuously).
            lastCaughtUpAt.merge(follower, previousFetchAt.getOrDefault(follower, now), Math::max);
        }
        previousFetchLogEnd.put(follower, (long) log.size());
        previousFetchAt.put(follower, now);
        // Re-admission: a dropped follower that has caught back up to the
        // watermark rejoins the roster and counts for commitment again.
        if (!inSyncReplicas.contains(fetch.followerId()) && fetch.fromOffset() >= highWatermark) {
            inSyncReplicas.add(fetch.followerId());
            lastCaughtUpAt.put(fetch.followerId(), now);
        }

        List<Message> out = new ArrayList<>(reviewInSyncRoster(now));
        out.addAll(advanceHighWatermark(now));
        if (fetch.fromOffset() > log.size()) {
            // The follower's log is LONGER than ours - it followed a
            // previous leader further than we ever got. A reset response
            // (empty, fromOffset = our log end) tells it where our log
            // ends; under the buggy rule the follower then cuts itself
            // down to match (pre-KIP-101 Kafka: OffsetOutOfRangeException
            // handling). The epoch-based fix will replace this with a
            // precise boundary lookup.
            out.add(new Message(id, fetch.followerId(),
                    new FetchResponse(epoch, log.size(), List.of(), highWatermark)));
            return out;
        }
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
        if (response.fromOffset() < log.size()) {
            if (truncationRule == TruncationRule.EPOCH_BOUNDARY) {
                // Under the fix a batch can never legitimately start below
                // our log end - if it does, our picture of the leader is
                // stale. Reconcile properly instead of conforming blindly.
                return List.of(reconcileWithLeader());
            }
            // THE DESTRUCTION MOMENT of the buggy rule: the new leader's
            // log is shorter than ours, and we conform by cutting ours
            // down to match. If the cut range held committed entries, they
            // are now gone from this replica - and if every other holder
            // already did the same, gone from the world. The commit ledger
            // still remembers the promise; the red test catches the lie.
            log.subList((int) response.fromOffset(), log.size()).clear();
        } else if (response.fromOffset() > log.size()) {
            throw new IllegalStateException("replica " + id + ": batch starts at "
                    + response.fromOffset() + " but log ends at " + log.size());
        }
        log.addAll(response.entries());
        highWatermark = Math.min(response.leaderHighWatermark(), log.size());
        return List.of(nextFetch());
    }

    /**
     * Power off. Everything volatile is gone; the log, epoch, watermark,
     * and last known leader survive on simulated stable storage.
     */
    private List<Message> onCrash() {
        crashed = true;
        role = Role.FOLLOWER;
        followerLogEnds.clear();
        waitingFollowers.clear();
        inSyncReplicas.clear();
        lastCaughtUpAt.clear();
        previousFetchLogEnd.clear();
        previousFetchAt.clear();
        return List.of();
    }

    /**
     * Power back on: apply the truncation rule, then resume life as a
     * follower of the last leader we knew. Under HIGH_WATERMARK the log is
     * torn back to the last watermark THIS replica knew - possibly older
     * than what the cluster committed. The tearing itself is safe as long
     * as the replica refetches before ever leading; the KIP-101 red test
     * will make it lead first.
     */
    private List<Message> onRecover() {
        crashed = false;
        role = Role.FOLLOWER;
        if (truncationRule == TruncationRule.HIGH_WATERMARK && log.size() > highWatermark) {
            log.subList((int) highWatermark, log.size()).clear();
        }
        if (leaderId != -1 && leaderId != id) {
            if (truncationRule == TruncationRule.EPOCH_BOUNDARY && !log.isEmpty()) {
                // THE FIX, restart half: nothing was torn on boot - ask the
                // leader where our last epoch ends and cut exactly there.
                return List.of(reconcileWithLeader());
            }
            return List.of(nextFetch());
        }
        // We led before crashing (or never heard of a leader): wait for the
        // controller's next decree.
        return List.of();
    }

    /**
     * Drops from the roster every follower that hasn't been caught up
     * within the lag limit. The watermark then advances without the
     * laggard - the cluster keeps committing while a machine is down,
     * which is the entire point of having replicas.
     */
    private List<Message> reviewInSyncRoster(long now) {
        boolean changed = false;
        for (int member : List.copyOf(inSyncReplicas)) {
            if (member == id) {
                continue;
            }
            if (waitingFollowers.containsKey(member)) {
                // On the waiting list = fully caught up RIGHT NOW, by
                // definition - it is silent because there is nothing to
                // say, not because it is behind. Keep its clock fresh, or
                // a long quiet stretch would read as lag and get a
                // perfectly healthy follower evicted.
                lastCaughtUpAt.put(member, now);
                continue;
            }
            boolean laggingTooLong = now - lastCaughtUpAt.getOrDefault(member, 0L) > inSyncLagLimit
                    && followerLogEnds.getOrDefault(member, 0L) < log.size();
            if (laggingTooLong) {
                inSyncReplicas.remove(member);
                waitingFollowers.remove(member);
                changed = true;
            }
        }
        // Removing a laggard can be exactly what unblocks the watermark.
        return changed ? advanceHighWatermark(now) : List.of();
    }

    /**
     * HW = the smallest log end across the in-sync roster (leader
     * included). When it advances, every newly covered entry becomes a
     * commitment - recorded in the ledger - and every follower on the
     * waiting list is told the news.
     */
    private List<Message> advanceHighWatermark(long now) {
        long minimum = log.size();
        for (int member : inSyncReplicas) {
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
        return answerWaitingFollowers(now);
    }

    /**
     * LEADER side of reconciliation: answer "where does epoch E end in
     * your log?" Same fencing and same decree-in-flight requeue as fetches.
     */
    private List<Message> onEpochEndRequest(EpochEndRequest request) {
        if (request.epoch() < epoch) {
            return List.of();
        }
        if (request.epoch() > epoch || role != Role.LEADER) {
            // Decree in flight (ours or theirs) - requeue, same as fetches.
            return List.of(new Message(id, id, request));
        }
        long endOffset = endOffsetForEpoch(log, request.queriedEpoch());
        return List.of(new Message(id, request.followerId(),
                new EpochEndResponse(epoch, request.queriedEpoch(), endOffset)));
    }

    /**
     * FOLLOWER side of reconciliation: cut the log exactly at the boundary
     * the leader named - everything below is shared history, everything
     * above is divergent scribble from a dead leadership - then resume
     * fetching from the cut.
     */
    private List<Message> onEpochEndResponse(EpochEndResponse response) {
        if (response.epoch() < epoch || role != Role.FOLLOWER) {
            return List.of();
        }
        long cutAt = Math.min(log.size(), response.endOffset());
        if (cutAt < log.size()) {
            log.subList((int) cutAt, log.size()).clear();
        }
        highWatermark = Math.min(highWatermark, log.size());
        return List.of(nextFetch());
    }

    /** Builds this follower's reconciliation question about its last epoch. */
    private Message reconcileWithLeader() {
        long lastEpochInLog = log.get(log.size() - 1).epoch();
        return new Message(id, leaderId, new EpochEndRequest(epoch, id, lastEpochInLog));
    }

    /**
     * Where does {@code queriedEpoch} end in this log? The offset of the
     * first entry stamped with a LARGER epoch - or the log end if no larger
     * epoch exists. Pure function of the log, so the edge cases (epoch
     * absent entirely, epoch older than the whole log, empty log) are unit
     * tested directly.
     */
    static long endOffsetForEpoch(List<Entry> log, long queriedEpoch) {
        for (Entry entry : log) {
            if (entry.epoch() > queriedEpoch) {
                return entry.offset();
            }
        }
        return log.size();
    }

    /**
     * Empties the waiting list: every follower that was waiting for news
     * gets a response carrying whatever is new since it started waiting -
     * fresh entries, a moved watermark, or both.
     */
    private List<Message> answerWaitingFollowers(long now) {
        List<Message> out = new ArrayList<>();
        for (Map.Entry<Integer, Long> waiting : waitingFollowers.entrySet()) {
            // They were fully current until this very moment - stamp it, so
            // the round-trip of the answer we are about to send does not
            // read as lag.
            lastCaughtUpAt.put(waiting.getKey(), now);
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

    public boolean crashed() {
        return crashed;
    }

    public List<Integer> inSyncReplicas() {
        return List.copyOf(inSyncReplicas);
    }

    public int refusedAppends() {
        return refusedAppends;
    }
}

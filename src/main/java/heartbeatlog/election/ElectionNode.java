package heartbeatlog.election;

import heartbeatlog.core.Role;
import heartbeatlog.simulation.Crash;
import heartbeatlog.simulation.Message;
import heartbeatlog.simulation.Node;
import heartbeatlog.simulation.Recover;

import java.util.ArrayList;
import java.util.List;

/**
 * One participant in a quorum-vote election - the consensus half of the
 * take-home prompt, in code. This is the piece the replication path's
 * ControllerStub stands in for: HOW a cluster grants leadership (and with
 * it, a fresh monotonic epoch) safely, with no central authority.
 *
 * <pre>
 *   ElectionTimeout                      VoteRequest(term, fingerprint)
 *        │                            ─────────────────────────────────▶
 *        ▼                                                    each voter:
 *   CANDIDATE: term++, vote for self,                 1. newer term? adopt
 *   ask everyone                                      2. already voted this
 *        ▲                                               term? refuse
 *        │        VoteReply(granted?)                 3. candidate's log
 *        └──────────────────────────                     behind mine? refuse
 *   majority of grants → LEADER                       4. else: RECORD vote
 *   (term = the granted epoch)                           durably, THEN reply
 * </pre>
 *
 * ELI5: to become class president you must convince most of the class,
 * each classmate votes at most once per round (and writes the vote in pen,
 * so fainting doesn't let them vote twice), and nobody votes for a
 * candidate whose notebook is missing pages. Majorities overlap, so two
 * presidents in one round is impossible - and the winner always holds
 * every page the class agreed on.
 *
 * The two safety rules this module exists to demonstrate:
 * - UP-TO-DATE CHECK (Raft §5.4.1): voters refuse candidates whose log
 *   fingerprint is behind their own. Any winning majority overlaps every
 *   commit majority, so the winner provably holds all committed entries.
 * - DURABLE VOTES (Raft Figure 2): currentTerm and votedFor are written to
 *   stable storage BEFORE the reply is sent. A voter that crashes and
 *   recovers remembers its vote - without this, one voter could vote twice
 *   in a term and two leaders could win the same epoch.
 *
 * Deliberately NOT here (scope, stated in the README): heartbeats, a
 * failure detector (timeouts are scripted events), log replication or
 * catch-up - this module grants epochs; the replication package moves data.
 */
public final class ElectionNode implements Node {

    private final int id;
    private final List<Integer> everyone; // all node ids, self included
    private final LogPosition logPosition;

    // Durable state - survives a crash, exactly like the replica's log.
    // Written BEFORE any reply leaves the node (Raft Figure 2's rule).
    private long currentTerm = 0;
    private Integer votedFor = null;

    // Volatile state - wiped by a crash.
    private Role role = Role.FOLLOWER;
    private boolean crashed = false;
    private int votesGranted = 0;
    private final List<Long> termsWon = new ArrayList<>();

    public ElectionNode(int id, List<Integer> everyone, LogPosition logPosition) {
        this.id = id;
        this.everyone = everyone;
        this.logPosition = logPosition;
    }

    @Override
    public List<Message> step(long now, Object event) {
        if (crashed) {
            if (event instanceof Recover) {
                crashed = false; // durable term/vote/log survive; role is already FOLLOWER
            }
            return List.of();
        }
        return switch (event) {
            case ElectionTimeout timeout -> standForElection();
            case VoteRequest request -> onVoteRequest(request);
            case VoteReply reply -> onVoteReply(reply);
            case Crash crash -> onCrash();
            case Recover recover -> List.of(); // not crashed - nothing to do
            default -> throw new IllegalArgumentException("election node " + id + ": unknown event " + event);
        };
    }

    /** Patience ran out: open a new term, vote for ourselves, ask everyone else. */
    private List<Message> standForElection() {
        if (role == Role.LEADER) {
            // A sitting leader has nothing to gain from a new election.
            return List.of();
        }
        role = Role.CANDIDATE;
        currentTerm++;              // durable
        votedFor = id;              // durable - our own vote, in pen
        votesGranted = 1;           // ourselves
        List<Message> out = new ArrayList<>();
        for (int peer : everyone) {
            if (peer != id) {
                out.add(new Message(id, peer, new VoteRequest(currentTerm, id, logPosition)));
            }
        }
        return out;
    }

    /**
     * Someone is asking for our vote. Order matters: adopt a newer term
     * first (stepping down if we led an older one), then apply the two
     * refusal rules, and only if both pass, record the vote durably BEFORE
     * the reply leaves - so a crash between deciding and replying can never
     * lead to a second, contradictory vote in this term.
     */
    private List<Message> onVoteRequest(VoteRequest request) {
        if (request.term() > currentTerm) {
            currentTerm = request.term();  // durable
            votedFor = null;               // durable - new term, fresh vote
            role = Role.FOLLOWER;
        }
        boolean grant = request.term() == currentTerm
                && (votedFor == null || votedFor == request.candidateId())
                && request.position().isAtLeastAsUpToDateAs(logPosition);
        if (grant) {
            votedFor = request.candidateId(); // durable, BEFORE the reply below
        }
        return List.of(new Message(id, request.candidateId(),
                new VoteReply(currentTerm, id, grant)));
    }

    /** A vote came back. Count it only if we are still the candidate of that term. */
    private List<Message> onVoteReply(VoteReply reply) {
        if (reply.term() > currentTerm) {
            // The electorate has moved on to a newer term without us.
            currentTerm = reply.term(); // durable
            votedFor = null;            // durable
            role = Role.FOLLOWER;
            return List.of();
        }
        if (reply.term() == currentTerm && role == Role.CANDIDATE && reply.granted()) {
            votesGranted++;
            if (votesGranted >= majority()) {
                role = Role.LEADER;
                termsWon.add(currentTerm); // the granted epoch - the module's product
            }
        }
        return List.of();
    }

    private List<Message> onCrash() {
        crashed = true;
        role = Role.FOLLOWER;
        votesGranted = 0;
        // currentTerm, votedFor, logPosition survive - that survival is the
        // whole point of the vote-crash-revote regression test.
        return List.of();
    }

    private int majority() {
        return everyone.size() / 2 + 1;
    }

    // Test-visible state

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    /** Every term this node won - the epochs it was granted. */
    public List<Long> termsWon() {
        return List.copyOf(termsWon);
    }
}

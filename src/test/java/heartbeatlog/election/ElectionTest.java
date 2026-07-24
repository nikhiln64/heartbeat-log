package heartbeatlog.election;

import heartbeatlog.core.Role;
import heartbeatlog.simulation.Crash;
import heartbeatlog.simulation.Message;
import heartbeatlog.simulation.Node;
import heartbeatlog.simulation.Recover;
import heartbeatlog.simulation.Simulation;
import heartbeatlog.simulation.SimulatedNetwork;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The election module's scripted suite: a clean election, the stale
 * candidate rejection (named test 9), the split vote (named test 7), the
 * vote-crash-revote regression (named test 10), leader step-down on a
 * newer term, and a many-seed election-safety sweep (at most one winner
 * per term, ever).
 */
class ElectionTest {

    private static final LogPosition CURRENT = new LogPosition(3, 10);
    private static final LogPosition STALE = new LogPosition(3, 6);

    private record Cluster(Simulation simulation, List<ElectionNode> nodes) {}

    private Cluster newCluster(long seed, LogPosition... positions) {
        SimulatedNetwork network = new SimulatedNetwork(seed, 1, 4, 0.0);
        Simulation simulation = new Simulation(network);
        List<Integer> everyone = List.of(0, 1, 2);
        List<ElectionNode> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ElectionNode node = new ElectionNode(i, everyone, positions[i]);
            nodes.add(node);
            simulation.addNode(node);
        }
        return new Cluster(simulation, nodes);
    }

    @Test
    void aCleanElectionElectsTheCandidateAndGrantsTheTerm() {
        Cluster cluster = newCluster(41, CURRENT, CURRENT, CURRENT);
        cluster.simulation().schedule(10, 0, new ElectionTimeout());
        cluster.simulation().run();

        assertEquals(Role.LEADER, cluster.nodes().get(0).role());
        assertEquals(List.of(1L), cluster.nodes().get(0).termsWon(), "term 1 is the granted epoch");
        assertEquals(Role.FOLLOWER, cluster.nodes().get(1).role());
        assertEquals(Role.FOLLOWER, cluster.nodes().get(2).role());
    }

    @Test
    void staleCandidateIsRejectedByUpToDateVoters() {
        // Named test 9. Node 0's log fingerprint is behind: it can campaign,
        // but neither up-to-date voter will have it, and 1 vote of 3 is no
        // majority. A fresher node then wins the NEXT term - proof the
        // refusal is about the candidate, not a wedged cluster.
        Cluster cluster = newCluster(42, STALE, CURRENT, CURRENT);
        cluster.simulation().schedule(10, 0, new ElectionTimeout());
        cluster.simulation().schedule(50, 1, new ElectionTimeout());
        cluster.simulation().run();

        assertEquals(List.of(), cluster.nodes().get(0).termsWon(), "a stale candidate must never win");
        assertFalse(cluster.nodes().get(0).role() == Role.LEADER);
        assertEquals(List.of(2L), cluster.nodes().get(1).termsWon(), "the up-to-date candidate wins term 2");
        assertEquals(Role.LEADER, cluster.nodes().get(1).role());
    }

    @Test
    void splitVoteElectsNobodyAndTheNextTermResolvesIt() {
        // Named test 7. With node 2 crashed, candidates 0 and 1 each vote
        // for themselves in term 1 and refuse each other (one vote per
        // term, in pen). One vote each, majority is two: nobody wins.
        // Safety intact, liveness paused - until a later timeout opens
        // term 2 and resolves it.
        Cluster cluster = newCluster(43, CURRENT, CURRENT, CURRENT);
        cluster.simulation().schedule(5, 2, new Crash());
        cluster.simulation().schedule(10, 0, new ElectionTimeout());
        cluster.simulation().schedule(10, 1, new ElectionTimeout());
        cluster.simulation().schedule(60, 0, new ElectionTimeout());
        cluster.simulation().run();

        assertFalse(cluster.nodes().get(0).termsWon().contains(1L), "term 1 split - nobody may win it");
        assertFalse(cluster.nodes().get(1).termsWon().contains(1L), "term 1 split - nobody may win it");
        assertEquals(List.of(2L), cluster.nodes().get(0).termsWon(), "the retry in term 2 must succeed");
        assertEquals(Role.LEADER, cluster.nodes().get(0).role());
    }

    @Test
    void aVoteSurvivesACrashAndIsNeverGrantedTwiceInATerm() {
        // Named test 10 - the regression the doc demanded: vote, crash,
        // recover, same-term request from a DIFFERENT candidate - no second
        // grant, because votedFor was written in pen before the reply left.
        SimulatedNetwork network = new SimulatedNetwork(44, 1, 4, 0.0);
        Simulation simulation = new Simulation(network);
        List<Integer> everyone = List.of(0, 1, 2);
        ElectionNode voter = new ElectionNode(0, everyone, CURRENT);
        ElectionNode candidate = new ElectionNode(1, everyone, CURRENT);
        ElectionNode bystander = new ElectionNode(2, everyone, CURRENT);
        simulation.addNode(voter);
        simulation.addNode(candidate);
        simulation.addNode(bystander);
        // A recorder in place of a rival candidate: it receives the voter's
        // replies so the test can read them verbatim.
        List<VoteReply> repliesToRival = new ArrayList<>();
        int rival = simulation.addNode((now, event) -> {
            if (event instanceof VoteReply reply) {
                repliesToRival.add(reply);
            }
            return List.of();
        });

        simulation.schedule(10, 1, new ElectionTimeout()); // candidate opens term 1, wins
        simulation.schedule(30, 0, new Crash());           // the voter faints...
        simulation.schedule(40, 0, new Recover());         // ...and wakes up
        // A rival asks for the SAME term's vote after the recovery.
        simulation.schedule(50, 0, new VoteRequest(1, rival, CURRENT));
        simulation.run();

        assertEquals(List.of(1L), candidate.termsWon(), "the real candidate won term 1 with the voter's grant");
        assertEquals(1, repliesToRival.size(), "the rival's request must be answered");
        assertFalse(repliesToRival.get(0).granted(),
                "REGRESSION GUARD: the vote was written in pen - crash and recovery must not allow a second grant in the same term");
    }

    @Test
    void aLeaderStepsDownWhenANewerTermAsksForVotes() {
        Cluster cluster = newCluster(45, CURRENT, CURRENT, CURRENT);
        cluster.simulation().schedule(10, 0, new ElectionTimeout()); // node 0 leads term 1
        cluster.simulation().schedule(50, 1, new ElectionTimeout()); // node 1 opens term 2
        cluster.simulation().run();

        assertEquals(Role.LEADER, cluster.nodes().get(1).role());
        assertEquals(List.of(2L), cluster.nodes().get(1).termsWon());
        assertEquals(Role.FOLLOWER, cluster.nodes().get(0).role(),
                "the term-1 leader must step down on seeing term 2");
    }

    @Test
    void electionSafetyHoldsAcrossManyContendedSeeds() {
        // The election-safety property, swept scripted-style: for many
        // seeds, three nodes stand at nearly the same instant, twice.
        // Delivery order varies per seed; the invariant may not: no term is
        // ever won by two nodes (majorities overlap; votes are in pen).
        // Liveness is deliberately NOT asserted - contended rounds may
        // elect nobody, and that is allowed; only double-winners are not.
        for (long seed = 1; seed <= 25; seed++) {
            Cluster cluster = newCluster(seed, CURRENT, CURRENT, CURRENT);
            for (int node = 0; node < 3; node++) {
                cluster.simulation().schedule(10 + node, node, new ElectionTimeout());
                cluster.simulation().schedule(40 + node, node, new ElectionTimeout());
            }
            cluster.simulation().run();

            Set<Long> wonTerms = new HashSet<>();
            for (ElectionNode node : cluster.nodes()) {
                for (long term : node.termsWon()) {
                    assertTrue(wonTerms.add(term),
                            "seed " + seed + ": term " + term + " was won twice - election safety violated");
                }
            }
        }
    }
}

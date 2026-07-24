package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.core.Role;
import heartbeatlog.simulation.Simulation;
import heartbeatlog.simulation.SimulatedNetwork;
import heartbeatlog.simulation.TracePrinter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failover and the in-sync roster (build-plan H5-7): crashes, recoveries,
 * roster shrink/re-admission, the min-in-sync write refusal, and the
 * HIGH_WATERMARK recovery truncation - each staged with a scripted
 * schedule and checked against the commit ledger's promises.
 *
 * Timing vocabulary used by every schedule here:
 * - network delay is 1-4 ticks per message, no drops
 * - LAG_LIMIT (20 ticks) is the roster patience: a follower not caught up
 *   for longer than this is dropped from the roster
 * - writes are spaced a few ticks apart so replication interleaves
 */
class ReplicationFailoverTest {

    private static final int CONTROLLER = 3;
    private static final long LAG_LIMIT = 20;

    private record Cluster(Simulation simulation, List<Replica> replicas, CommitLedger ledger) {}

    /** 3 replicas + controller; replica 0 appointed leader via the controller at t=0. */
    private Cluster newCluster(long seed) {
        SimulatedNetwork network = new SimulatedNetwork(seed, 1, 4, 0.0);
        Simulation simulation = new Simulation(network);
        CommitLedger ledger = new CommitLedger();
        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Replica replica = new Replica(i, ledger, TruncationRule.HIGH_WATERMARK, LAG_LIMIT);
            replicas.add(replica);
            simulation.addNode(replica);
        }
        simulation.addNode(new ControllerStub(CONTROLLER, List.of(0, 1, 2)));
        simulation.schedule(0, CONTROLLER, new LeaderChange(0));
        return new Cluster(simulation, replicas, ledger);
    }

    /** Every promise in the ledger must exist, identically, in the given replica's log. */
    private void assertNoCommittedEntryLost(Cluster cluster, Replica survivor) {
        List<Entry> log = survivor.log();
        for (int i = 0; i < cluster.ledger().committed().size(); i++) {
            Entry promised = cluster.ledger().committed().get(i);
            assertTrue(log.size() > promised.offset() && log.get((int) promised.offset()).equals(promised),
                    () -> "committed entry lost: " + promised + "\n" + TracePrinter.render(cluster.simulation().trace()));
        }
    }

    @Test
    void singleFailoverLosesNothing() {
        Cluster cluster = newCluster(21);
        // Phase 1: 10 writes commit under leader 0.
        for (int n = 0; n < 10; n++) {
            cluster.simulation().schedule(10 + 3L * n, 0, new ClientAppend("before-" + n));
        }
        // Phase 2: leader 0 dies; controller appoints replica 1; writes go there.
        cluster.simulation().schedule(60, 0, new Crash());
        cluster.simulation().schedule(61, CONTROLLER, new LeaderChange(1));
        for (int n = 0; n < 5; n++) {
            // Writes resume after the roster drops the crashed old leader
            // (patience is LAG_LIMIT, so schedule past 61 + 20).
            cluster.simulation().schedule(90 + 3L * n, 1, new ClientAppend("after-" + n));
        }
        cluster.simulation().run();

        Replica newLeader = cluster.replicas().get(1);
        Replica follower = cluster.replicas().get(2);
        assertEquals(Role.LEADER, newLeader.role());
        assertEquals(15, newLeader.log().size(), "10 before + 5 after the failover");
        assertEquals(newLeader.log(), follower.log(), "survivors must converge");
        assertEquals(15, newLeader.highWatermark());
        assertNoCommittedEntryLost(cluster, newLeader);
        assertNoCommittedEntryLost(cluster, follower);
    }

    @Test
    void laggingFollowerIsDroppedFromRosterAndCommitsContinue() {
        Cluster cluster = newCluster(22);
        cluster.simulation().schedule(30, 2, new Crash());
        // Writes spanning the crash: early ones commit with all three, the
        // ones after (30 + LAG_LIMIT) require the roster to shrink to {0,1}
        // or the watermark would freeze forever.
        for (int n = 0; n < 15; n++) {
            cluster.simulation().schedule(10 + 4L * n, 0, new ClientAppend("w-" + n));
        }
        cluster.simulation().run();

        Replica leader = cluster.replicas().get(0);
        assertEquals(List.of(0, 1), leader.inSyncReplicas(), "crashed follower 2 must be dropped");
        assertEquals(15, leader.highWatermark(), "commits must continue without the laggard");
        assertEquals(15, cluster.ledger().committed().size());
        assertNoCommittedEntryLost(cluster, leader);
    }

    @Test
    void recoveredFollowerCatchesUpAndRejoinsRoster() {
        Cluster cluster = newCluster(23);
        cluster.simulation().schedule(30, 2, new Crash());
        for (int n = 0; n < 15; n++) {
            cluster.simulation().schedule(10 + 4L * n, 0, new ClientAppend("w-" + n));
        }
        // Well after the roster dropped it, follower 2 comes back, refetches
        // everything it missed, and must be re-admitted.
        cluster.simulation().schedule(120, 2, new Recover());
        cluster.simulation().run();

        Replica leader = cluster.replicas().get(0);
        Replica recovered = cluster.replicas().get(2);
        assertEquals(List.of(0, 1, 2), leader.inSyncReplicas(), "caught-up follower must be re-admitted");
        assertEquals(leader.log(), recovered.log(), "recovered follower must converge");
        assertEquals(15, recovered.highWatermark());
    }

    @Test
    void belowMinInSyncTheLeaderRefusesWritesInsteadOfRiskingThem() {
        Cluster cluster = newCluster(24);
        // Both followers die early; after LAG_LIMIT the roster is {0} alone.
        cluster.simulation().schedule(20, 1, new Crash());
        cluster.simulation().schedule(20, 2, new Crash());
        // First writes land while the roster still has everyone; the late
        // ones (t >= 45 > 20 + LAG_LIMIT) meet a roster of one and must be
        // refused - the cluster chooses unavailability over promises held
        // by a single machine.
        for (int n = 0; n < 4; n++) {
            cluster.simulation().schedule(5 + 2L * n, 0, new ClientAppend("early-" + n));
        }
        for (int n = 0; n < 5; n++) {
            cluster.simulation().schedule(45 + 2L * n, 0, new ClientAppend("refused-" + n));
        }
        // Recovery: followers return, catch up, roster heals - and a final
        // write commits again, proving refusal was a pause, not a wedge.
        cluster.simulation().schedule(60, 1, new Recover());
        cluster.simulation().schedule(60, 2, new Recover());
        cluster.simulation().schedule(100, 0, new ClientAppend("resumed"));
        cluster.simulation().run();

        Replica leader = cluster.replicas().get(0);
        assertEquals(5, leader.refusedAppends(), "all five below-floor writes must be refused");
        assertEquals(5, leader.log().size(), "4 early + 1 resumed; refused writes never enter the log");
        assertEquals("resumed", leader.log().get(4).payload());
        assertEquals(List.of(0, 1, 2), leader.inSyncReplicas(), "roster must heal after recovery");
        assertEquals(5, leader.highWatermark());
        for (Entry promised : cluster.ledger().committed()) {
            assertFalse(promised.payload().startsWith("refused-"), "a refused write must never be promised");
        }
        assertNoCommittedEntryLost(cluster, leader);
    }

    @Test
    void recoveryTruncatesToLocalWatermarkUnderTheBuggyRule() {
        Cluster cluster = newCluster(25);
        // Kill follower 1 early so the roster becomes {0, 2}: watermark
        // advancement then depends on follower 2 alone, keeping follower
        // 2's own watermark KNOWLEDGE about one fetch behind its log end
        // while writes flow - exactly the gap the HIGH_WATERMARK rule
        // tears away.
        cluster.simulation().schedule(5, 1, new Crash());
        for (int n = 0; n < 12; n++) {
            cluster.simulation().schedule(30 + 2L * n, 0, new ClientAppend("w-" + n));
        }
        // Crash follower 2 just after the write stream ends (t=54 > last
        // write at t=52), while its watermark knowledge still trails what
        // it stored; recover it later. Crashing mid-stream would also be
        // legal but adds a refusal window (with follower 1 already dead,
        // evicting 2 leaves the roster below the floor) - that behavior is
        // covered by the min-in-sync test; this one isolates truncation.
        cluster.simulation().schedule(54, 2, new Crash());
        cluster.simulation().schedule(120, 2, new Recover());
        cluster.simulation().run();

        Replica leader = cluster.replicas().get(0);
        Replica recovered = cluster.replicas().get(2);
        assertEquals(0, leader.refusedAppends(), "no write in this schedule should meet a below-floor roster");
        // The interesting assertion is convergence AFTER a truncation: the
        // recovered follower tore its log to its stale watermark on boot,
        // then refetched the difference. Loss requires a leadership bounce
        // on top of this tear - that is the red test's job (H7-8), not
        // this one.
        assertEquals(leader.log(), recovered.log(),
                () -> "recovered follower must refetch what it truncated\n"
                        + TracePrinter.render(cluster.simulation().trace()));
        assertEquals(12, recovered.log().size());
        assertNoCommittedEntryLost(cluster, recovered);
    }

    @Test
    void sameSeedReplaysIdenticalFailoverRun() {
        String first = runSingleFailoverTrace();
        String second = runSingleFailoverTrace();
        assertEquals(first, second, "failover schedules must replay byte-identically");
    }

    private String runSingleFailoverTrace() {
        Cluster cluster = newCluster(26);
        for (int n = 0; n < 8; n++) {
            cluster.simulation().schedule(10 + 3L * n, 0, new ClientAppend("w-" + n));
        }
        cluster.simulation().schedule(40, 0, new Crash());
        cluster.simulation().schedule(41, CONTROLLER, new LeaderChange(1));
        cluster.simulation().schedule(70, 1, new ClientAppend("post"));
        cluster.simulation().run();
        return cluster.simulation().renderTrace();
    }
}

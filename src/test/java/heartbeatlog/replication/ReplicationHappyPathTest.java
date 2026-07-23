package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.core.Role;
import heartbeatlog.simulation.Simulation;
import heartbeatlog.simulation.SimulatedNetwork;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The happy path (build-plan H3-5): one leader, two followers, no faults.
 * N client writes must end up byte-identical on every replica, the high
 * watermark must reach N everywhere, and the commit ledger must hold
 * exactly the N entries in order - the cluster promised nothing it didn't
 * replicate.
 */
class ReplicationHappyPathTest {

    private static final int REPLICAS = 3;
    private static final int WRITES = 20;

    // Shape of the scenario on the simulated clock:
    // - t=0: every replica hears the appointment; followers start pulling.
    // - t=5: first client write. The 5-tick head start lets the first
    //   fetches (network delay is 1-4 ticks) reach the leader first, so
    //   the run exercises "write arrives while followers are waiting".
    // - one write every 3 ticks: deliberately INSIDE the 1-4 tick delay
    //   range, so writes and fetch round-trips interleave in shuffled
    //   orders instead of the degenerate "all writes land, then all
    //   replication happens" sequence.
    private static final long FIRST_WRITE_AT = 5;
    private static final long WRITE_INTERVAL = 3;

    private record Cluster(Simulation simulation, List<Replica> replicas, CommitLedger ledger) {}

    /**
     * Builds and runs the whole happy-path scenario: 3 replicas, replica 0
     * appointed leader at t=0, then 20 staggered client writes aimed at the
     * leader, over a lossless network with per-message delays of 1-4 ticks.
     * Returns everything a test needs to inspect the end state.
     */
    private Cluster runHappyPath(long seed) {
        SimulatedNetwork network = new SimulatedNetwork(seed, 1, 4, 0.0);
        Simulation simulation = new Simulation(network);
        CommitLedger ledger = new CommitLedger();
        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < REPLICAS; i++) {
            Replica replica = new Replica(i, ledger);
            replicas.add(replica);
            simulation.addNode(replica);
        }
        LeaderAppointment appointment = new LeaderAppointment(1, 0, List.of(0, 1, 2));
        for (int i = 0; i < REPLICAS; i++) {
            simulation.schedule(0, i, appointment);
        }
        for (int n = 0; n < WRITES; n++) {
            simulation.schedule(FIRST_WRITE_AT + WRITE_INTERVAL * n, 0, new ClientAppend("sample-" + n));
        }
        simulation.run();
        return new Cluster(simulation, replicas, ledger);
    }

    @Test
    void allReplicasConvergeToIdenticalLogs() {
        Cluster cluster = runHappyPath(7);

        List<Entry> leaderLog = cluster.replicas().get(0).log();
        assertEquals(WRITES, leaderLog.size());
        for (Replica replica : cluster.replicas()) {
            assertEquals(leaderLog, replica.log(), "every replica must hold the identical log");
        }
        for (int n = 0; n < WRITES; n++) {
            assertEquals("sample-" + n, leaderLog.get(n).payload(), "log order must match append order");
            assertEquals(1, leaderLog.get(n).epoch(), "all entries were appended under epoch 1");
        }
    }

    @Test
    void highWatermarkReachesEveryReplica() {
        Cluster cluster = runHappyPath(7);

        for (Replica replica : cluster.replicas()) {
            assertEquals(WRITES, replica.highWatermark(),
                    "replica must learn the final high watermark, not just the leader");
        }
        assertEquals(Role.LEADER, cluster.replicas().get(0).role());
        assertEquals(Role.FOLLOWER, cluster.replicas().get(1).role());
        assertEquals(Role.FOLLOWER, cluster.replicas().get(2).role());
    }

    @Test
    void commitLedgerHoldsExactlyTheAppendedEntriesInOrder() {
        Cluster cluster = runHappyPath(7);

        assertEquals(cluster.replicas().get(0).log(), cluster.ledger().committed(),
                "the cluster must promise exactly what it replicated, in order");
    }

    @Test
    void quiescentClusterIsSilentNotPingPonging() {
        // The waiting list is the difference between "caught up, waiting
        // quietly" and an infinite empty-fetch loop. If it broke, the run
        // would hit the runaway backstop instead of draining - but also
        // check the trace stays proportional to the work done.
        Cluster cluster = runHappyPath(7);
        long traceLines = cluster.simulation().trace().size();
        assertTrue(traceLines < WRITES * REPLICAS * 12L,
                "trace has " + traceLines + " events - a quiesced cluster should not keep chattering");
    }

    @Test
    void sameSeedReplaysIdenticalRun() {
        assertEquals(runHappyPath(11).simulation().renderTrace(),
                runHappyPath(11).simulation().renderTrace(),
                "the replication protocol must preserve whole-run determinism");
    }
}

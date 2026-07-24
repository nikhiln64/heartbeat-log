package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.simulation.Crash;
import heartbeatlog.simulation.Recover;
import heartbeatlog.simulation.Simulation;
import heartbeatlog.simulation.SimulatedNetwork;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The mechanics of the fix, tested on their own: the epoch-end lookup's
 * edge cases (pure function, no simulation needed) and a true-divergence
 * scenario where the boundary cut discards exactly the right entries.
 */
class EpochReconciliationTest {

    // Named test 5: the lookup's edges, directly.

    private static List<Entry> logOfEpochs(long... epochs) {
        List<Entry> log = new ArrayList<>();
        for (int i = 0; i < epochs.length; i++) {
            log.add(new Entry(epochs[i], i, "e" + epochs[i] + "-" + i));
        }
        return log;
    }

    @Test
    void epochEndLookupHandlesAllEdges() {
        List<Entry> log = logOfEpochs(1, 1, 2, 2, 5);

        assertEquals(2, Replica.endOffsetForEpoch(log, 1), "epoch 1 ends where epoch 2 begins");
        assertEquals(4, Replica.endOffsetForEpoch(log, 2), "epoch 2 ends where epoch 5 begins");
        assertEquals(4, Replica.endOffsetForEpoch(log, 3),
                "an epoch the leader never saw ends where the first LARGER epoch begins");
        assertEquals(4, Replica.endOffsetForEpoch(log, 4),
                "same rule for any absent epoch below the latest");
        assertEquals(5, Replica.endOffsetForEpoch(log, 5), "the latest epoch ends at the log end");
        assertEquals(5, Replica.endOffsetForEpoch(log, 9), "a future epoch also ends at the log end");
        assertEquals(0, Replica.endOffsetForEpoch(log, 0),
                "an epoch older than the whole log ends before it starts");
        assertEquals(0, Replica.endOffsetForEpoch(List.of(), 3), "empty log: everything ends at 0");
    }

    // True divergence: the cut lands exactly at the epoch boundary.

    @Test
    void divergentUncommittedSuffixIsCutAtTheBoundaryAndReplaced() {
        SimulatedNetwork network = new SimulatedNetwork(31, 1, 4, 0.0);
        Simulation simulation = new Simulation(network);
        CommitLedger ledger = new CommitLedger();
        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Replica replica = new Replica(i, ledger, TruncationRule.EPOCH_BOUNDARY, 20);
            replicas.add(replica);
            simulation.addNode(replica);
        }
        simulation.addNode(new ControllerStub(3, List.of(0, 1, 2)));

        simulation.schedule(0, 3, new LeaderChange(0));
        // Four entries commit everywhere under epoch 1.
        for (int n = 0; n < 4; n++) {
            simulation.schedule(10 + 3L * n, 0, new ClientAppend("shared-" + n));
        }
        // Both followers die FIRST - so the next append truly reaches
        // nobody (the answers to their waiting fetches land on dead nodes).
        simulation.schedule(45, 1, new Crash());
        simulation.schedule(45, 2, new Crash());
        // The divergent entry: accepted (the roster hasn't learned the
        // followers are gone), replicated to no one, and - because the
        // leader crashes before any further event could shrink the roster
        // and advance the watermark - never committed. A true divergent,
        // uncommitted, epoch-1 suffix existing only on replica 0.
        simulation.schedule(50, 0, new ClientAppend("divergent-0"));
        simulation.schedule(52, 0, new Crash());
        // Epoch 2: the followers return (their fetches to the dead leader
        // go nowhere until the decree arrives), replica 1 leads from the
        // fully-committed prefix and commits two entries at the offset the
        // divergent entry occupied.
        simulation.schedule(60, 1, new Recover());
        simulation.schedule(60, 2, new Recover());
        simulation.schedule(65, 3, new LeaderChange(1));
        simulation.schedule(90, 1, new ClientAppend("epoch2-0"));
        simulation.schedule(92, 1, new ClientAppend("epoch2-1"));
        // The old leader returns. Reconciliation asks: "where does epoch 1
        // end in YOUR log?" Answer: offset 4. The divergent entry dies; the
        // epoch-2 entries take its place.
        simulation.schedule(120, 0, new Recover());
        simulation.schedule(125, 3, new LeaderChange(1)); // re-decree so the returnee learns the leadership
        simulation.schedule(200, 1, new ClientAppend("settle"));
        simulation.run();

        Replica returned = replicas.get(0);
        Replica leader = replicas.get(1);
        assertEquals(leader.log(), returned.log(), "the returnee must converge on the epoch-2 history");
        assertEquals("epoch2-0", returned.log().get(4).payload(),
                "the divergent suffix must be replaced by the committed epoch-2 entries");
        assertTrue(returned.log().stream().noneMatch(e -> e.payload().startsWith("divergent")),
                "uncommitted divergent entries must not survive reconciliation");
        // And nothing that was ever PROMISED went missing.
        for (Entry promised : ledger.committed()) {
            assertEquals(promised, returned.log().get((int) promised.offset()),
                    "every committed entry survives reconciliation");
        }
    }
}

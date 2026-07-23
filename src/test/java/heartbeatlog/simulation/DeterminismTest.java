package heartbeatlog.simulation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Named test 2 (seed replay): the same seed replays a byte-identical trace.
 * This property is the foundation everything else stands on - the KIP-101
 * red/green pair is only a controlled experiment if the fault schedule is
 * identical between the two runs.
 *
 * Workload: a 3-node gossip storm. Node 0 receives a TTL counter; every node
 * forwards (ttl - 1) to both peers until it hits zero. With drops enabled the
 * storm is pruned differently per seed, exercising delivery, delay, and drop
 * paths together.
 */
class DeterminismTest {

    private static final int NODES = 3;

    private String runTrace(long seed) {
        SimulatedNetwork network = new SimulatedNetwork(seed, 1, 5, 0.10);
        Simulation simulation = new Simulation(network);
        for (int i = 0; i < NODES; i++) {
            final int self = i;
            simulation.addNode((now, event) -> {
                int ttl = (Integer) event;
                if (ttl <= 0) {
                    return List.of();
                }
                List<Message> out = new ArrayList<>();
                for (int peer = 0; peer < NODES; peer++) {
                    if (peer != self) {
                        out.add(new Message(self, peer, ttl - 1));
                    }
                }
                return out;
            });
        }
        simulation.schedule(0, 0, 8);
        simulation.run();
        return simulation.renderTrace();
    }

    @Test
    void sameSeedReplaysByteIdenticalTrace() {
        String first = runTrace(42);
        String second = runTrace(42);

        assertEquals(first, second, "same seed must replay the identical event trace");
        assertTrue(first.lines().count() > 50,
                "trace suspiciously small (" + first.lines().count() + " lines) - the workload is not exercising the simulation");
        assertTrue(first.contains(" drop "), "expected at least one drop at 10% loss - drop path untested otherwise");
    }

    @Test
    void differentSeedsDiverge() {
        // Not a probabilistic flake: fates are pure functions of the seed, so
        // two seeds either produce identical traces every run (a bug worth
        // failing on) or different traces every run.
        assertNotEquals(runTrace(42), runTrace(43),
                "two seeds produced identical traces - the network is ignoring its seed");
    }
}

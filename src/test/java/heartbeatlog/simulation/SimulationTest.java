package heartbeatlog.simulation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SimulationTest {

    /**
     * Named test 1 (tie-break determinism): events scheduled for the SAME
     * simulated time pop in schedule order, because the queue is totally
     * ordered by (time, seq) - never by PriorityQueue's unspecified tie
     * behavior.
     */
    @Test
    void equalTimestampEventsPopInScheduleOrder() {
        Simulation simulation = new Simulation(SimulatedNetwork.reliable());
        List<String> seen = new ArrayList<>();
        int node = simulation.addNode((now, event) -> {
            seen.add((String) event);
            return List.of();
        });

        simulation.schedule(5, node, "first-at-5");
        simulation.schedule(5, node, "second-at-5");
        simulation.schedule(5, node, "third-at-5");
        simulation.schedule(1, node, "earliest");
        simulation.run();

        assertEquals(List.of("earliest", "first-at-5", "second-at-5", "third-at-5"), seen);
    }

    @Test
    void clockAdvancesToDeliveryTime() {
        Simulation simulation = new Simulation(SimulatedNetwork.reliable());
        List<Long> times = new ArrayList<>();
        int node = simulation.addNode((now, event) -> {
            times.add(now);
            return List.of();
        });

        simulation.schedule(3, node, "a");
        simulation.schedule(10, node, "b");
        simulation.run();

        assertEquals(List.of(3L, 10L), times);
        assertEquals(10, simulation.clock().now());
    }

    @Test
    void schedulingIntoThePastIsRejected() {
        Simulation simulation = new Simulation(SimulatedNetwork.reliable());
        int node = simulation.addNode((now, event) -> List.of());
        simulation.schedule(5, node, "advance");
        simulation.run();

        assertThrows(IllegalArgumentException.class, () -> simulation.schedule(4, node, "too-late"));
    }

    @Test
    void messagesFlowThroughTheNetworkWithDelay() {
        // reliable() delays every message exactly 1 tick
        Simulation simulation = new Simulation(SimulatedNetwork.reliable());
        List<String> log = new ArrayList<>();

        int ponger = simulation.addNode((now, event) -> {
            log.add(now + ":pong-received-" + event);
            return List.of();
        });
        int pinger = simulation.addNode((now, event) -> {
            log.add(now + ":ping-received-" + event);
            return List.of(new Message(1, ponger, "pong"));
        });

        simulation.schedule(7, pinger, "ping");
        simulation.run();

        assertEquals(List.of("7:ping-received-ping", "8:pong-received-pong"), log);
    }
}

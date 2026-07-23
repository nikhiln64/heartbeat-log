package heartbeatlog.simulation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * The heart of the project - a deterministic simulation: one thread, one queue, one clock.
 *
 * <pre>
 *  schedule(t, node, payload)         ┌──────────────────────────────┐
 *  ────────────────────────────────▶  │ PriorityQueue<(time, seq)>   │
 *  (scripted schedules, fuzzer,       └──────────────┬───────────────┘
 *   and message deliveries)                          │ pop lowest (time, seq)
 *                                                    ▼
 *                                        clock.advanceTo(time)
 *                                        node.step(now, payload)
 *                                                    │ outgoing List<Message>
 *                                   ┌────────────────┴────────────────┐
 *                                   ▼                                 ▼
 *                        network.drops(link, now)?          delay = network.delay(link, now)
 *                          yes → trace "drop"               schedule(now + delay, to, payload)
 * </pre>
 *
 * Total event order is (time, seq): seq is a monotonic counter stamped at
 * enqueue, so equal-timestamp events pop in schedule order on every machine
 * and JVM. PriorityQueue alone leaves ties unspecified - that unspecified
 * order is exactly where "deterministic on any machine" would quietly die.
 *
 * Strictly single-threaded: nodes run only inside run(), on this thread.
 */
public final class Simulation {

    private record Queued(long time, long seq, int target, Object payload) {}

    // ponytail: 5M events is a runaway-loop backstop, not a tuning knob -
    // the largest planned schedule is a few thousand events.
    private static final long MAX_EVENTS = 5_000_000;

    private final PriorityQueue<Queued> queue = new PriorityQueue<>(
            Comparator.comparingLong(Queued::time).thenComparingLong(Queued::seq));
    private final SimulatedClock clock = new SimulatedClock();
    private final SimulatedNetwork network;
    private final List<Node> nodes = new ArrayList<>();
    private final List<TraceEntry> trace = new ArrayList<>();
    private long nextSeq = 0;

    public Simulation(SimulatedNetwork network) {
        this.network = network;
    }

    /** Registers a node; node ids are dense ints in registration order. */
    public int addNode(Node node) {
        nodes.add(node);
        return nodes.size() - 1;
    }

    public SimulatedClock clock() {
        return clock;
    }

    /**
     * Enqueues an event for delivery to a node at an absolute simulated time.
     * This is the single entry point for scripted schedule events
     * (ClientAppend, Crash, LeaderChange, ElectionTimeout, ...) and is also
     * used internally for message deliveries.
     */
    public void schedule(long time, int target, Object payload) {
        if (time < clock.now()) {
            throw new IllegalArgumentException(
                    "cannot schedule into the past: " + time + " < " + clock.now());
        }
        queue.add(new Queued(time, nextSeq++, target, payload));
    }

    /** Runs until the queue drains (or the runaway backstop trips). */
    public void run() {
        long delivered = 0;
        while (!queue.isEmpty()) {
            if (++delivered > MAX_EVENTS) {
                throw new IllegalStateException("runaway simulation: > " + MAX_EVENTS + " events");
            }
            Queued q = queue.poll();
            clock.advanceTo(q.time());
            trace.add(new TraceEntry(q.time(), q.seq(), "deliver", "node" + q.target() + " <- " + q.payload()));
            List<Message> outgoing = nodes.get(q.target()).step(clock.now(), q.payload());
            for (Message m : outgoing) {
                send(m);
            }
        }
    }

    private void send(Message m) {
        long now = clock.now();
        if (network.drops(m.from(), m.to(), now)) {
            trace.add(new TraceEntry(now, nextSeq++, "drop", m.from() + "->" + m.to() + " " + m.payload()));
        } else {
            schedule(now + network.delay(m.from(), m.to(), now), m.to(), m.payload());
        }
    }

    public List<TraceEntry> trace() {
        return List.copyOf(trace);
    }

    /** The full run as one string - what the determinism tests compare byte-for-byte. */
    public String renderTrace() {
        StringBuilder sb = new StringBuilder();
        for (TraceEntry entry : trace) {
            sb.append(entry).append('\n');
        }
        return sb.toString();
    }
}

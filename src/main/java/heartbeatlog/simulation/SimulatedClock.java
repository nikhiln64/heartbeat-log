package heartbeatlog.simulation;

/**
 * Simulated time. Only the Simulation advances it; everyone else reads it.
 * There is no wall clock anywhere in this project - if you are tempted to
 * call System.currentTimeMillis() or Thread.sleep(), determinism dies and
 * with it the same-seed red/green experiment.
 */
public final class SimulatedClock {

    private long now = 0;

    public long now() {
        return now;
    }

    void advanceTo(long time) {
        if (time < now) {
            throw new IllegalStateException("time went backwards: " + time + " < " + now);
        }
        now = time;
    }
}

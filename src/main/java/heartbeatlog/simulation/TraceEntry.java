package heartbeatlog.simulation;

/**
 * One line of the run's event trace. The rendered trace is the substrate of
 * the determinism tests ("same seed, byte-identical trace") and of the
 * live-walkthrough pretty-printer, so toString() must stay stable: change it
 * and every recorded seed's expected trace changes with it.
 */
public record TraceEntry(long time, long seq, String kind, String detail) {

    @Override
    public String toString() {
        return time + " #" + seq + " " + kind + " " + detail;
    }
}

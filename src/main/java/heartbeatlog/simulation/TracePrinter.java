package heartbeatlog.simulation;

import java.util.List;

/**
 * Turns a run's event trace into a readable, column-aligned timeline - the
 * artifact for debugging a failed schedule and for walking a run in a live
 * demo. The RAW trace (TraceEntry.toString) is what determinism tests
 * compare byte-for-byte; this printer is presentation only and can change
 * freely without touching recorded expectations.
 *
 * <pre>
 *   TIME   EVENT    WHAT
 *      0   deliver  node0 &lt;- LeaderAppointment[epoch=1, ...]
 *      5   deliver  node0 &lt;- ClientAppend[payload=sample-0]
 *      6   drop     0-&gt;2 FetchResponse[...]
 * </pre>
 */
public final class TracePrinter {

    private TracePrinter() {}

    public static String render(List<TraceEntry> trace) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%6s   %-7s  %s%n", "TIME", "EVENT", "WHAT"));
        for (TraceEntry entry : trace) {
            sb.append(String.format("%6d   %-7s  %s%n", entry.time(), entry.kind(), entry.detail()));
        }
        return sb.toString();
    }
}

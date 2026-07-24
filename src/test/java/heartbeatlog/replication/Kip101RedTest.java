package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.simulation.TracePrinter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE RED TEST. Run it with: ./gradlew redTest
 *
 * It states the property every storage user believes they have - "data the
 * cluster acknowledged as committed is never lost" - and runs the pinned
 * KIP-101 schedule under the pre-KIP-101 truncation rule. It FAILS, by
 * design: two committed telemetry entries (w-10, w-11) vanish from every
 * surviving replica after a crash and two quick leadership changes. The
 * failure message prints exactly which promises were broken and the full
 * event timeline of how.
 *
 * This is a faithful reproduction of Apache Kafka's KIP-101 data-loss bug
 * (fixed in 0.11, 2017). The next commit adds the same fix Kafka shipped -
 * leader-epoch-based truncation - and the green twin of this test replays
 * the IDENTICAL seed with nothing lost.
 *
 * Tagged out of the default suite so CI stays green; the failure is the
 * demo, not a regression.
 */
@Tag("kip101-red")
class Kip101RedTest {

    @Test
    void committedTelemetryIsNeverLost() {
        Kip101Schedule.Result result = Kip101Schedule.run(TruncationRule.HIGH_WATERMARK);

        List<Entry> lost = result.lostFrom(result.replicas().get(1));
        assertTrue(lost.isEmpty(), () ->
                "THE CLUSTER BROKE ITS PROMISE (seed " + Kip101Schedule.SEED
                        + ", crash at t=" + Kip101Schedule.CRASH_TIME + ").\n"
                        + "Committed entries lost from the new leader: " + lost + "\n"
                        + "Every one of these was acknowledged to the client as safely stored.\n\n"
                        + "How it happened, tick by tick:\n"
                        + TracePrinter.render(result.simulation().trace()));
    }
}

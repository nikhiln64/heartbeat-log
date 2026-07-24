package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.simulation.TracePrinter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE GREEN TWIN - the other half of the controlled experiment.
 *
 * Same pinned schedule as {@link Kip101RedTest}: same seed, same crash at
 * t=67, same double leadership bounce. One variable changes: the truncation
 * rule is EPOCH_BOUNDARY - Kafka's KIP-101 fix - instead of HIGH_WATERMARK.
 * Replica 1 recovers WITHOUT tearing its log, leads with all 12 entries,
 * and nothing committed is ever lost.
 *
 * Red run: committed w-10, w-11 vanish. Green run: identical faults,
 * nothing lost. The fault schedule is provably identical because message
 * fates are pure functions of (seed, link, sendTime) - the fix adding
 * reconciliation messages cannot shift any other message's outcome. That
 * is what makes this a controlled experiment rather than two anecdotes.
 */
class Kip101GreenTest {

    @Test
    void theIdenticalScheduleLosesNothingUnderEpochTruncation() {
        Kip101Schedule.Result result = Kip101Schedule.run(TruncationRule.EPOCH_BOUNDARY);

        // The same promises were made: 12 epoch-1 entries committed before
        // the bounce, w-10 and w-11 among them.
        List<Entry> epoch1Promises = result.ledger().committed().stream()
                .filter(entry -> entry.epoch() == 1).toList();
        assertEquals(Kip101Schedule.COMMITTED_BEFORE_BOUNCE, epoch1Promises.size(),
                "the green run must make the same promises the red run breaks");
        assertEquals("w-10", epoch1Promises.get(10).payload());
        assertEquals("w-11", epoch1Promises.get(11).payload());

        // And every promise is kept: nothing committed is missing from any
        // surviving replica.
        for (int survivor : List.of(1, 2)) {
            List<Entry> lost = result.lostFrom(result.replicas().get(survivor));
            assertTrue(lost.isEmpty(), () -> "replica " + survivor
                    + " lost committed entries under the FIX - that must be impossible: " + lost
                    + "\n" + TracePrinter.render(result.simulation().trace()));
        }

        // The post-bounce writes landed AFTER the preserved entries
        // (offsets 12, 13) instead of overwriting their graves (10, 11).
        List<Entry> newLeaderLog = result.replicas().get(1).log();
        assertEquals("w-10", newLeaderLog.get(10).payload());
        assertEquals("w-11", newLeaderLog.get(11).payload());
        assertEquals("post-0", newLeaderLog.get(12).payload());
        assertEquals(2, newLeaderLog.get(12).epoch());
    }
}

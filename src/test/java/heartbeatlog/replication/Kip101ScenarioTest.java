package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The anomaly guard - lives in the DEFAULT suite and stays green by
 * asserting that the KIP-101 schedule keeps LOSING data under the buggy
 * rule. If a refactor ever accidentally "fixes" (or differently breaks) the
 * reproduction, CI notices immediately, and the red demo stays honest.
 *
 * The demo itself - the same schedule asserted the way a user would state
 * it ("committed data must never vanish"), and therefore FAILING - is
 * {@link Kip101RedTest}, tagged out of CI and run via ./gradlew redTest.
 */
class Kip101ScenarioTest {

    @Test
    void theBuggyRuleLosesExactlyTheTwoWindowEntries() {
        Kip101Schedule.Result result = Kip101Schedule.run(TruncationRule.HIGH_WATERMARK);

        // The promises were real: 12 epoch-1 entries were committed before
        // the bounce, w-10 and w-11 among them.
        List<Entry> epoch1Promises = result.ledger().committed().stream()
                .filter(entry -> entry.epoch() == 1).toList();
        assertEquals(Kip101Schedule.COMMITTED_BEFORE_BOUNCE, epoch1Promises.size());
        assertEquals("w-10", epoch1Promises.get(10).payload());
        assertEquals("w-11", epoch1Promises.get(11).payload());

        // The betrayal: exactly those two committed entries are gone from
        // every surviving replica - overwritten at their offsets by epoch-2
        // entries.
        for (int survivor : List.of(1, 2)) {
            List<Entry> lost = result.lostFrom(result.replicas().get(survivor));
            assertEquals(2, lost.size(), "replica " + survivor + " must have lost exactly w-10 and w-11");
            assertEquals("w-10", lost.get(0).payload());
            assertEquals("w-11", lost.get(1).payload());
            assertEquals(1, lost.get(0).epoch(), "the lost entries are the epoch-1 commitments");
        }

        // And the cluster kept running as if nothing happened - the new
        // epoch committed fresh entries over the graves.
        assertTrue(result.ledger().committed().size() >= 14,
                "post-bounce writes must have committed at the overwritten offsets");
    }

    @Test
    void theLossReplaysByteIdentically() {
        assertEquals(
                Kip101Schedule.run(TruncationRule.HIGH_WATERMARK).simulation().renderTrace(),
                Kip101Schedule.run(TruncationRule.HIGH_WATERMARK).simulation().renderTrace(),
                "the anomaly must be a controlled, replayable experiment - not a flake");
    }
}

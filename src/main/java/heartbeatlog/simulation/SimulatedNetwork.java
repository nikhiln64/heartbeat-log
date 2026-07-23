package heartbeatlog.simulation;

/**
 * Seeded lossy network. Decides, per message, whether it is dropped and how
 * long it takes to arrive.
 * <p>
 * LOAD-BEARING DESIGN RULE: a message's fate is a pure function of
 * (seed, from, to, sendTime) - never of how many messages were sent before
 * it. Drawing fates from a sequential RNG stream would mean that enabling
 * epoch truncation (which adds OffsetsForLeaderEpoch messages to the run)
 * shifts the stream and silently changes every later message's fate,
 * breaking the same-seed red/green controlled experiment. Hashing the
 * coordinates instead keeps the fault pattern fixed before any message
 * exists.
 * <p>
 * Corollary: two messages on the same link at the same simulated time share
 * a fate. That correlation is accepted - it is deterministic, cheap, and
 * irrelevant to the protocol properties under test.
 */
public final class SimulatedNetwork {

    private final long seed;
    private final long minDelay;
    private final long maxDelay;
    private final double dropProbability;

    public SimulatedNetwork(long seed, long minDelay, long maxDelay, double dropProbability) {
        if (minDelay < 1 || maxDelay < minDelay) {
            throw new IllegalArgumentException("need 1 <= minDelay <= maxDelay");
        }
        if (dropProbability < 0.0 || dropProbability >= 1.0) {
            throw new IllegalArgumentException("dropProbability must be in [0, 1)");
        }
        this.seed = seed;
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.dropProbability = dropProbability;
    }

    /** Fixed 1-tick delay, no drops - for tests that want a perfect network. */
    public static SimulatedNetwork reliable() {
        return new SimulatedNetwork(0, 1, 1, 0.0);
    }

    public boolean drops(int from, int to, long sendTime) {
        // 53 high-quality bits -> uniform double in [0, 1)
        double u = (fate(from, to, sendTime, 0) >>> 11) / (double) (1L << 53);
        return u < dropProbability;
    }

    public long delay(int from, int to, long sendTime) {
        return minDelay + Math.floorMod(fate(from, to, sendTime, 1), maxDelay - minDelay + 1);
    }

    /** splitmix64 over the message coordinates; pure arithmetic, identical on any JVM. */
    private long fate(int from, int to, long sendTime, int stream) {
        long h = splitmix64(seed ^ (from * 0x9E3779B97F4A7C15L));
        h = splitmix64(h ^ (to * 0xC2B2AE3D27D4EB4FL));
        h = splitmix64(h ^ sendTime);
        return splitmix64(h ^ stream);
    }

    private static long splitmix64(long z) {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}

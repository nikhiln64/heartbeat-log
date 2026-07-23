package heartbeatlog.simulation;

/**
 * PURPOSE: this class plays the role of the (bad) home WiFi between the
 * replicas. For every message a node sends, it answers the two questions a
 * real network answers: "does this message get lost?" and "if not, how long
 * does it take to arrive?" The protocol code never knows the difference
 * between this and a real network - it just sees messages vanish or arrive
 * late.
 * <p>
 * ELI5: imagine a post office that loses some letters and delivers the rest
 * late. This particular post office works from a rulebook (the seed): for a
 * letter from house A to house B mailed at 3 o'clock, the rulebook always
 * says the same thing - "lose it" or "deliver it 4 minutes later" - no coin
 * is ever flipped at delivery time. So if you replay the whole day, every
 * letter meets exactly the same outcome, which is what lets our tests replay
 * a run and get the identical result, byte for byte.
 * <p>
 * WHY THE RULEBOOK LOOKS UP BY ADDRESS AND TIME (load-bearing detail): the
 * outcome depends only on (seed, from, to, sendTime) - NEVER on how many
 * letters were mailed before this one. If outcomes came from one shared
 * random stream instead, mailing ONE extra letter would shift every later
 * outcome in the day. Our green run does exactly that - the epoch-lookup fix
 * sends extra messages - and it must not disturb the rest of the schedule,
 * or the red/green comparison stops being a controlled experiment.
 * <p>
 * Fine print: two letters on the same route at the same instant share one
 * outcome. Accepted - it is deterministic, cheap, and irrelevant to the
 * protocol properties under test.
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

    /**
     * A perfect post office: nothing gets lost, everything arrives exactly
     * one tick later. For tests that need messages to flow without chaos.
     */
    public static SimulatedNetwork reliable() {
        return new SimulatedNetwork(0, 1, 1, 0.0);
    }

    // Each yes/no question we ask about a message gets its own constant, so
    // the drop decision and the delay decision draw two INDEPENDENT random
    // values from the same coordinates - otherwise "was it dropped?" and
    // "how slow was it?" would always be correlated.
    private static final int DROP_DECISION = 0;
    private static final int DELAY_DECISION = 1;

    public boolean drops(int from, int to, long sendTime) {
        // Top 53 bits of the random value -> a uniform double in [0, 1),
        // compared against the configured drop probability.
        long bits = randomValueFor(from, to, sendTime, DROP_DECISION);
        double uniform = (bits >>> 11) / (double) (1L << 53);
        return uniform < dropProbability;
    }

    public long delay(int from, int to, long sendTime) {
        long bits = randomValueFor(from, to, sendTime, DELAY_DECISION);
        return minDelay + Math.floorMod(bits, maxDelay - minDelay + 1);
    }

    /**
     * The single source of randomness in the network: one deterministic
     * 64-bit random value per (message coordinates, decision type).
     * <p>
     * There is no RNG object with hidden state here on purpose. The value
     * depends ONLY on the arguments and the seed, so the same message gets
     * the same answer on every run and every machine - which is what makes
     * a recorded seed replayable and the red/green runs comparable.
     * <p>
     * The multiplications by large odd constants just spread the small
     * integer inputs (node ids are 0..2) across the 64-bit space before
     * mixing, so links (0,1) and (1,0) get unrelated values.
     */
    private long randomValueFor(int from, int to, long sendTime, int decision) {
        long h = mixBits(seed ^ (from * 0x9E3779B97F4A7C15L));
        h = mixBits(h ^ (to * 0xC2B2AE3D27D4EB4FL));
        h = mixBits(h ^ sendTime);
        return mixBits(h ^ decision);
    }

    /**
     * Scrambles a 64-bit value so that similar inputs give unrelated
     * outputs: flip one input bit and about half the output bits flip
     * (the "avalanche" property). Without this, consecutive send times
     * like t=5 and t=6 would produce nearly identical "random" values and
     * the fault pattern would be visibly striped instead of random-looking.
     * <p>
     * The constants are the standard SplitMix64 finalizer (Steele et al.,
     * also used inside java.util.SplittableRandom) - pure arithmetic, no
     * state, identical result on any JVM.
     */
    private static long mixBits(long value) {
        value += 0x9E3779B97F4A7C15L;
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }
}

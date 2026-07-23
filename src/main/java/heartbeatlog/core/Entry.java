package heartbeatlog.core;

/**
 * One record in the replicated telemetry log.
 *
 * <pre>
 *   log:   [e1,0] [e1,1] [e1,2] [e2,3] [e2,4]
 *            │      │      │      │      │
 *          epoch  offset            └── epoch bumps on every leadership
 *          stamps position               change; the (epoch, offset) pair
 *          entries at append time        is what EPOCH_BOUNDARY truncation
 *                                        reasons about (KIP-101's fix)
 * </pre>
 *
 * Entries are immutable: the leader stamps its current epoch at append time
 * and neither field ever changes afterward. Divergence between replicas is
 * detected by comparing (epoch, offset) pairs, never by comparing payloads.
 *
 * @param epoch   leader epoch at append time (monotonic across leaderships)
 * @param offset  zero-based position in the log (dense, no gaps)
 * @param payload the telemetry sample itself (opaque to the protocol)
 */
public record Entry(long epoch, long offset, String payload) {
    public Entry {
        if (epoch < 0) throw new IllegalArgumentException("epoch must be >= 0, got " + epoch);
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        if (payload == null) throw new IllegalArgumentException("payload must not be null");
    }
}

package heartbeatlog.replication;

/**
 * A follower asking the leader "give me everything from offset X onward".
 *
 * Doubles as the acknowledgement: fromOffset IS the follower's log end, so
 * by fetching at X the follower proves it has stored everything below X.
 * The leader advances the high watermark from these numbers - there is no
 * separate ack message. (This is exactly how Kafka's followers ack.)
 *
 * The follower also reports the high watermark it currently believes, so
 * the leader can tell a truly up-to-date follower (put it on the waiting
 * list, stay silent) from one whose fetch crossed a watermark advance in
 * flight (answer it with the news it missed). Without this, a fetch racing
 * the final watermark advance would wait forever holding a stale watermark.
 *
 * @param epoch                 the leadership the follower believes in
 * @param followerId            who is asking
 * @param fromOffset            the follower's log end - first offset it does NOT have
 * @param followerHighWatermark the watermark the follower currently knows
 */
public record FetchRequest(long epoch, int followerId, long fromOffset, long followerHighWatermark) {}

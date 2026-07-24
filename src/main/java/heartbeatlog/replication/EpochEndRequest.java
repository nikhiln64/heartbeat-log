package heartbeatlog.replication;

/**
 * A reconciling follower asking the leader: "in YOUR log, where does epoch
 * {@code queriedEpoch} end?" Sent instead of blind truncation when the
 * truncation rule is EPOCH_BOUNDARY - the first half of Kafka's KIP-101
 * fix (Kafka calls it OffsetsForLeaderEpoch).
 *
 * @param epoch        the leadership the follower believes in (fencing)
 * @param followerId   who is asking
 * @param queriedEpoch the last epoch present in the follower's own log
 */
public record EpochEndRequest(long epoch, int followerId, long queriedEpoch) {}

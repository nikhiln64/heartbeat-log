package heartbeatlog.replication;

import heartbeatlog.core.Entry;

import java.util.List;

/**
 * The leader's answer to a fetch: new entries (possibly none) plus the
 * leader's high watermark, so followers learn how much of the log the
 * cluster has safely committed. An entries-free response still matters -
 * it is how a fully caught-up follower hears that the watermark moved.
 *
 * @param epoch               the leadership this response belongs to
 * @param fromOffset          where the entries batch starts (follower sanity check)
 * @param entries             log entries from fromOffset, in order (may be empty)
 * @param leaderHighWatermark how many entries the leader knows are committed
 */
public record FetchResponse(long epoch, long fromOffset, List<Entry> entries, long leaderHighWatermark) {}

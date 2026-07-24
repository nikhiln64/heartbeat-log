package heartbeatlog.replication;

/**
 * The leader's answer to "where does epoch E end?": the exact offset where
 * the follower must cut its log so that everything below is shared history
 * and everything above is divergent scribble from a dead leadership. The
 * follower truncates to min(its log end, endOffset) and resumes fetching.
 *
 * @param epoch        the leadership this answer belongs to (fencing)
 * @param queriedEpoch the epoch that was asked about
 * @param endOffset    where that epoch ends in the leader's log
 */
public record EpochEndResponse(long epoch, long queriedEpoch, long endOffset) {}

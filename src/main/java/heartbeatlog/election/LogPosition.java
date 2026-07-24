package heartbeatlog.election;

/**
 * A node's log fingerprint for election purposes: the epoch of its last
 * entry and how long its log is. Voters compare fingerprints to refuse
 * candidates whose logs are behind - Raft's §5.4.1 up-to-date check.
 *
 * ELI5: before voting someone class president, you check their notebook is
 * at least as complete as yours. A kid with missing pages can campaign all
 * he wants; nobody with fuller notes will vote for him, and without those
 * votes he can't win - so the winner always has every page the class
 * agreed on.
 *
 * @param lastEntryEpoch the epoch stamped on the log's final entry (0 if empty)
 * @param logEndOffset   how many entries the log holds
 */
public record LogPosition(long lastEntryEpoch, long logEndOffset) {

    /** Raft §5.4.1: later last-epoch wins; same epoch - longer log wins. */
    public boolean isAtLeastAsUpToDateAs(LogPosition other) {
        if (lastEntryEpoch != other.lastEntryEpoch) {
            return lastEntryEpoch > other.lastEntryEpoch;
        }
        return logEndOffset >= other.logEndOffset;
    }
}

package heartbeatlog.replication;

/**
 * What a replica does to its log when it must reconcile with a (possibly
 * new) leader - after a leadership change, or when coming back from a
 * crash. This flag is the experiment knob of the whole project: the same
 * fault schedule runs red under the buggy rule and will run green under
 * the epoch-based fix (added in a later commit).
 */
public enum TruncationRule {

    /**
     * The pre-KIP-101 rule: cut the log back to the local high watermark.
     * <p>
     * ELI5: "keep only the pages the teacher had already called safe LAST
     * time I heard - tear out everything after." Sounds cautious, but the
     * torn-out pages may include entries the cluster HAD committed (the
     * follower just hadn't heard the watermark move yet). Bounce leadership
     * twice, fast, and those committed entries vanish from every copy -
     * that is Kafka's KIP-101 data-loss bug, reproduced by this project's
     * red test.
     */
    HIGH_WATERMARK
}

package heartbeatlog.replication;

import heartbeatlog.core.Entry;

import java.util.ArrayList;
import java.util.List;

/**
 * The promises notebook. The moment the acting leader advances its high
 * watermark past an entry - the moment the system would tell a client
 * "your data is safe" - that entry is written here, in ink, by the test
 * harness's all-seeing hand.
 *
 * No replica can read this; it exists only so tests can later ask THE
 * question this whole project is about: is every promise still present in
 * the surviving logs? The KIP-101 red run is precisely a promise made here
 * and then broken by the cluster.
 */
public final class CommitLedger {

    private final List<Entry> committed = new ArrayList<>();

    void recordCommitted(Entry entry) {
        committed.add(entry);
    }

    /** Every entry the cluster ever claimed was safe, in commit order. */
    public List<Entry> committed() {
        return List.copyOf(committed);
    }
}

package heartbeatlog.replication;

import heartbeatlog.core.Entry;
import heartbeatlog.simulation.Simulation;
import heartbeatlog.simulation.SimulatedNetwork;

import java.util.ArrayList;
import java.util.List;

/**
 * The KIP-101 fault schedule - one deterministic scenario shared by the red
 * test (the demo, expected to fail) and the anomaly guard (in CI, asserting
 * the loss keeps reproducing).
 *
 * The recipe, on the simulated clock:
 *
 * <pre>
 *  t=0        controller appoints replica 0 leader (epoch 1)
 *  t=10..37   writes w0..w9 - commit and settle everywhere (HW=10)
 *  t=60, 62   writes w10, w11 - replica 1 STORES both, acks both...
 *  t=CRASH    ...and crashes INSIDE the one-round-trip window where the
 *             cluster has committed 12 but replica 1 still believes HW=10
 *             (its watermark news is in flight). The promise ledger says
 *             12; replica 1's private knowledge says 10.
 *  t=90       replica 0 (the old leader) crashes - nobody left who can
 *             hand w10, w11 back.
 *  t=95       replica 1 recovers: buggy rule tears its log to HW=10.
 *  t=100      controller appoints replica 1 leader (epoch 2) - the second
 *             leadership change, before it refetched what it tore.
 *  t=120,122  new writes commit at offsets 10, 11 under epoch 2 -
 *             OVERWRITING the offsets where committed w10, w11 lived.
 * </pre>
 *
 * End state under HIGH_WATERMARK: the ledger holds epoch-1 entries w10, w11
 * as committed, but no live replica has them - replica 2 conformed to the
 * shorter new leader via the out-of-range reset. Committed telemetry is
 * gone: "your Deep-sleep minutes vanished."
 */
final class Kip101Schedule {

    /** Found by scanning seeds/crash times; any (seed, crash) pair that puts
     *  the crash inside the watermark-news window works (seed 5 loses for
     *  crash times 66-69; 67 sits mid-window). Pinned so the run replays
     *  byte-identically everywhere, and so exactly w-10 and w-11 - no more,
     *  no less - are the entries that vanish. */
    static final long SEED = 5;
    static final long CRASH_TIME = 67;

    static final int COMMITTED_BEFORE_BOUNCE = 12;
    static final int CONTROLLER = 3;
    private static final long LAG_LIMIT = 20;

    record Result(Simulation simulation, List<Replica> replicas, CommitLedger ledger) {

        /** Committed promises absent from (or replaced in) the given replica's log. */
        List<Entry> lostFrom(Replica replica) {
            List<Entry> lost = new ArrayList<>();
            List<Entry> log = replica.log();
            for (Entry promised : ledger.committed()) {
                boolean present = log.size() > promised.offset()
                        && log.get((int) promised.offset()).equals(promised);
                if (!present) {
                    lost.add(promised);
                }
            }
            return lost;
        }
    }

    static Result run(TruncationRule rule) {
        return run(rule, SEED, CRASH_TIME);
    }

    static Result run(TruncationRule rule, long seed, long crashTime) {
        SimulatedNetwork network = new SimulatedNetwork(seed, 1, 4, 0.0);
        Simulation simulation = new Simulation(network);
        CommitLedger ledger = new CommitLedger();
        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Replica replica = new Replica(i, ledger, rule, LAG_LIMIT);
            replicas.add(replica);
            simulation.addNode(replica);
        }
        simulation.addNode(new ControllerStub(CONTROLLER, List.of(0, 1, 2)));

        simulation.schedule(0, CONTROLLER, new LeaderChange(0));
        for (int n = 0; n < 10; n++) {
            simulation.schedule(10 + 3L * n, 0, new ClientAppend("w-" + n));
        }
        simulation.schedule(60, 0, new ClientAppend("w-10"));
        simulation.schedule(62, 0, new ClientAppend("w-11"));
        simulation.schedule(crashTime, 1, new Crash());
        simulation.schedule(90, 0, new Crash());
        simulation.schedule(95, 1, new Recover());
        simulation.schedule(100, CONTROLLER, new LeaderChange(1));
        simulation.schedule(120, 1, new ClientAppend("post-0"));
        simulation.schedule(122, 1, new ClientAppend("post-1"));
        simulation.run();
        return new Result(simulation, replicas, ledger);
    }

    private Kip101Schedule() {}
}

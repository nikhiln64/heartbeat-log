package heartbeatlog.replication;

import heartbeatlog.simulation.Message;
import heartbeatlog.simulation.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * A stand-in for the cluster's control plane - the thing that decides who
 * leads. In production Kafka this is the controller (backed historically by
 * ZooKeeper's consensus, now by KRaft); here it is deliberately a stub that
 * obeys scripted {@link LeaderChange} events, so failover timing is exact
 * and reproducible.
 *
 * Its one non-negotiable job is the epoch counter: every appointment gets a
 * strictly larger number than every appointment before it. That
 * monotonicity is what lets replicas recognize and ignore stale decrees.
 *
 * The honest boundary, stated in the README: deciding leadership safely is
 * a consensus problem. This project demonstrates that consensus in a
 * standalone election module; the replication experiment keeps its
 * leadership changes scripted so the KIP-101 bug - a truncation bug, not an
 * election bug - is isolated from election timing.
 */
public final class ControllerStub implements Node {

    private final int id;
    private final List<Integer> replicas;
    private long epoch = 0;

    public ControllerStub(int id, List<Integer> replicas) {
        this.id = id;
        this.replicas = replicas;
    }

    @Override
    public List<Message> step(long now, Object event) {
        if (!(event instanceof LeaderChange change)) {
            throw new IllegalArgumentException("controller: unknown event " + event);
        }
        epoch++;
        LeaderAppointment decree = new LeaderAppointment(epoch, change.newLeaderId(), replicas);
        List<Message> out = new ArrayList<>();
        for (int replica : replicas) {
            out.add(new Message(id, replica, decree));
        }
        return out;
    }
}

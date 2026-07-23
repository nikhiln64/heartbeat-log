package heartbeatlog.replication;

import java.util.List;

/**
 * "You are the leader now" - the controller's decree, delivered to every
 * replica. Carries the new epoch (a number that only ever goes up, one per
 * leadership) so a late-arriving decree from an older leadership can be
 * recognized and ignored.
 *
 * In real Kafka this comes from the controller (historically ZooKeeper, now
 * KRaft); here it comes from a scripted schedule, and later from the
 * ControllerStub.
 *
 * @param epoch    the leadership number - strictly greater than all before it
 * @param leaderId which replica leads for this epoch
 * @param replicas all replica ids in the cluster (the replication group)
 */
public record LeaderAppointment(long epoch, int leaderId, List<Integer> replicas) {}

package heartbeatlog.replication;

/**
 * Scripted event aimed at the {@link ControllerStub}: "make this replica
 * the leader now." The stub turns it into a numbered decree
 * ({@link LeaderAppointment}) broadcast to every replica.
 *
 * @param newLeaderId the replica to appoint
 */
public record LeaderChange(int newLeaderId) {}

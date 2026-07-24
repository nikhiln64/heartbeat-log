package heartbeatlog.replication;

/**
 * A client asking the leader to store one telemetry sample ("write this
 * down"). Scheduled as a first-class simulation event, aimed at the leader.
 *
 * @param payload the telemetry sample, e.g. "hr=61,hrv=48"
 */
public record ClientAppend(String payload) {}

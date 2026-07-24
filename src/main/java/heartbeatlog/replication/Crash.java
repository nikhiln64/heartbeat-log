package heartbeatlog.replication;

/**
 * Scripted event: this replica loses power right now. Everything volatile
 * (role, leader bookkeeping) is gone; the log survives on disk. The replica
 * ignores every message until a {@link Recover} event brings it back.
 */
public record Crash() {}

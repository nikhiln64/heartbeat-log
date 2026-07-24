package heartbeatlog.simulation;

/**
 * Scripted event: a crashed node boots back up. It reloads its durable
 * log, applies its truncation rule (under HIGH_WATERMARK it tears the log
 * back to the last watermark it knew - KIP-101's "restart" loss scenario
 * starts exactly here), comes back as a follower, and resumes pulling from
 * the last leader it knew about. If that knowledge is stale, epoch fencing
 * keeps it harmless until the next appointment arrives.
 */
public record Recover() {}

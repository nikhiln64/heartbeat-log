package heartbeatlog.simulation;

/**
 * A message in flight between two nodes. The payload is opaque to the
 * Simulation and the network - protocol semantics live entirely in the nodes.
 */
public record Message(int from, int to, Object payload) {}

package heartbeatlog.simulation;

import java.util.List;

/**
 * A simulated process, driven exclusively by the simulation's single thread.
 *
 * The contract is a step function: one event in, zero or more outgoing
 * messages out. Implementations keep their own state between steps but must
 * never touch wall-clock time, real randomness, or threads - all
 * nondeterminism enters through the Simulation and the seeded
 * network, nowhere else.
 */
public interface Node {

    /** Handle one delivered event; return messages to send now. */
    List<Message> step(long now, Object event);
}

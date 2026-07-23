package heartbeatlog.core;

/**
 * Replica role state machine.
 *
 * <pre>
 *                 appointment / won election
 *   FOLLOWER ──────────────────────────────────▶ LEADER
 *      ▲  │                                        │
 *      │  │ ElectionTimeout                        │ sees higher
 *      │  ▼ (standalone election module)           │ epoch/term
 *   CANDIDATE ─────────────────────────────────────┘
 *      │        lost / split vote                  (steps down)
 *      └──────────▶ FOLLOWER
 * </pre>
 *
 * On the replication path (the KIP-101 experiment) leadership changes only
 * via ControllerStub appointments, so replicas move FOLLOWER <-> LEADER
 * directly. CANDIDATE exists only inside the standalone election module.
 * Crash-recover always resets the role to FOLLOWER (roles are volatile;
 * the log, currentTerm, and votedFor are the durable state).
 */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

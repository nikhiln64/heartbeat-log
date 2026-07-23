# heartbeat-log

A from-scratch, ISR-style replicated append-only log for sleep telemetry, built to demonstrate how state is replicated reliably and how consensus grants leadership - and to reproduce a real production data-loss bug (Kafka's [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation), 2017) as a controlled red/green experiment.

**The experiment:** with the pre-KIP-101 truncation rule (`HIGH_WATERMARK`), a deterministic seeded fault schedule makes acknowledged sleep samples silently vanish after two quick leader changes. Switch the truncation rule to `EPOCH_BOUNDARY`, replay the identical seed, and nothing is lost.

```
./gradlew test       # default suite - deterministic, green, includes the EPOCH_BOUNDARY twin
./gradlew redTest    # the KIP-101 red variant - EXPECTED TO FAIL; the failure is the demo
./gradlew fuzzRed    # dev-only: biased schedule fuzzing under the buggy rule
```

## Design rules (load-bearing)

- **Single-threaded, always.** The whole cluster runs inside one deterministic simulation: a priority event queue totally ordered by `(simTime, seqNo)`, a simulated clock, and a seeded per-link lossy network. No threads, no executors, no wall-clock sleeps - one thread or determinism dies, and with it the same-seed red/green experiment.
- **Fault schedules are pre-materialized.** Drops, delays, crashes, and timeouts are fixed against simulated time before a run starts - never drawn from the RNG per message - so enabling epoch truncation (which adds messages) cannot silently change the fault pattern between the red and green runs.
- **Durability over availability.** Commits require acks from the full ISR with `min.insync.replicas=2` (of 3): when the ISR shrinks below 2, the leader refuses writes. Unavailable, never lossy - the same trade Kafka makes with `acks=all` + `min.insync.replicas=2` and unclean leader election disabled.

## Where consensus lives (the ControllerStub boundary)

The replication path - and the KIP-101 experiment - takes leadership appointments and epochs from a `ControllerStub`, exactly as Kafka's brokers take them from the controller (historically ZooKeeper's ZAB, now KRaft). Consensus still exists in this system: a standalone quorum-vote election module (terms, RequestVote, majority, Raft §5.4.1's up-to-date check, persistent `currentTerm`/`votedFor`) demonstrates how those epochs are granted safely, with its own scripted tests and election-safety property. The two are deliberately not wired together: KIP-101 is a truncation bug, not an election bug, and keeping the flagship experiment on the stub keeps it isolated, deterministic, and honest about the same architectural boundary the real system has.

## Status

Scaffold (H0-1 of the build plan). Protocol, simulation kernel, and the experiment land in subsequent commits - the git log is part of the deliverable and tells the build story in order.

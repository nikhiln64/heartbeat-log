package heartbeatlog.election;

/**
 * A voter's answer. The term rides along so a candidate that fell behind
 * (its term is old news) learns about the newer term and steps down.
 *
 * @param term    the voter's current term
 * @param voterId who answered
 * @param granted whether the vote was given
 */
public record VoteReply(long term, int voterId, boolean granted) {}

package heartbeatlog.election;

/**
 * A candidate asking for a vote: "make me leader for this term - and here
 * is my log fingerprint so you can check I'm not missing anything."
 *
 * @param term        the term the candidate is standing for
 * @param candidateId who is standing
 * @param position    the candidate's log fingerprint (voters apply the
 *                    up-to-date check against their own)
 */
public record VoteRequest(long term, int candidateId, LogPosition position) {}

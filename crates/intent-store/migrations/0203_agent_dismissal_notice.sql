-- One row per questions-dismissed notice an agent was sent
-- (`agent.dismissQuestions`): the insert (INSERT OR IGNORE) is the claim, so
-- repeated or interleaved dismissals of one message id deliver one notice,
-- also across a daemon restart. Deleted with the agent.
CREATE TABLE agent_dismissal_notice (
    agent_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, message_id)
);

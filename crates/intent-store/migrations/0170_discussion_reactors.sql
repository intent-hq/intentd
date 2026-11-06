-- Local mutation identity is separate from immutable, transferable display identity.
ALTER TABLE discussion_reaction RENAME TO discussion_reaction_old;
CREATE TABLE discussion_reaction (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    principal_id TEXT,
    agent_id TEXT,
    reaction TEXT NOT NULL CHECK (reaction IN ('👍','👎','👀','✅','🙈','🙂')),
    author_json TEXT NOT NULL DEFAULT '{"principalId":null,"login":null,"displayName":null,"avatarUrl":null}',
    CHECK (principal_id IS NULL OR agent_id IS NULL),
    FOREIGN KEY(workspace_id,message_id) REFERENCES workspace_discussion_message(workspace_id,id) ON DELETE CASCADE,
    UNIQUE(message_id, principal_id, reaction),
    UNIQUE(message_id, agent_id, reaction)
);
INSERT INTO discussion_reaction(id,workspace_id,message_id,principal_id,reaction,author_json)
SELECT r.id,r.workspace_id,r.message_id,r.principal_id,r.reaction,
       json_object('principalId',r.principal_id,'login',p.login,'displayName',p.display_name,'avatarUrl',p.avatar_url)
FROM discussion_reaction_old r LEFT JOIN principal p ON p.id=r.principal_id;
DROP TABLE discussion_reaction_old;
CREATE INDEX discussion_reaction_workspace ON discussion_reaction(workspace_id);
CREATE INDEX discussion_reaction_sample ON discussion_reaction(message_id,reaction,id);
-- Seed stored samples once. History reads never aggregate the roster or join profiles.
UPDATE workspace_discussion_message AS m
SET reactions_json=(SELECT json_group_array(json_set(j.value,'$.reactors',json(
    (SELECT json_group_array(json(author_json)) FROM (
        SELECT author_json FROM discussion_reaction r
        WHERE r.message_id=m.id AND r.reaction=json_extract(j.value,'$.reaction')
        ORDER BY r.id LIMIT 20
    ))))) FROM json_each(m.reactions_json) j);

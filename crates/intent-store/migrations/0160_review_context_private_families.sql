-- Local denial history only. Never authority, owner recovery or a receipt.
CREATE TABLE review_context_private_family (
    acp_session_id TEXT PRIMARY KEY NOT NULL CHECK(length(trim(acp_session_id)) BETWEEN 1 AND 256 AND instr(acp_session_id,char(0))=0),
    version INTEGER NOT NULL CHECK(version=1)
);
INSERT INTO review_context_private_family(acp_session_id,version)
SELECT DISTINCT acp_session_id,1 FROM review_context_enrollment;
CREATE TRIGGER review_context_private_family_update_guard
BEFORE UPDATE ON review_context_private_family BEGIN
    SELECT RAISE(ABORT,'private native family denial is immutable');
END;
CREATE TRIGGER review_context_private_family_delete_guard
BEFORE DELETE ON review_context_private_family BEGIN
    SELECT RAISE(ABORT,'private native family denial is immutable');
END;
CREATE TRIGGER review_context_private_family_enroll
AFTER INSERT ON review_context_enrollment BEGIN
    INSERT INTO review_context_private_family(acp_session_id,version)
    VALUES(NEW.acp_session_id,1) ON CONFLICT(acp_session_id) DO NOTHING;
END;
CREATE TRIGGER review_context_private_family_retire
BEFORE DELETE ON review_context_enrollment BEGIN
    INSERT INTO review_context_private_family(acp_session_id,version)
    VALUES(OLD.acp_session_id,1) ON CONFLICT(acp_session_id) DO NOTHING;
END;

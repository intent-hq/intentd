-- Daemon-local selection continuity, not permission or verified target provenance.
-- No deleting FK: root removal, replacement and same-ID recreation retain tombstones.
-- Counters are independent checked SQLite integers; live admission still needs its
-- original process/connection/root lifetime. Neither timestamps nor paths prove it.
CREATE TABLE repository_selection_state (
  workspace_id TEXT NOT NULL,
  root_kind TEXT NOT NULL CHECK(root_kind IN ('primary','registered')),
  root_id TEXT NOT NULL,
  root_incarnation INTEGER NOT NULL,
  selection_revision INTEGER NOT NULL,
  root_present INTEGER NOT NULL,
  repository_path TEXT,
  worktree_path TEXT,
  is_remote INTEGER,
  registered_path TEXT,
  choice_incarnation INTEGER NOT NULL,
  choice_mode TEXT NOT NULL,
  remote_name TEXT,
  historical_source TEXT,
  historical_record_id TEXT,
  PRIMARY KEY(workspace_id,root_kind,root_id)
) WITHOUT ROWID;

CREATE TRIGGER repository_selection_no_delete BEFORE DELETE ON repository_selection_state
BEGIN SELECT RAISE(ABORT,'repository selection tombstone cannot be deleted'); END;
CREATE TRIGGER repository_selection_no_replace BEFORE INSERT ON repository_selection_state
WHEN EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id
  AND root_kind=NEW.root_kind AND root_id=NEW.root_id)
BEGIN SELECT RAISE(ABORT,'repository selection tombstone cannot be replaced'); END;
CREATE TRIGGER repository_selection_monotonic BEFORE UPDATE ON repository_selection_state
WHEN NEW.workspace_id IS NOT OLD.workspace_id OR NEW.root_kind IS NOT OLD.root_kind
 OR NEW.root_id IS NOT OLD.root_id OR NEW.root_incarnation < OLD.root_incarnation
 OR NEW.selection_revision <= OLD.selection_revision
BEGIN SELECT RAISE(ABORT,'repository selection continuity must advance'); END;

CREATE TRIGGER repository_selection_validate_insert BEFORE INSERT ON repository_selection_state
WHEN typeof(NEW.root_incarnation)<>'integer' OR NEW.root_incarnation<=0
 OR typeof(NEW.selection_revision)<>'integer' OR NEW.selection_revision<=0
 OR typeof(NEW.choice_incarnation)<>'integer' OR NEW.choice_incarnation<=0
 OR NEW.choice_incarnation>NEW.root_incarnation
 OR NEW.root_present NOT IN (0,1) OR typeof(NEW.root_present)<>'integer'
 OR NEW.root_kind NOT IN ('primary','registered')
 OR (NEW.root_kind='primary' AND (NEW.root_id<>'' OR NEW.registered_path IS NOT NULL OR NEW.is_remote NOT IN (0,1)))
 OR (NEW.root_kind='registered' AND (NEW.root_id='' OR NEW.registered_path IS NULL
   OR NEW.repository_path IS NOT NULL OR NEW.worktree_path IS NOT NULL OR NEW.is_remote IS NOT NULL))
 OR NEW.choice_mode NOT IN ('never','reset','automatic','remote','unresolved')
 OR (NEW.choice_mode='remote' AND (NEW.remote_name IS NULL OR length(NEW.remote_name)=0))
 OR (NEW.choice_mode<>'remote' AND NEW.remote_name IS NOT NULL)
 OR (NEW.choice_mode<>'unresolved' AND (NEW.historical_source IS NOT NULL OR NEW.historical_record_id IS NOT NULL))
 OR (NEW.historical_source IS NULL)<>(NEW.historical_record_id IS NULL)
 OR (NEW.historical_source IS NOT NULL AND
   (NEW.historical_source NOT IN ('workspace-metadata','registered-root-metadata') OR length(NEW.historical_record_id)=0))
BEGIN SELECT RAISE(ABORT,'invalid repository selection state or exhausted counter'); END;

CREATE TRIGGER repository_selection_validate_update BEFORE UPDATE ON repository_selection_state
WHEN typeof(NEW.root_incarnation)<>'integer' OR NEW.root_incarnation<=0
 OR typeof(NEW.selection_revision)<>'integer' OR NEW.selection_revision<=0
 OR typeof(NEW.choice_incarnation)<>'integer' OR NEW.choice_incarnation<=0
 OR NEW.choice_incarnation>NEW.root_incarnation
 OR NEW.root_present NOT IN (0,1) OR typeof(NEW.root_present)<>'integer'
 OR NEW.root_kind NOT IN ('primary','registered')
 OR (NEW.root_kind='primary' AND (NEW.root_id<>'' OR NEW.registered_path IS NOT NULL OR NEW.is_remote NOT IN (0,1)))
 OR (NEW.root_kind='registered' AND (NEW.root_id='' OR NEW.registered_path IS NULL
   OR NEW.repository_path IS NOT NULL OR NEW.worktree_path IS NOT NULL OR NEW.is_remote IS NOT NULL))
 OR NEW.choice_mode NOT IN ('never','reset','automatic','remote','unresolved')
 OR (NEW.choice_mode='remote' AND (NEW.remote_name IS NULL OR length(NEW.remote_name)=0))
 OR (NEW.choice_mode<>'remote' AND NEW.remote_name IS NOT NULL)
 OR (NEW.choice_mode<>'unresolved' AND (NEW.historical_source IS NOT NULL OR NEW.historical_record_id IS NOT NULL))
 OR (NEW.historical_source IS NULL)<>(NEW.historical_record_id IS NULL)
 OR (NEW.historical_source IS NOT NULL AND
   (NEW.historical_source NOT IN ('workspace-metadata','registered-root-metadata') OR length(NEW.historical_record_id)=0))
BEGIN SELECT RAISE(ABORT,'invalid repository selection state or exhausted counter'); END;

INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
SELECT r.id,'primary','',1,1,1,r.repository_path,r.worktree_path,r.is_remote,NULL,1,
 CASE WHEN r.repository_owner IS NOT NULL OR r.repository_name IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
 CASE WHEN r.repository_owner IS NOT NULL OR r.repository_name IS NOT NULL THEN 'workspace-metadata' END,CASE WHEN r.repository_owner IS NOT NULL OR r.repository_name IS NOT NULL THEN r.id END
FROM workspace r;

INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
SELECT r.workspace_id,'registered',r.id,1,1,1,NULL,NULL,NULL,r.path,1,
 CASE WHEN r.repo_owner IS NOT NULL OR r.repo_name IS NOT NULL OR r.registered_commit_sha IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
 CASE WHEN r.repo_owner IS NOT NULL OR r.repo_name IS NOT NULL OR r.registered_commit_sha IS NOT NULL THEN 'registered-root-metadata' END,CASE WHEN r.repo_owner IS NOT NULL OR r.repo_name IS NOT NULL OR r.registered_commit_sha IS NOT NULL THEN r.id END
FROM workspace_git_root r;

CREATE TRIGGER repository_selection_workspace_insert AFTER INSERT ON workspace
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NEW.repository_path,worktree_path=NEW.worktree_path,is_remote=NEW.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN 'workspace-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN NEW.id ELSE historical_record_id END
  WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.id,'primary','',1,1,1,NEW.repository_path,NEW.worktree_path,NEW.is_remote,NULL,1,
    CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN 'workspace-metadata' END,CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN NEW.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='');
END;

CREATE TRIGGER repository_selection_workspace_delete AFTER DELETE ON workspace
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=OLD.repository_path,worktree_path=OLD.worktree_path,is_remote=OLD.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL) THEN 'workspace-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL) THEN OLD.id ELSE historical_record_id END
  WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.id,'primary','',1,1,0,OLD.repository_path,OLD.worktree_path,OLD.is_remote,NULL,1,
    CASE WHEN OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL THEN 'workspace-metadata' END,CASE WHEN OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL THEN OLD.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='');
END;

CREATE TRIGGER repository_selection_workspace_binding AFTER UPDATE OF id,repository_path,worktree_path,is_remote ON workspace
WHEN (NEW.id IS NOT OLD.id OR NEW.repository_path IS NOT OLD.repository_path OR NEW.worktree_path IS NOT OLD.worktree_path OR NEW.is_remote IS NOT OLD.is_remote) AND NOT (NEW.id IS NOT OLD.id)
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NEW.repository_path,worktree_path=NEW.worktree_path,is_remote=NEW.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN 'workspace-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN NEW.id ELSE historical_record_id END
  WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.id,'primary','',1,1,1,NEW.repository_path,NEW.worktree_path,NEW.is_remote,NULL,1,
    CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN 'workspace-metadata' END,CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN NEW.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='');
END;

CREATE TRIGGER repository_selection_workspace_move AFTER UPDATE OF id,repository_path,worktree_path,is_remote ON workspace
WHEN NEW.id IS NOT OLD.id
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=OLD.repository_path,worktree_path=OLD.worktree_path,is_remote=OLD.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL) THEN 'workspace-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL) THEN OLD.id ELSE historical_record_id END
  WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.id,'primary','',1,1,0,OLD.repository_path,OLD.worktree_path,OLD.is_remote,NULL,1,
    CASE WHEN OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL THEN 'workspace-metadata' END,CASE WHEN OLD.repository_owner IS NOT NULL OR OLD.repository_name IS NOT NULL THEN OLD.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='');
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NEW.repository_path,worktree_path=NEW.worktree_path,is_remote=NEW.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN 'workspace-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL) THEN NEW.id ELSE historical_record_id END
  WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.id,'primary','',1,1,1,NEW.repository_path,NEW.worktree_path,NEW.is_remote,NULL,1,
    CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN 'workspace-metadata' END,CASE WHEN NEW.repository_owner IS NOT NULL OR NEW.repository_name IS NOT NULL THEN NEW.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='');
END;

CREATE TRIGGER repository_selection_root_insert AFTER INSERT ON workspace_git_root
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=NEW.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN 'registered-root-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN NEW.id ELSE historical_record_id END
  WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.workspace_id,'registered',NEW.id,1,1,1,NULL,NULL,NULL,NEW.path,1,
    CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN 'registered-root-metadata' END,CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN NEW.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id);
END;

CREATE TRIGGER repository_selection_root_delete AFTER DELETE ON workspace_git_root
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=OLD.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL) THEN 'registered-root-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL) THEN OLD.id ELSE historical_record_id END
  WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.workspace_id,'registered',OLD.id,1,1,0,NULL,NULL,NULL,OLD.path,1,
    CASE WHEN OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL THEN 'registered-root-metadata' END,CASE WHEN OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL THEN OLD.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id);
END;

CREATE TRIGGER repository_selection_root_binding AFTER UPDATE OF id,workspace_id,path ON workspace_git_root
WHEN (NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id OR NEW.path IS NOT OLD.path) AND NOT (NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id)
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=NEW.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN 'registered-root-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN NEW.id ELSE historical_record_id END
  WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.workspace_id,'registered',NEW.id,1,1,1,NULL,NULL,NULL,NEW.path,1,
    CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN 'registered-root-metadata' END,CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN NEW.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id);
END;

CREATE TRIGGER repository_selection_root_move AFTER UPDATE OF id,workspace_id,path ON workspace_git_root
WHEN NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=OLD.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL) THEN 'registered-root-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL) THEN OLD.id ELSE historical_record_id END
  WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.workspace_id,'registered',OLD.id,1,1,0,NULL,NULL,NULL,OLD.path,1,
    CASE WHEN OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL THEN 'registered-root-metadata' END,CASE WHEN OLD.repo_owner IS NOT NULL OR OLD.repo_name IS NOT NULL OR OLD.registered_commit_sha IS NOT NULL THEN OLD.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id);
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=NEW.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END,
    choice_mode=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN 'unresolved' ELSE choice_mode END,
    historical_source=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN 'registered-root-metadata' ELSE historical_source END,
    historical_record_id=CASE WHEN choice_mode='never' AND (NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL) THEN NEW.id ELSE historical_record_id END
  WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.workspace_id,'registered',NEW.id,1,1,1,NULL,NULL,NULL,NEW.path,1,
    CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN 'unresolved' ELSE 'never' END,NULL,
    CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN 'registered-root-metadata' END,CASE WHEN NEW.repo_owner IS NOT NULL OR NEW.repo_name IS NOT NULL OR NEW.registered_commit_sha IS NOT NULL THEN NEW.id END
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id);
END;

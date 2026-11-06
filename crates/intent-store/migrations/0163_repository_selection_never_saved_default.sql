-- intent-hq/intent#7449: owner/name metadata is not a saved repository choice.
--
-- 0146's lifecycle triggers turned a never-saved root into 'unresolved' history
-- whenever the workspace or registered root carried owner/name (or a registered
-- commit SHA). Every new GitHub workspace therefore started as "selection
-- required", and only the repo-less PR read worked around it. Recreate the eight
-- triggers so a root without a saved choice stays 'never' (read as Automatic);
-- binding, incarnation and revision bookkeeping is unchanged, and a choice the
-- user actually saved still becomes unresolved history across same-ID recreation
-- (repository_selection_repo::project's incarnation guard).

DROP TRIGGER repository_selection_workspace_insert;
CREATE TRIGGER repository_selection_workspace_insert AFTER INSERT ON workspace
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NEW.repository_path,worktree_path=NEW.worktree_path,is_remote=NEW.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.id,'primary','',1,1,1,NEW.repository_path,NEW.worktree_path,NEW.is_remote,NULL,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='');
END;

DROP TRIGGER repository_selection_workspace_delete;
CREATE TRIGGER repository_selection_workspace_delete AFTER DELETE ON workspace
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=OLD.repository_path,worktree_path=OLD.worktree_path,is_remote=OLD.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.id,'primary','',1,1,0,OLD.repository_path,OLD.worktree_path,OLD.is_remote,NULL,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='');
END;

DROP TRIGGER repository_selection_workspace_binding;
CREATE TRIGGER repository_selection_workspace_binding AFTER UPDATE OF id,repository_path,worktree_path,is_remote ON workspace
WHEN (NEW.id IS NOT OLD.id OR NEW.repository_path IS NOT OLD.repository_path OR NEW.worktree_path IS NOT OLD.worktree_path OR NEW.is_remote IS NOT OLD.is_remote) AND NOT (NEW.id IS NOT OLD.id)
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NEW.repository_path,worktree_path=NEW.worktree_path,is_remote=NEW.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.id,'primary','',1,1,1,NEW.repository_path,NEW.worktree_path,NEW.is_remote,NULL,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='');
END;

DROP TRIGGER repository_selection_workspace_move;
CREATE TRIGGER repository_selection_workspace_move AFTER UPDATE OF id,repository_path,worktree_path,is_remote ON workspace
WHEN NEW.id IS NOT OLD.id
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=OLD.repository_path,worktree_path=OLD.worktree_path,is_remote=OLD.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.id,'primary','',1,1,0,OLD.repository_path,OLD.worktree_path,OLD.is_remote,NULL,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.id AND root_kind='primary' AND root_id='');
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NEW.repository_path,worktree_path=NEW.worktree_path,is_remote=NEW.is_remote,registered_path=NULL,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='';
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.id,'primary','',1,1,1,NEW.repository_path,NEW.worktree_path,NEW.is_remote,NULL,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.id AND root_kind='primary' AND root_id='');
END;

DROP TRIGGER repository_selection_root_insert;
CREATE TRIGGER repository_selection_root_insert AFTER INSERT ON workspace_git_root
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=NEW.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.workspace_id,'registered',NEW.id,1,1,1,NULL,NULL,NULL,NEW.path,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id);
END;

DROP TRIGGER repository_selection_root_delete;
CREATE TRIGGER repository_selection_root_delete AFTER DELETE ON workspace_git_root
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=OLD.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.workspace_id,'registered',OLD.id,1,1,0,NULL,NULL,NULL,OLD.path,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id);
END;

DROP TRIGGER repository_selection_root_binding;
CREATE TRIGGER repository_selection_root_binding AFTER UPDATE OF id,workspace_id,path ON workspace_git_root
WHEN (NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id OR NEW.path IS NOT OLD.path) AND NOT (NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id)
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=NEW.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.workspace_id,'registered',NEW.id,1,1,1,NULL,NULL,NULL,NEW.path,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id);
END;

DROP TRIGGER repository_selection_root_move;
CREATE TRIGGER repository_selection_root_move AFTER UPDATE OF id,workspace_id,path ON workspace_git_root
WHEN NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id
BEGIN
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=0,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=OLD.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT OLD.workspace_id,'registered',OLD.id,1,1,0,NULL,NULL,NULL,OLD.path,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=OLD.workspace_id AND root_kind='registered' AND root_id=OLD.id);
  UPDATE repository_selection_state SET root_incarnation=root_incarnation+1,
    selection_revision=selection_revision+1,root_present=1,repository_path=NULL,worktree_path=NULL,is_remote=NULL,registered_path=NEW.path,
    choice_incarnation=CASE WHEN choice_mode='never' THEN root_incarnation+1 ELSE choice_incarnation END
  WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id;
  INSERT INTO repository_selection_state (workspace_id,root_kind,root_id,root_incarnation,selection_revision,root_present,repository_path,worktree_path,is_remote,registered_path,choice_incarnation,choice_mode,remote_name,historical_source,historical_record_id)
  SELECT NEW.workspace_id,'registered',NEW.id,1,1,1,NULL,NULL,NULL,NEW.path,1,
    'never',NULL,NULL,NULL
  WHERE NOT EXISTS(SELECT 1 FROM repository_selection_state WHERE workspace_id=NEW.workspace_id AND root_kind='registered' AND root_id=NEW.id);
END;

-- One-time repair. Only these triggers and the 0146 backfill ever wrote a
-- historical_source: every user write (automatic/remote/reset) clears it, and
-- imported roots are marked 'unresolved' without one. So this never touches a
-- saved choice, a reset, or an unproven import. The revision advances so the
-- monotonic trigger accepts the write and cached snapshots are invalidated.
UPDATE repository_selection_state
SET selection_revision=selection_revision+1,choice_incarnation=root_incarnation,
  choice_mode='never',remote_name=NULL,historical_source=NULL,historical_record_id=NULL
WHERE choice_mode='unresolved' AND historical_source IS NOT NULL;

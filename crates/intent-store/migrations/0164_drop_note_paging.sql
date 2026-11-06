-- Remove the note paging system (intentd#2298 and follow-ups).
--
-- Migrations 0149-0156 stay in place because released databases have applied
-- them. This drops every trigger they created and every table except
-- note_page_entry and its parent note_page_head. On a 17 GB database dropping
-- note_page_entry (14.5M rows) here held startup for about two minutes, so the
-- daemon removes those two in short batches after startup
-- (intent-store note_paging_cleanup). Nothing reads or writes them any more.
-- The canonical note, comment and note_line_attribution rows are untouched:
-- the paging tables only held derived indexes, operation receipts and staged
-- edits.
--
-- Triggers first: several fire on note, comment and note_line_attribution.
DROP TRIGGER IF EXISTS note_page_insert;
DROP TRIGGER IF EXISTS note_page_update;
DROP TRIGGER IF EXISTS note_annotation_insert;
DROP TRIGGER IF EXISTS note_annotation_update;
DROP TRIGGER IF EXISTS note_annotation_legacy_insert;
DROP TRIGGER IF EXISTS note_annotation_legacy_update;
DROP TRIGGER IF EXISTS note_comment_thread_count_insert;
DROP TRIGGER IF EXISTS note_comment_thread_count_update;
DROP TRIGGER IF EXISTS note_comment_thread_count_delete;
DROP TRIGGER IF EXISTS note_comment_projection_insert;
DROP TRIGGER IF EXISTS note_comment_projection_delete;
DROP TRIGGER IF EXISTS note_comment_insert;
DROP TRIGGER IF EXISTS note_comment_update;
DROP TRIGGER IF EXISTS note_comment_delete;
DROP TRIGGER IF EXISTS note_comment_anchor_insert;
DROP TRIGGER IF EXISTS note_comment_anchor_delete;
DROP TRIGGER IF EXISTS note_comment_anchor_update;
DROP TRIGGER IF EXISTS note_annotation_state_exhaustion;
DROP TRIGGER IF EXISTS note_annotation_state_advance;
DROP TRIGGER IF EXISTS note_annotation_state_move;
DROP TRIGGER IF EXISTS note_annotation_state_delete;
DROP TRIGGER IF EXISTS note_annotation_head_state_insert;
DROP TRIGGER IF EXISTS note_annotation_head_state_update;
DROP TRIGGER IF EXISTS note_page_head_state_insert;
DROP TRIGGER IF EXISTS note_page_head_state_update;
DROP TRIGGER IF EXISTS note_comment_detail_insert;
DROP TRIGGER IF EXISTS note_comment_detail_update;
DROP TRIGGER IF EXISTS note_attribution_author_fragment;
DROP TRIGGER IF EXISTS note_annotation_match_invalidate;
DROP TRIGGER IF EXISTS note_comment_anchor_cover_insert;
DROP TRIGGER IF EXISTS note_comment_anchor_cover_update;
DROP TRIGGER IF EXISTS note_annotation_retirement_note_insert;
DROP TRIGGER IF EXISTS note_annotation_retirement_note_update;
DROP TRIGGER IF EXISTS note_annotation_retirement_comment_insert;
DROP TRIGGER IF EXISTS note_annotation_retirement_comment_update;
DROP TRIGGER IF EXISTS note_annotation_retirement_attribution_insert;
DROP TRIGGER IF EXISTS note_annotation_retirement_attribution_update;
DROP TRIGGER IF EXISTS note_operation_reclaim_insert;
DROP TRIGGER IF EXISTS note_stage_root_reclaim_insert;
DROP TRIGGER IF EXISTS note_stage_pin_insert;
DROP TRIGGER IF EXISTS note_stage_pin_delete;
DROP TRIGGER IF EXISTS note_stage_reclaim_insert;
DROP TRIGGER IF EXISTS note_stage_reclaim_phase;
DROP TRIGGER IF EXISTS note_stage_reclaim_delete;
DROP TRIGGER IF EXISTS note_stage_preserve_deleted_piece;
DROP TRIGGER IF EXISTS note_stage_preserve_moved_piece;

-- Dependent tables before the tables they reference. Dropping a table also
-- drops its indexes; dropping the R*Tree drops its shadow tables.
DROP TABLE IF EXISTS note_annotation_match_head;
DROP TABLE IF EXISTS note_annotation_state;
DROP TABLE IF EXISTS note_annotation_workspace_retirement;
DROP TABLE IF EXISTS note_artifact_source;
DROP TABLE IF EXISTS note_attribution_author_piece;
DROP TABLE IF EXISTS note_comment_anchor_cover;
DROP TABLE IF EXISTS note_comment_anchor_extent;
DROP TABLE IF EXISTS note_comment_detail_piece;
DROP TABLE IF EXISTS note_comment_projection;
DROP TABLE IF EXISTS note_comment_root;
DROP TABLE IF EXISTS note_comment_thread;
DROP TABLE IF EXISTS note_operation_detail;
DROP TABLE IF EXISTS note_operation_item;
DROP TABLE IF EXISTS note_operation_reclaim;
DROP TABLE IF EXISTS note_operation_reference;
DROP TABLE IF EXISTS note_operation_scalar;
DROP TABLE IF EXISTS note_operation_source;
DROP TABLE IF EXISTS note_operation_text;
DROP TABLE IF EXISTS note_page_backend;
DROP TABLE IF EXISTS note_page_piece;
DROP TABLE IF EXISTS note_stage_base_piece;
DROP TABLE IF EXISTS note_stage_record;
DROP TABLE IF EXISTS note_stage_root_pin;
DROP TABLE IF EXISTS note_stage_root_reclaim;
DROP TABLE IF EXISTS note_stage_search_input;
DROP TABLE IF EXISTS note_stage_search_range;
DROP TABLE IF EXISTS note_stage_stream;
DROP TABLE IF EXISTS note_stage_text_piece;
DROP TABLE IF EXISTS note_stage_validation;
DROP TABLE IF EXISTS note_stage_view_piece;
DROP TABLE IF EXISTS note_annotation_snapshot;
DROP TABLE IF EXISTS note_attribution_author;
DROP TABLE IF EXISTS note_comment_anchor;
DROP TABLE IF EXISTS note_comment_detail;
DROP TABLE IF EXISTS note_stage_chunk;
DROP TABLE IF EXISTS note_stage_text;
DROP TABLE IF EXISTS note_stage_view;
DROP TABLE IF EXISTS note_attribution_line;
DROP TABLE IF EXISTS note_stage;
DROP TABLE IF EXISTS note_annotation_head;
DROP TABLE IF EXISTS note_operation;
DROP TABLE IF EXISTS note_stage_root;

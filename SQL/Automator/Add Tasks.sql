-- ====================================================================================
-- 1: Start transaction (this lets you ROLLBACK if the INSERT doesn't work as intended)
-- ====================================================================================
BEGIN;

-- ================
-- 2: Insert Task 1
-- ================
INSERT INTO
    automator.t_auto_tasks (namex, comments, operational, ownerx, owner_email, department, manual_time, manual_recurrence, auto_recurrence, recurrence_day_of_month, auto_recurrence_day, recurrence_hour, created_date, data_source, sql_database, data_source_id, data_storage_type, data_storage_id, storage_type, storage_id, file_name, dynamic_name, after_before, wb_sheet_name, wb_start_row, wb_end_row, wb_start_column, wb_end_column, append, insert_timestamp, tables_populated, dependencies, no_data_notification, no_data_is_error, run_requested)
    VALUES ('Project Backlog Planning - Risk Management', 'This task populates the daily inflow, outflow, netflow, and work in progress metrics for the working queues within Risk Management.', 'Operational', 'Robert', 'robert.anderson@vivintsolar.com', 'ART/ERT', '1', 'Daily', 'Daily', NULL, NULL, NULL, TO_TIMESTAMP('09/20/2019 11:13:11 AM', 'mm/dd/yyyy hh:mi:ss am'), 'SQL', 'Data Warehouse', 'https://drive.google.com/open?id=1bDV0EfrgHZrgpVm_WRAL1Km8miBmstt2', 'Google Sheets', 'https://docs.google.com/spreadsheets/d/1qhT1Ucdc9OvnrkLvsWO7FUs2XbNJ-3m0UvzNcOg306c/edit#gid=2031721361', NULL, NULL, NULL, NULL, NULL, 'Data', '2', NULL, '1', NULL, 'False', 'False', NULL, NULL, 'True', 'False', 'TRUE')
;

-- ========================
-- 3: Check Automator table
-- ========================
SELECT
    namex, comments, operational, ownerx, owner_email, department, manual_time, manual_recurrence, auto_recurrence, recurrence_day_of_month, auto_recurrence_day, recurrence_hour, created_date, data_source, sql_database, data_source_id, data_storage_type, data_storage_id, storage_type, storage_id, file_name, dynamic_name, after_before, wb_sheet_name, wb_start_row, wb_end_row, wb_start_column, wb_end_column, append, insert_timestamp, tables_populated, dependencies, no_data_notification, no_data_is_error, run_requested
FROM
    automator.t_auto_tasks
ORDER BY
    id DESC
LIMIT 50
;

-- =============================
-- 4: Rollback or Commit Changes
-- =============================
ROLLBACK; -- If it don't look good.
COMMIT; -- If it do.
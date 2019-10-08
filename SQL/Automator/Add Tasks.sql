-- ====================================================================================
-- 1: Start transaction (this lets you ROLLBACK if the INSERT doesn't work as intended)
-- ====================================================================================
BEGIN;

USE ROLE AUTOMATOR_SUPER_R;

-- ================
-- 2: Insert Task 1
-- ================
insert into automator.t_auto_tasks (
  namex,
  ownerx,
  manual_time,
  manual_recurrence,
  auto_recurrence,
  auto_recurrence_day,
  data_source,
  data_source_id,
  data_storage_type,
  data_storage_id,
  storage_type,
  storage_id,
  file_name,
  dynamic_name,
  after_before,
  wb_sheet_name,
  wb_start_column,
  wb_start_row,
  owner_email,
  operational,
  department,
  run_requested,
  wb_end_row,
  wb_end_column,
  created_date,
  recurrence_hour,
  no_data_notification,
  no_data_is_error,
  tables_populated,
  append,
  insert_timestamp,
  sql_database,
  dependencies,
  comments
)
values (
  'Delinquency Saves', --namex
  'Mack', --ownerx
  5, --manual_time
  'Monthly', --manual_recurrence
  'Monthly', --auto_recurrence
  null, --auto_recurrence_day
  'SQL Command', --data_source
  '1_CYVtGg1J6z8YH7ZcDct9FfCIlPx5I3F', --data_source_id
  null, --data_storage_type
  null, --data_storage_id
  null, --storage_type
  null, --storage_id
  null, --file_name
  null, --dynamic_name,
  null, --after_before
  null, --wb_sheet_name
  null, --wb_start_column
  null, --wb_start_row
  'mackenzie.damavandi@vivintsolar.com', --owner_email
  'Operational', --operational
  'Asset Management', --department
  'FALSE', --run_requested
  null, --wb_end_row
  null, --wb_end_column
  current_timestamp::timestamp_ntz, --created_date
  10, --recurrence_hour
  null, --no_data_notification,
  true, --no_data_is_error,
  'D_CAPITAL_MKTS.T_DELINQUENCY_SAVES', --tables_populated
  false, --append,
  false, --insert_timestamp,
  'Data Warehouse', --sql_database
  null, --dependencies
  'This query creates d_capital_mkts.t_delinquency_saves. It refreshes at 10am on the first of every month. The data is used for a MSTR report.' --comments
);

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
-- 4: Rollback or Commit Changes-- =============================
ROLLBACK; -- If it don't look good.
COMMIT; -- If it do.
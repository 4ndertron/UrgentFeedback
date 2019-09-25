-- ====================================================================================
-- 1: Start transaction (this lets you ROLLBACK if the INSERT doesn't work as intended)
-- ====================================================================================
BEGIN;

-- ================
-- 2: Insert Task 1
-- ================
DELETE
-- SELECT ID, NAMEX, OWNERX
FROM AUTOMATOR.T_AUTO_TASKS
WHERE ID IN (1031, 960, 959, 958, 942, 941, 940, 939, 938, 937, 933, 931, 929, 875)
;

-- ========================
-- 3: Check Automator table
-- ========================
SELECT ID
     , namex
     , operational
     , ownerx
     , data_source
     , data_source_id
     , data_storage_type
     , data_storage_id
     , wb_sheet_name
     , run_requested
FROM automator.t_auto_tasks
WHERE OWNERX = 'Robert'
--   AND OPERATIONAL IN ('Non-Operational', 'Disabled')
ORDER BY id DESC
LIMIT 50
;

-- =============================
-- 4: Rollback or Commit Changes
-- =============================
ROLLBACK; -- If it don't look good.
COMMIT; -- If it do.
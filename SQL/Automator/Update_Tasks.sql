SELECT * FROM AUTOMATOR.t_auto_tasks WHERE NAMEX ILIKE '%ROOT%';

--*****!!!!! ALWAYS BEGIN THE SESSION BEFORE UPDATING THE TABLE !!!!!*****--
BEGIN;
USE ROLE AUTOMATOR_SUPER_R;

--*****!!!!! WAIT TO UPDATE UNTIL YOU BEGIN THE SESSION !!!!!*****--
UPDATE AUTOMATOR.t_auto_tasks SET RUN_REQUESTED = TRUE WHERE OWNERX ILIKE '%ROBERT%' AND NAMEX ILIKE '%CX PERFORMANCE%';

--*****!!!!! WAIT TO UPDATE UNTIL YOU BEGIN THE SESSION !!!!!*****--
DELETE FROM AUTOMATOR.T_AUTO_TASKS WHERE ID IN (933, 929, 931, 875, 958, 959, 960, 937, 938, 939, 940, 941, 942);

--*****!!!!! ALWAYS COMMIT AFTER YOU TEST YOUR UPDATE !!!!!*****--
COMMIT;
--*****!!!!! ALWAYS ROLLBACK IF THE TEST LOOKS WRONG !!!!!*****--
ROLLBACK;

SELECT * FROM AUTOMATOR.t_auto_tasks WHERE NAMEX ILIKE '%STABLE%' ORDER BY ID DESC;

SELECT DISTINCT OPERATIONAL FROM AUTOMATOR.T_AUTO_TASKS AS T;
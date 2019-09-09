SELECT * FROM d_post_install.t_auto_tasks WHERE NAMEX ILIKE '%CX PERFORMANCE%';

--*****!!!!! ALWAYS BEGIN THE SESSION BEFORE UPDATING THE TABLE !!!!!*****--
BEGIN;

--*****!!!!! WAIT TO UPDATE UNTIL YOU BEGIN THE SESSION !!!!!*****--
UPDATE d_post_install.t_auto_tasks SET RUN_REQUESTED = TRUE WHERE OWNERX ILIKE '%ROBERT%' AND NAMEX ILIKE '%CX PERFORMANCE%';

--*****!!!!! ALWAYS COMMIT AFTER YOU TEST YOUR UPDATE !!!!!*****--
COMMIT;
--*****!!!!! ALWAYS ROLLBACK IF THE TEST LOOKS WRONG !!!!!*****--
ROLLBACK;

SELECT * FROM d_post_install.t_auto_tasks WHERE NAMEX ILIKE '%STABLE%' ORDER BY ID DESC;

SELECT DISTINCT OPERATIONAL FROM D_POST_INSTALL.T_AUTO_TASKS AS T;
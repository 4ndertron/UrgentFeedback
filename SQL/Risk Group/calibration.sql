WITH LIST1 AS (
    /*
     https://docs.google.com/spreadsheets/d/1SDUq-pLOIohkXtY1N5ZGfry69dStdQMWzm2mXMk2xUA/edit?ts=5df79cf2#gid=2051437666
     */
    SELECT 'super' AS ATTITUDE
)
   , LIST2 AS (
    SELECT 'dooper' AS ATTITUDE
)

   , MAIN AS (
    SELECT *
    FROM (
                 (SELECT * FROM LIST1)
                 UNION
                 (SELECT * FROM LIST2)
         )
)

   , TEST_CTE AS (
    SELECT DISTINCT *
    FROM MAIN
)

SELECT *
FROM MAIN
;
/* Goal) "A list of any transfers that we have done in New York following a customer's death."
 * 
 * 1) processed transfer cases that have a secondary reason of �deceased customer�
 * 2) The subject will be transfer
 * 3) The primary reason of home transfer
 * 
 */

SELECT C.CASE_NUMBER      AS "Case Number"
     , P.SERVICE_NAME     AS "Service Number"
     , CT.FULL_NAME       AS "Customer Name"
     , C.SUBJECT          AS "Case Subject"
     , C.PRIMARY_REASON   AS "Primary Reason"
     , C.SECONDARY_REASON AS "Secondary Reason"
     , P.SERVICE_STATE    AS "Service State"
     , C.CREATED_DATE     AS "Case Created Date"
     , C.STATUS           AS "Case Status"
FROM RPT.T_CASE AS C
         LEFT OUTER JOIN RPT.T_CONTACT AS CT
                         ON CT.CONTACT_ID = C.CONTACT_ID
         LEFT OUTER JOIN RPT.T_PROJECT AS P
                         ON P.PROJECT_ID = C.PROJECT_ID
WHERE C.RECORD_TYPE = 'Solar - Transfer'
  AND C.SECONDARY_REASON = 'Customer Deceased'
  AND P.SERVICE_STATE = 'NY'
ORDER BY C.CREATED_DATE
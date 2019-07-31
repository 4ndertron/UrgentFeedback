WITH T1 AS (
    SELECT P.SERVICE_NAME
         , P.SOLAR_BILLING_ACCOUNT_NUMBER
         , CT.FULL_NAME AS PRIMARY_CONTACT
         , p.SERVICE_ADDRESS
         , P.SERVICE_CITY
         , P.SERVICE_COUNTY
         , P.SERVICE_STATE
         , P.SERVICE_ZIP_CODE
         , C.CASE_NUMBER
         , C.LAST_MODIFIED_BY
         , C.LAST_MODIFIED_DATE
         , C.CREATED_DATE

    FROM RPT.T_CASE AS C
             LEFT JOIN
         RPT.T_PROJECT AS P
         ON P.PROJECT_ID = C.PROJECT_ID
             LEFT JOIN
         RPT.T_CONTACT AS CT
         ON CT.CONTACT_ID = C.CONTACT_ID
             LEFT JOIN
         RPT.V_SF_USER AS USR
         ON USR.ID = C.LAST_MODIFIED_BY_ID
    WHERE C.RECORD_TYPE = 'Solar - Transfer'
      AND C.STATUS = 'In Progress'
      AND P.SERVICE_STATE = 'CA'
)

/*
 Populate with one tab, Open Transfer cases of In Progress...
 That allows, with service number, case number,
 One column for ownership... Dropdown with LD or VSLR. In Progress || Completed.
 Every Hour. Remove the colmpleted crecords from the page during hourly refresh.

 Pull notes from notes for contact information...

 Second tab with comments for scrub to collect the contact information.

 Trust LD to do the filings. Not happening. Transfers is suffering.
Agent's don't know where the account is in the filings process.
Give them visibility into Luna.
Just get done within 24 hours.
In Progress, signal brittany to terminate filing.
Audit making sure there's TA, original.
Put on spreadsheet & send to LD.
Or they create the termination themselves.
How do you share the workload, while having visibility.
Transfer would be a comment. Release would be a task as SF.
Or take out of Salesforce, into a google sheet.
Open In Progress, pull out and into the sheet. Batch by time frames. Noon to 4 is LD. All else is VSLR.
Split the batches from LD to filings.
One tab is LD and one is Filings.
LD marks an account complete in the bacth.
Then the SF does not mark the complete.... Need to audit? Or fix?
 */

SELECT *
FROM T1
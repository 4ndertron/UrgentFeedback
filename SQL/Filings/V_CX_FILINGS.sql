/*
 Inbox:
 ------
 Response Time
    What platform is the inbox located in? How do you find the start and end indicators for the Inbox response time?
 -
 24/48 hrs.
 Team delegated G-mail inbox.... May need IT support for that? Where do I get access to that information?
 Separated by folders in the inbox
 Use timestamp on the email and response if available.
 -

 APH
    Where are the inbox records stored?
 -
 Every agent creates an activity for every email they work on. That will be the best way to find an APH metric for the inbox.
 -


 Daily Transfers:
 ----------------
 Turn Around Time
    How do you find the start and end indicators for the Daily Terminations turn around time?
 -
24/48 hrs.
 Transfers and Refinance
 Activity codes for refinance. Transfers in the cases.
 Not ready activity code.
 In progress for started.
 Completed.

 Not audit properly prepped.
 Gets returnned... Not always filings, if provided incorrect information.
 Relesaed incorrectly... Not a good way to find that information.
 If we don't have updated... Feedback as an email.
 All emails are turned into a task... unkown when the original email comes in...
 Refinance Request Sheet.
 -

 Error Rate
    What are the errors that can happen, and who catches them? How are they returned? Where are the errors stored?
 -
 The most definitive way to get
 -
 */

WITH T1 AS (
    /*
     Placeholder query
     */
    SELECT DISTINCT C.OWNER
    FROM RPT.T_CASE AS C
    WHERE C.RECORD_TYPE ILIKE '%TRANSFER%'
)

/*
 Required Fields:
 ----------------
 Employee
 Employee ID
 Business Title
 Direct Manager
 Supervisory Org


 */

SELECT *
FROM T1
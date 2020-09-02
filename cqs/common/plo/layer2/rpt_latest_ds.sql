/*
rpt_latest_ds PLO

Notes: This PLO lists all the disposition states with the latest disposition event for a given subject 
        flagged to be the latest disposition event.

Revision History: 12-Sep-2016 Michelle Engler - New header and add Comprehend Update Time
                  14-Dec-2016 Adam Kaus - bug fix on join to latest_ds CTE which excluded records with null dsstdtc
                  14-Jan-2017 Michelle Engler - Add comments for tech writer
*/

create table rpt_latest_ds as
-- Latest DS uses the max disposition event sequence number to determine the latest disposition.  Note that
--  it is possible that the latest disposition has a null disposition date, in which case, it will be included
--  in a null dsstdtc_by_month which will not be included in the KPI
WITH latest_ds AS (
  SELECT
    a.dsseq,
    a.comprehendid,
    to_char(ds.dsstdtc, 'YYYY-MM') AS dsstdtc_by_month
  FROM (
         SELECT
           max(ds.dsseq) AS dsseq,
           ds.comprehendid
         FROM rpt_subject_disposition ds
         GROUP BY ds.comprehendid) a
    JOIN rpt_subject_disposition ds
      ON (a.comprehendid = ds.comprehendid AND a.dsseq = ds.dsseq)
)
-- All disposition events are included in the select below.  If the disposition 
--  is the latest disposition (meaning it is found in the latest_ds cte), then it 
--  will be flagged as latest.  Even the null disposition events will be flagged 
--  as the latest; however, they will be provided under a null dsstdtc_by_month 
--  value
SELECT DISTINCT
  ds.dsseq::NUMERIC AS dsseq,
  ds.comprehendid::TEXT AS comprehendid,
  ds.objectuniquekey::TEXT AS objectuniquekey,
  to_char(ds.dsstdtc, 'YYYY-MM')::TEXT AS dsstdtc_by_month,
  ds.usubjid::TEXT AS usubjid,
  ds.dsterm::TEXT AS dsterm,
  ds.dslabel::TEXT AS dslabel,
  ds.dscat::TEXT AS dscat,
  ds.dsscat::TEXT AS dsscat,
  ds.dsevent::TEXT AS dsevent,
  ds.dswithdrawn::BOOLEAN AS dswithdrawn,
  ds.studyid::TEXT AS studyid,
  study.studyname::TEXT AS studyname,
  ds.siteid::TEXT AS siteid,
  site.sitecountry::TEXT AS sitecountry,
  site.sitename::TEXT AS sitename,
  site.comprehendid::TEXT AS sitekey,
  (CASE WHEN latest_ds.comprehendid IS NULL
    THEN FALSE
  ELSE TRUE END)::BOOLEAN AS is_latest_ds,
  NOW()::TIMESTAMP AS comprehend_update_time
FROM rpt_subject_disposition ds
  JOIN site site
    ON
      site.siteid = ds.siteid AND
      site.studyid = ds.studyid
  JOIN study
    ON
      study.studyid = ds.studyid
  LEFT JOIN latest_ds
    ON
      ds.comprehendid = latest_ds.comprehendid AND
      ds.dsseq = latest_ds.dsseq AND
      coalesce(dsstdtc_by_month, 'x') = coalesce(latest_ds.dsstdtc_by_month, 'x');

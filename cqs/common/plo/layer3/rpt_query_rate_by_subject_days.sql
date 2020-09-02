/*
rpt_query_rate_by_subject_days PLO

Notes: Query Rate by Subject Days PLO is a dual purpose PLO.  It contains the queries AND all the subject days
        for each subject provided across a time series

Revision History: 02-Sep-2016 Michelle Engler - Added comment and made subject days count to start from configurable enroll/consent date
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  09-Nov-2016 Adam Kaus - Added logic to handle missing ds dates. If start date or exit date is missing then subject days is null
                  14-Nov-2016 Michelle Engler - Removed 5.1 per tP 18829
                  16-Nov-2016 Michelle Engler - Apply fix per tp18947 to handle  missing data on query/ds side at build time
		              09-Jan-2017 Michelle Engler - Make subject day count consistent with other plos
                  09-Jan-2017 Palaniraja Dhanavelu - Updated code for subject days calcualtion to make it consistent with other PLO's - tp 20739
                  14-Jan-2017 Michelle Engler - Add comments for tech writer
                  14-Mar-2017 Adam Kaus - Revision history will be maintained via git history from this point forward
*/

create table rpt_query_rate_by_subject_days as
with 
    -- Get the disposition state per study used to determine the subject start date.
    -- This is configurable per study within comprehendcodelist and populated in rpt_subject_days
    study_start_disposition AS (
        SELECT DISTINCT studyid, subjectstartdisposition AS startdisposition 
        FROM rpt_subject_days
    ),        
                   
    months AS (
            -- min_date is used to find the start date for the time series
            WITH min_date AS (
                    SELECT studyid, min(dt.min_dt) as date
                    FROM (SELECT studyid, min(dsstdtc)::date as min_dt from rpt_subject_disposition group by studyid
                            UNION
                          SELECT studyid, min(queryopeneddate) as min_dt from query group by studyid
                         ) dt
                    GROUP BY STUDYID
            ),
            -- Generic time series that runs for each study from the minimum date (calculated above) to the current date)
            month_starts AS (
              SELECT studyid, generate_series(date_trunc('month', greatest(min_date."date", '1/1/1970'::date)), date_trunc('month', now()), '1 mon'::interval)::date AS month_start
                FROM min_date
            )
        SELECT studyid, month_start, least(now()::date, (month_start + interval '1 month' - interval '1 day')::date) AS month_end
          FROM month_starts
    ),

    -- Get the subject days per month. If no record exists in rpt_subject_days for the subject/month
    -- then default to 0 subject days
    subject_days AS (
        SELECT s.comprehendid, 
                m.month_start AS month, 
                CASE WHEN sd.comprehendid IS NULL THEN 0 ELSE sd.thismonthsubjectdays END AS subject_days, 
                CASE WHEN sd.comprehendid IS NULL THEN 0 ELSE sd.cumulativesubjectdays END AS subject_days_cumulative
        FROM months m
        JOIN subject s ON (m.studyid = s.studyid)
        LEFT JOIN rpt_subject_days sd ON (s.comprehendid = sd.comprehendid AND m.month_start = sd.thismonth)
    ),

    -- All Query CTE joins all subjects to the generated series of months
    --  Note that the generated series of months starts from the minimum disposition date
    --  thus if a query open date is prior to that, the query will be excluded
    all_query AS (
      WITH subject_months AS (
        -- Cross join of subjects and months used to fill gaps in time series.
        SELECT subject.comprehendid, month_start AS month
          FROM subject, months
         WHERE subject.studyid = months.studyid
      ),
      -- Use the above CTE's to get the query counts
      all_query_raw_breakdown AS (
          SELECT query.comprehendid, date_trunc('month', queryopeneddate)::date AS month, count(query.*) AS query_count
            FROM query
        GROUP BY query.comprehendid, month
      )
      SELECT sm.comprehendid, sm.month,
             coalesce(query_count, 0) AS query_count,
             sum(coalesce(query_count, 0)) OVER (PARTITION BY sm.comprehendid ORDER BY sm.month) AS query_count_cumulative
        FROM subject_months AS sm LEFT JOIN all_query_raw_breakdown AS aqrb
          ON sm.comprehendid = aqrb.comprehendid AND sm.month = aqrb.month
    ),

    -- Get the open queries by month using the queryopeneddate and queryclosedate
    --  compared against the time series period start date / end date to determine
    --  status of the queries.
    open_query AS (
      WITH query_state_by_month AS (
        SELECT query.comprehendid, queryid, month_start AS month,
               (CASE
                 WHEN month_end < queryopeneddate THEN  -- The query hasn't been opened yet.
                   false
                 WHEN querycloseddate IS NULL THEN  -- The query is currently open.
                   true
                 ELSE  -- The query has already been closed.
                   month_end < querycloseddate
               END) AS is_open
          FROM query, months
         WHERE query.studyid = months.studyid
      )
        SELECT comprehendid, month, sum(is_open::integer) AS open_query_count
          FROM query_state_by_month
      GROUP BY comprehendid, month
    ),

    -- Create a table that uses the configuration of the comprehendcodelist SUBJECT DAY START
    --  to determine if a subject has been "started" or not.  Note that the terminology of the column
    --  name in the plo as "isenrolled" is misleading and is being kept due to legacy.  Instead, the 
    --  is enrolled column reflects the "started" status of a subject rather than the enrolled status of a subject
    subject_extended AS (
      WITH study_subjects AS (
        SELECT DISTINCT ds.comprehendid FROM rpt_subject_disposition ds 
        WHERE ds.dsstdtc IS NOT NULL
        AND dsterm = study_start_disposition

      )
      SELECT subject.comprehendid::text, subject.studyid::text, subject.siteid::text, subject.usubjid::text,
             study.studyname::text, site.siteregion::text, site.sitecountry::text, site.sitename::text,
             (subject.comprehendid IN (SELECT * FROM study_subjects))::boolean AS is_enrolled
        FROM study, site, subject
       WHERE study.comprehendid = site.studyid AND site.comprehendid = subject.sitekey
    )

SELECT subject.*, coalesce(sd.month, aq.month)::text AS month,
       sd.subject_days::integer, sd.subject_days_cumulative::integer,
       coalesce(aq.query_count,0)::integer as query_count, 
       coalesce(aq.query_count_cumulative,0)::integer as query_count_cumulative,
       coalesce(oq.open_query_count, 0)::integer AS open_query_count,
       clh.startdisposition as subject_day_start,
       now()::timestamp as comprehend_update_time
  FROM subject_extended AS subject /*LEFT*/ JOIN -- MDE: Commenting this out until application can handle the nulls
       subject_days AS sd ON subject.comprehendid = sd.comprehendid LEFT JOIN
       all_query AS aq ON subject.comprehendid = aq.comprehendid AND sd.month = aq.month LEFT JOIN
       open_query AS oq ON subject.comprehendid = oq.comprehendid AND sd.month = oq.month LEFT JOIN
       study_start_disposition as clh on (subject.studyid = clh.studyid);

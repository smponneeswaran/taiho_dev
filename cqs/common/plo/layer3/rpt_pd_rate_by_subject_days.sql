/*
rpt_pd_rate_by_subject_days PLO
CDM Version: 2.6

Revision History: 04-Jan-2017 sshort - Create PD Rate / Subject Days PLO
                  09-Jan-2017 Adam Kaus - Adjusted dv_data to ensure study and site data appears for records without dv data
                  10-Jan-2017 Michelle Engler - Refactored to synchronize subject days
                  10-Jan-2017 Palaniraja Dhanavelu - Updated to make the plo consider the subjects with Failed screen status and subjects having dv startdate/enddate before the subject start date - tp 208
                  14-Mar-2017 Adam Kaus - Revision history will be maintained via git history from this point forward
*/

CREATE TABLE rpt_pd_rate_by_subject_days AS
    WITH 
        /* Earliest and Latest date for the series - Expected to be different for different PLOs*/
        earliest_date as (
            SELECT coalesce(dv.studyid,ds.studyid) as studyid, least(min(dv.dvstdtc),min(ds.dsstdtc))::date as min_dt, greatest(max(dv.dvstdtc),max(ds.dsstdtc))::date as max_dt from dv full join rpt_subject_disposition ds on (dv.studyid = ds.studyid) group by 1
        ),

        /* Subject Dates - Start and Exiting Dates */
        subject_dates as (
            SELECT s.comprehendid, s.studyid, s.siteid, s.usubjid,
                    sd.subjectdaystartdt AS subject_start_date,
                    sd.exitdate AS exit_date,
                    sd.subjectexitdisposition
             FROM subject s 
             LEFT JOIN (SELECT DISTINCT comprehendid, subjectdaystartdt, exitdate, subjectexitdisposition
                        FROM rpt_subject_days) sd ON (s.comprehendid = sd.comprehendid) ),

        /* custom needed for DV's to account for dv's that happen before the subject start date */
        dv_dates as (
            SELECT comprehendid,
                   min(least(dvstdtc, dvendtc)) as min_dt
            FROM dv
            GROUP BY 1
        ),

        -- Generate a monthly date series per subject
        -- The start date is the earliest date between the subject start date or the subject's first deviation date. 
        -- If both of those are null then use the earliest deviation or disposition date for the study.
        -- The end date is the last day of the current month.
        date_series AS (
            SELECT comprehendid, 
                    studyid,
                    siteid,
                    usubjid,
                    subject_start_date,
                    exit_date,
                    subjectexitdisposition,
                    period_starting_date,
                    (period_starting_date + interval '1 month' - interval '1 day')::date AS period_ending_date
            FROM (SELECT sd.comprehendid, 
                            sd.studyid,
                            sd.siteid,
                            sd.usubjid,
                            sd.subject_start_date,
                            sd.exit_date,
                            sd.subjectexitdisposition,
                            generate_series( date_trunc( 'month',  greatest(coalesce(least(dd.min_dt, sd.subject_start_date), ed.min_dt)::date, '1/1/1970'::date) )::date, 
                                                (date_trunc( 'month', now() )::date + interval '1 month' - interval '1 day')::date
                                                , interval '1 month' )::date AS period_starting_date 
                    FROM subject_dates sd
                    JOIN earliest_date ed ON (sd.studyid = ed.studyid)
                    LEFT JOIN dv_dates dd ON (sd.comprehendid = dd.comprehendid) ) a ),

        -- Get the subject days per month. If no record exists in rpt_subject_days for the subject/month
        -- then default to 0 subject days
        subject_days as (
            select  ser.comprehendid, 
                    ser.studyid, 
                    ser.siteid, 
                    ser.usubjid, 
                    ser.subject_start_date, 
                    ser.exit_date, 
                    ser.subjectexitdisposition,
                    ser.period_starting_date, 
                    ser.period_ending_date, 
                    CASE WHEN sd.comprehendid IS NULL THEN 0 ELSE sd.thismonthsubjectdays END AS subject_day_this_month
            from date_series ser 
            left join rpt_subject_days sd on (sd.comprehendid = ser.comprehendid and sd.thismonth = ser.period_starting_date )
        ),

  dv_data as (
  SELECT subject.comprehendid,
         site.sitecro,
         subject.studyid,
         study.studyname,
         subject.siteid,
         site.sitename,
         site.sitecountry,
         site.siteregion,
         subject.usubjid,
         dv.visit,
         dv.formid,
         dv.dvcat,
         dv.dvterm,
         dv.dvstdtc,
         dv.dvendtc,
         dv.dvscat,
         dv.dvseq,
         dv.objectuniquekey,
         sd.period_starting_date AS month_trunc,
         /*
         The following columns are added purely for the subject days calculation. Note that because of the join on sd,
         dv.dvstdtc is equal to sd.period_starting_date, so we only need the above property
          */
         sd.subject_day_this_month,
         sd.subject_start_date,
         sd.exit_date,
         ds.dsseq AS exit_seq
  FROM study 
       JOIN site on (study.studyid = site.studyid) 
       JOIN subject on (site.comprehendid = subject.sitekey)
       LEFT JOIN subject_days sd on (subject.comprehendid = sd.comprehendid)
       LEFT JOIN dv on (sd.comprehendid = dv.comprehendid and sd.period_starting_date = date_trunc('month',dv.dvstdtc))
       LEFT JOIN (SELECT comprehendid, dsterm, max(dsseq) dsseq FROM rpt_subject_disposition ds GROUP BY 1, 2) ds ON (sd.comprehendid = ds.comprehendid AND lower(sd.subjectexitdisposition) = lower(ds.dsterm))
),

-- Grab the counts of protocol deviations per subject per month
pd_counts as
(
  select comprehendid,
         date_trunc('month', dvstdtc)::date as month_trunc,
         count(*) as pd_count
  from dv
  where dvstdtc is not null
  group by 1, 2
)

-- This is the main query - go ahead and construct the table with information from the other CTEs
SELECT
  dv_data.comprehendid::text as comprehendid,
  dv_data.sitecro::text as sitecro,
  dv_data.studyid::text as studyid,
  dv_data.studyname::text as studyname,
  dv_data.siteid::text as siteid,
  dv_data.sitename::text as sitename,
  dv_data.sitecountry::text as sitecountry,
  dv_data.siteregion::text as siteregion,
  dv_data.usubjid::text as usubjid,
  dv_data.visit::text as visit,
  dv_data.formid::text as formid,
  dv_data.dvcat::text as dvcat,
  dv_data.dvterm::text as dvterm,
  dv_data.dvstdtc::date as dvstdtc,
  dv_data.dvendtc::date as dvendtc,
  dv_data.dvscat::text as dvscat,
  dv_data.dvseq::integer as dvseq,
  /*
  We need to include rows for subjects whom are enrolled in the study, have had PDs in the past, but had no PDs occur during these specific months.
  This approach is needed to ensure that we calculate subject days properly, and will need to be removed once we create a subject days PLO.
   */
  coalesce(pd_counts.pd_count, 1)::integer as weight,
  coalesce(dv_data.subject_day_this_month, 0)::integer as subject_day_this_month,
  dv_data.month_trunc::date as month_trunc,
  dv_data.subject_start_date::date as subject_start_date,
  dv_data.exit_date::date as subject_exit_date,
  dv_data.exit_seq::int as subject_exit_seq,
  coalesce(dv_data.objectuniquekey, random()::text || random()::text || random()::text)::text as objectuniquekey, 
  now()::timestamp as comprehend_update_time
FROM dv_data
left join pd_counts on (dv_data.comprehendid = pd_counts.comprehendid and dv_data.month_trunc = pd_counts.month_trunc)
where dv_data.month_trunc is not null;


/*
rpt_site_performance PLO

Notes: The site performance PLO is bringing together information, counts, etc. in order to support the Site Performance KPI. Known redundancy
        in the information included in this PLO exists in comparison with other PLOs. 

Revision History: 23-Sep-2016 Huy Dang - Initial Version
                  30-Dec-2016 Huy Dang - Change AE, PD count and subject days query conditions
                  09-Jan-2016 Adam Kaus - Updated AE count to be as of the first of the month of subject start (to be consistent with other PLOs)
                  14-Jan-2017 Michelle Engler - Add comments for tech writer
                  14-Mar-2017 Adam Kaus - Revision history will be maintained via git history from this point forward
*/
create table rpt_site_performance as
with 
        -- Get counts of subjects that have had disposition events of Enrolled, Failed Screen, Screened, and Withdrawn.  Note that disposition events
        --  with a null disposition date are excluded.
        dsc as (select ds.studyid, ds.siteid,
                    sum(case when ds.dsevent = 'ENROLLED' then 1::integer else 0::integer end) as enrolled_count,
                    sum(case when ds.dsevent = 'FAILED_SCREEN' then 1::integer else 0::integer end) as failed_screen_count,
                    sum(case when ds.dsevent = 'ENROLLED' then 1::integer when ds.dsevent = 'FAILED_SCREEN' then 1::integer else 0::integer end) as subjects_screened_count,
                    sum(case when ds.dsevent = 'WITHDRAWN' then 1::integer else 0::integer end) as withdrawn_count,
                    sum(case when ds.dsevent = 'EARLY_TERMINATION_TREATMENT' then 1::integer else 0::integer end) as early_eot_count
             from rpt_subject_disposition ds
             where ds.dsstdtc is not null
             group by 1,2),
-- Get the number of days a site has been active as determined by the site activation date and site deactivation dates (or current date if site
--   deactivation is null or greater than today).  Sites with a null siteactivationdate are excluded
    site_active as (select site.studyid, site.siteid,
                           case when site.siteactivationdate is null then 0::integer
                                when (site.sitedeactivationdate is not null and site.sitedeactivationdate > current_date)
                                  or (site.sitedeactivationdate is null) then (current_date - site.siteactivationdate)::integer
                           else (site.sitedeactivationdate - site.siteactivationdate)::integer end as site_active_days_count
                    from site
                    where site.siteactivationdate is not null),
-- CTE to assemble Subject Dates in terms of the subject start date and exit date.
-- If the record does not exist in rpt_subject_days then default to 0 for totalsubjectdays.
    subject_dates as (
        SELECT s.comprehendid, s.studyid, s.siteid, s.usubjid,
                sd.subjectdaystartdt AS subject_start_date,
                sd.exitdate AS exit_date,
                CASE WHEN sd.comprehendid IS NULL THEN 0 ELSE sd.totalsubjectdays END AS totalsubjectdays
         FROM subject s
         LEFT JOIN (SELECT DISTINCT comprehendid, subjectdaystartdt, exitdate, max(totalsubjectdays) AS totalsubjectdays
                    FROM rpt_subject_days
                    GROUP BY 1, 2, 3) sd ON (s.comprehendid = sd.comprehendid) ),

-- CTE to get total subject day counts (no time series)
    sbjday as (
        select study.studyid,
               site.siteid,
               coalesce(sum(sd.totalsubjectdays), 0) as subject_days_count
        from study
        join site on (study.studyid = site.studyid)
        join subject on (site.comprehendid = subject.sitekey)
        left join subject_dates sd on (subject.comprehendid = sd.comprehendid)        
        group by 1,2
    ),

-- Select all open queries determined by query closed date equaling null 
    query as (select query.studyid, query.siteid, count(*) as open_query_count
              from query
              where query.querycloseddate is null
              group by 1,2),

-- Minimum date for the items to include is the minimum date for all disposition events for the study
    min_date as (select studyid, min(dsstdtc)::date as min_dt from rpt_subject_disposition group by studyid),

-- AE counts includes all AEs with the AE start date that is greater than or equal to the month of the subject start date
--    and the AE start date is less than the current date
    ae as (select ae.studyid, ae.siteid, count(1) as ae_count
           from ae 
           join subject_dates sd on (ae.comprehendid = sd.comprehendid)
           join min_date md on (ae.studyid = md.studyid)
           where ae.aestdtc is not null
             and ae.aestdtc >= date_trunc('MONTH', md.min_dt)
             and ae.aestdtc >= greatest(coalesce(date_trunc('MONTH', sd.subject_start_date), date_trunc('MONTH', md.min_dt)), '1/1/1970'::date) -- make sure ae_start_date >= month of subject_start_date (Informed Consent date/Enrolled date)
             and ae.aestdtc <= now()::date
           group by 1,2),

-- DV counts included where the DV Start Date is not null
    dv as (select dv.studyid, dv.siteid, count(*) as dv_count
           from dv
           where dvstdtc is not null
           group by 1,2)

select site.comprehendid,
       study.studyid,
       study.studyname,
       site.siteid,
       site.sitename,
       site.sitecro,
       site.sitecountry,
       site.siteregion,
       site.sitecraname,

       dsc.subjects_screened_count,
       dsc.failed_screen_count,
       site_active.site_active_days_count,
       dsc.enrolled_count,
       dsc.withdrawn_count,
       dsc.early_eot_count,
       query.open_query_count,
       ae.ae_count,
       dv.dv_count,
       sbjday.subject_days_count,

       case when dsc.subjects_screened_count = 0 or dsc.subjects_screened_count is null or dsc.failed_screen_count is null          then 0::numeric else dsc.failed_screen_count::numeric/dsc.subjects_screened_count::numeric end as screen_failure_rate,
       case when site_active.site_active_days_count = 0 or site_active.site_active_days_count is null or dsc.enrolled_count is null then 0::numeric else dsc.enrolled_count::numeric/site_active.site_active_days_count::numeric*30::numeric end as enrollment_rate,
       case when dsc.enrolled_count = 0 or dsc.enrolled_count is null or dsc.withdrawn_count is null                                then 0::numeric else dsc.withdrawn_count::numeric/dsc.enrolled_count::numeric end as withdrawal_rate,
       case when sbjday.subject_days_count = 0 or sbjday.subject_days_count is null or query.open_query_count is null               then 0::numeric else query.open_query_count::numeric/sbjday.subject_days_count::numeric end as open_query_rate,
       case when sbjday.subject_days_count = 0 or sbjday.subject_days_count is null or ae.ae_count is null                          then 0::numeric else ae.ae_count::numeric/sbjday.subject_days_count::numeric end as ae_rate,
       case when sbjday.subject_days_count = 0 or sbjday.subject_days_count is null or dv.dv_count is null                          then 0::numeric else dv.dv_count::numeric/sbjday.subject_days_count::numeric end as pd_rate,
       now()::timestamp as comprehend_update_time
from study
    join site on (study.studyid = site.studyid)
    left join site_active on (site.studyid = site_active.studyid and site.siteid = site_active.siteid)
    left join ae on (site.studyid = ae.studyid and site.siteid = ae.siteid)
    left join dsc on (site.studyid = dsc.studyid and site.siteid = dsc.siteid)
    left join dv on (site.studyid = dv.studyid and site.siteid = dv.siteid)
    left join query on (site.studyid = query.studyid and site.siteid = query.siteid)
    left join sbjday on (site.studyid = sbjday.studyid and site.siteid = sbjday.siteid);


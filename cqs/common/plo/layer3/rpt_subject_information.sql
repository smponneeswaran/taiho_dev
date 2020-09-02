/*
rpt_subject_information

Notes: Adding PLO that includes counts at the subject level for study risk analytics

Revision History 27-Aug-2016 MDE New PLO
                 31-Aug-2016 Adam Kaus - Added dsterm to where clauses
                 12-Sep-2016 Michelle Engler - Add Comprehend Update Time and remove last_refresh_ts
                 09-Nov-2016 Adam Kaus - Added logic to handle missing ds dates. If start date or exit date is missing then subject days is null
                 14-Nov-2016 Michelle Engler - Removed 5.1 per tp 18829
                 14-Mar-2017 Adam Kaus - Revision history will be maintained via git history from this point forward
*/

create table rpt_subject_information as
with 
     -- Get the disposition state per study used to determine the subject start date.
     -- This is configurable per study within comprehendcodelist and populated in rpt_subject_days
     study_start_disposition AS (SELECT DISTINCT studyid, subjectstartdisposition AS startdisposition 
                                 FROM rpt_subject_days),     
 
     dsc as (select comprehendid, dsseq, min(dsstdtc) dsstdtc
                from rpt_subject_disposition
                where dsevent = 'CONSENTED'
                group by 1,2),

     dse as (select comprehendid, dsseq, min(dsstdtc) as dsstdtc
                from rpt_subject_disposition
                where dsevent = 'ENROLLED'
                group by 1,2),

     prior_dv as (select comprehendid, count(*) as dv_count
            from dv
            where dvstdtc is not null and
                    dvstdtc < (now()::date - interval '30 days')::date
            group by comprehendid),

     prior_ae as (select comprehendid, count(*) as ae_count
            from ae
            where aeterm is not null and
                aestdtc is not null and
                aestdtc < (now()::date - interval '30 days')::date
            group by comprehendid),

     ae as (select comprehendid, count(*) as ae_count
            from ae
            where aeterm is not null and
                    aestdtc is not null
            group by comprehendid),

     dv as (select comprehendid, count(*) as dv_count
            from dv
            where dvterm is not null and
                    dvstdtc is not null
            group by comprehendid)

select subject.comprehendid,
       site.sitecro,
       study.studyid,
       study.studyname,
       site.siteid,
       site.sitename,
       site.sitecountry,
       site.siteregion,
       subject.usubjid,
       subject.status as subject_status,
       dsc.dsstdtc as consent_date,
       dse.dsstdtc as enrollment_date,
       sd.subjectdaystartdt as subject_start_date,
       sd.exitdate as exit_date,
       sd.totalsubjectdays as subject_days,
       -- When the disposition date is not null, calculate the number of days from the disposition date to the exitdate (if populated)
       --     or the current date minus 30 days
       --     Note that the first part of case statement is checking for negative subject days and if negative subject days, setting the days to 0
       --        and the second part (per else statement) uses the same logic but returns the actual subject days (which are expected to be positive)
       case when sd.subjectdaystartdt is null or (sd.subjectexitdisposition is not null and sd.exitdate is null) then null
            when coalesce(((
        -- If the exitdate happened more than 30 days ago, then use the exitdate
                                (case when sd.exitdate is not null and now()::date - sd.exitdate > 30  then sd.exitdate

             -- If the exitdate happened less than or equal to 30 days ago, then use the current date minus 30 days
                                     when sd.exitdate is not null and now()::date - sd.exitdate <= 30 then (now()::date - interval '30 days')::date

             -- If there is no exitdate, then use the current date minus 30 days
                                     else (now()::date - interval '30 days')::date
                                end)

          -- and subtract the disposition date.  Add 1 to make the count include the enddate.
                            - sd.subjectdaystartdt + 1)::integer
                         ), 0) < 0 then 0::integer
            else
                 coalesce(
                         ((
                                (case when sd.exitdate is not null and now()::date - sd.exitdate > 30  then sd.exitdate
                                     when sd.exitdate is not null and now()::date - sd.exitdate <= 30 then (now()::date - interval '30 days')::date
                                     else (now()::date - interval '30 days')::date
                                end)
                            - sd.subjectdaystartdt + 1)::integer
                         ), 0)
            end as prior_30day_subject_days,
       coalesce(ae.ae_count,0)::integer as ae_count,
       coalesce(prior_ae.ae_count,0)::integer as prior_30day_ae_count,
       coalesce(dv.dv_count,0)::integer as dv_count,
       coalesce(prior_dv.dv_count,0)::integer as prior_30day_dv_count,
       clh.startdisposition  as subject_day_start,
       now()::timestamp as comprehend_update_time
from study
    join site on (study.studyid = site.studyid)
    join subject on (site.comprehendid = subject.sitekey)
    left join dsc on (subject.comprehendid = dsc.comprehendid)
    left join dse on (subject.comprehendid = dse.comprehendid)
    left join (select distinct comprehendid, subjectdaystartdt, exitdate, subjectexitdisposition, max(totalsubjectdays) AS totalsubjectdays
                from rpt_subject_days
                group by 1, 2, 3, 4) sd on (subject.comprehendid = sd.comprehendid)
    left join ae on (subject.comprehendid = ae.comprehendid)
    left join dv on (subject.comprehendid = dv.comprehendid)
    left join prior_ae on (subject.comprehendid = prior_ae.comprehendid)
    left join prior_dv on (subject.comprehendid = prior_dv.comprehendid)
    left join study_start_disposition clh on (study.studyid = clh.studyid);

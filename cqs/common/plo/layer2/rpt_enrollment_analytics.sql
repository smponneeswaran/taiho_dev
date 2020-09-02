/*
rpt_enrollment_analytics PLO
Notes: 
     
Revision History: 12-Jul-2016 MDE - Adding new PLO
                  15-Aug-2016 ACK - Fixing bug where comprehendid selected twice
                  31-Aug-2016 Adam Kaus - Added dsterm to where clause for filtering by enrollment
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  16-Nov-2016 Michelle Engler - Apply fix to prevent against nulls when null DS per TP18947
                  17-NOV-2016 Palaniraja Dhanavelu - Applying fix to prevent the division by zero error as per TP18947 
                  18-NOV-2016 Michelle Engler - Further fix for protecting against division by zero
                  20-Mar-2017 Palaniraja Dhanavelu From this point forward, revision history will be maintained in GitHub, exclusively
*/

create table rpt_enrollment_analytics as
select  comprehendid,
        therapeuticarea,
        program,
        studyid,
        studyname,
        studystartdate,
        studycompletiondate,
        currentdate,
        planned_fsi_date,
        planned_lsi_date,
        planned_enrollment_days,
        actual_fsi_date,
        actual_lsi_date,
        actual_enrollment_days,
        planned_enrollment_count,
        actual_enrollment_count,
        planned_enrollment_count_todate,
        case 
            when actual_enrollment_count = 0 or
                 planned_lsi_date is null or
                 planned_fsi_date is null or
                 planned_lsi_date - planned_fsi_date = 0 or 
                 planned_enrollment_count_todate = 0 then
                null::text
            when ((coalesce(actual_lsi_date,now()::date) - actual_fsi_date)::numeric / (planned_lsi_date - planned_fsi_date)::numeric) <=
                (actual_enrollment_count::numeric / planned_enrollment_count_todate::numeric) then 
                    'ON ENROLLMENT PLAN'::text 
            else 
                'BEHIND ENROLLMENT PLAN'::text
            end::text as enrollment_status,
         now()::timestamp as comprehend_update_time
from (
        select 
        study.comprehendid as comprehendid,
        study.therapeuticarea as therapeuticarea,
        study.program as program,
        study.studyid,
        study.studyname,
        study.studystartdate,
        study.studycompletiondate,
        now()::date as currentdate,
        fpi_planned.expecteddate::date as planned_fsi_date,
        lpi_planned.expecteddate::date as planned_lsi_date,
        (lpi_planned.expecteddate - fpi_planned.expecteddate)::integer as planned_enrollment_days,
        fpi_actual.actualdate::date as actual_fsi_date,
        lpi_actual.actualdate::date as actual_lsi_date,
        coalesce((coalesce(lpi_actual.actualdate::date,now()::date) - fpi_actual.actualdate)::integer,0::integer) as actual_enrollment_days,
        coalesce(spe.enrollment_count,0)::integer as planned_enrollment_count,
        coalesce(subj_enrolled.enrollment_count,0)::integer as actual_enrollment_count,
        coalesce(spe2.enrollment_count,0)::integer as planned_enrollment_count_todate
        from study
        left join (select studyid, expecteddate from studymilestone where milestonelabel = 'ALL SUBJECTS ENROLLED' and lower(milestonetype) = 'planned') lpi_planned on (lpi_planned.studyid = study.studyid)
        left join (select studyid, expecteddate from studymilestone where milestonelabel = 'FIRST SUBJECT IN' and lower(milestonetype) = 'planned') fpi_planned on (fpi_planned.studyid = study.studyid)
        left join (select studyid, expecteddate as actualdate from studymilestone where milestonelabel = 'ALL SUBJECTS ENROLLED' and lower(milestonetype) = 'actual') lpi_actual on (lpi_actual.studyid = study.studyid)
        left join (select studyid, min(dsstdtc) as actualdate from rpt_subject_disposition group by 1) fpi_actual on (fpi_actual.studyid = study.studyid)
        left join (select studyid, count(*) as enrollment_count from rpt_subject_disposition where dsevent = 'ENROLLED'  group by 1) subj_enrolled on (study.studyid = subj_enrolled.studyid)
        left join (select studyid, sum(recruitmentcount) as enrollment_count from studyplannedrecruitment
                       where lower(type) = 'planned' and lower(category) = 'enrollment' and lower(frequency) = 'monthly' group by 1) spe on (study.studyid = spe.studyid)
        left join (select studyid, sum(recruitmentcount) as enrollment_count from studyplannedrecruitment
                       where lower(type) = 'planned' and lower(category) = 'enrollment' and lower(frequency) = 'monthly' and enddate < now()::date group by 1) spe2 on (study.studyid = spe2.studyid)) a;





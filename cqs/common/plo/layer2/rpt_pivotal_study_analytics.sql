/*
rpt_pivotal_study_analytics PLO
CDM Version: 2.5
Notes:  The Pivotal Study Analytic PLO rolls up key counts and forecast dates that are used by the application to
    present the Pivital Study Analytic Dashboard

        The forecast dates and milestones statuses are calculated by the plo post processing script

Revision History: 27-Jul-2016 Palaniraja Dhanavelu - Initial version
                  11-Aug-2016 Palaniraja Dhanavelu - Added columns first_site_activation_planned_date, first_site_activation_actual_date,
                                                     lsi_projected_date_50percent_patients, logistic_lsi_projected_date
                  31-Aug-2016 Adam Kaus - Added dsterm to where clause for filtering by enrollment
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  16-Nov-2016 Palaniraja Dhanavelu - Apply fix to handle null due to null values in DS as per tp TP18947
                  19-Dec-2016 Palaniraja Dhanavelu - Fixing the target enrollment count calcualtion as per tp20067
                  09-Feb-2017 Michelle Engler - From this point forward, the revision history will be maintained in github, exclusively
*/

create table rpt_pivotal_study_analytics as
with study_details as (
                select s.comprehendid,
                       s.therapeuticarea,
                       s.program,
                       s.studyid,
                       s.studyname
                       from study s
                      ),
    -- Current milestone is determined as the milestone that is expected to occur after the last.  The ordering of milestones is
    --    set my the milestoneseq; the "next" milestone will be the milestone with the next milestoneseq
    --    completed milestone.
    current_milestone as (
                select studyid, milestoneseq, current_milestone, current_milestone_planned_date, iscriticalpath
                from (  select s1.studyid, s1.milestoneseq, s1.iscriticalpath,
                               s1.milestonelabel as current_milestone,
                               s2.expecteddate  as current_milestone_planned_date,
                               rank () over (partition by s1.studyid order by s2.milestoneseq ) as rank
                        from studymilestone s1
                        join (select studyid, milestonelabel, milestoneseq, expecteddate
                              from studymilestone
                              where ( milestonetype = 'Planned')) s2 on s1.studyid = s2.studyid and s1.milestonelabel = s2.milestonelabel
                        where s1.milestonetype = 'Actual' and s1.expecteddate is null) a
                where rank = 1),
    -- Previous milestone is the milestone has precede milestoneseq of current milestone. In special case if previous milestone is
    --     critical milestone then we need to get previous critical milestone to assure the consistent with "Study Milestones"
    previous_milestone as (
                select studyid, previous_milestone_planned_date,
                       (select expecteddate
                        from studymilestone s
                        where milestonetype = 'Actual'
                          and milestoneseq = prev_mt.previous_milestoneseq
                          and studyid = prev_mt.studyid
                        limit 1) as previous_milestone_actual_completion_date
                from (select s.studyid,
                             s.milestoneseq as previous_milestoneseq,
                             s.expecteddate as previous_milestone_planned_date,
                             rank () over (partition by s.studyid order by s.milestoneseq desc) as rank
                      from studymilestone s join current_milestone c on s.studyid = c.studyid
                      where s.milestonetype = 'Planned' and s.milestoneseq < c.milestoneseq
                        and (c.iscriticalpath = false or (c.iscriticalpath = true and s.iscriticalpath=true))) prev_mt
                where rank = 1),
    -- Enrollment is determined by a count of subjects that have an enrollment disposition event
    current_enrollemnt as (
                select studyid,
                       count(1) as current_enrollment_count
                       from rpt_subject_disposition ds where dsevent = 'ENROLLED' AND ds.dsstdtc is not null
                       group by studyid),
    -- Target Enrollment is determined by the total enrollment count contained in the Study Planned Recruitment with category = 'Enrollment'
    target_enrollment as (
                select studyid,
                       sum(recruitmentcount) as target_enrollment_count
                       from studyplannedrecruitment
                       where lower(type) = 'planned' and lower(category) = 'enrollment' and lower(frequency) = 'monthly'
                       group by studyid),

    -- The last subject in (LSI) planned date is determined from the planned study milestones
    lsi_planned as (
                select studyid,
                       expecteddate as lsi_planned_date
                       from studymilestone
                       where milestonetype = 'Planned' and milestonelabel = 'LAST SUBJECT IN'
                       group by studyid, expecteddate),

    -- Current site activation by study is determined by counting the number of sites with a populated site activation date
    current_site_activation as (
                select studyid,
                       count(1)  as current_site_activation_count
                       from site where siteactivationdate is not null
                       group by studyid),

    -- Target Site Activation is determined by taking value of column `statval` with `statcat = 'SITE_ACTIVATION' in table `studyplannedstatistic`. There is only one record for each study.
    target_site_activation as (
                select studyid,
                       statval as target_site_activation_count
                from studyplannedstatistic
                where statcat = 'SITE_ACTIVATION'),

    -- Site Activation Planned Date is determined from the All Sites Activated planning study milestone
    site_activation_planned as (
                select studyid,
                       max(expecteddate) as site_activation_planned_date
                       from studymilestone where  milestonetype = 'Planned' and upper(milestonelabel) = 'ALL SITES ACTIVATED'
                       group by studyid),

    -- First Site Activation Planned is determined from the First Site Ready to Enroll planning study milestone
    first_site_activation_planned as (
                select studyid, expecteddate as first_site_activation_planned_date
                from studymilestone where  milestonetype = 'Planned' and upper(milestonelabel) = 'FIRST SITE READY TO ENROLL'),

    -- First Site Activation Actual is determined by taking the date from the First Site Ready to Enroll actual study milestone
    first_site_activation_actual as (
                select studyid, expecteddate as first_site_activation_actual_date
                from studymilestone where  milestonetype = 'Actual' and upper(milestonelabel) = 'FIRST SITE READY TO ENROLL'),

    -- CRO name string aggregation per study
    cro AS (
                SELECT studyid, STRING_AGG(DISTINCT NULLIF(croname, ''), ', ' ORDER BY NULLIF(croname, '')) AS cronames
                FROM studycro 
                GROUP BY studyid),

    planned_monitoring_visit_frequency AS (
                SELECT studyid, STRING_AGG((statval||'-'||statunit), ', ' ORDER BY statval) AS frequency
                FROM studyplannedstatistic
                WHERE statcat = 'MONITORING_VISIT_FREQUENCY'
                GROUP BY studyid)

select s.comprehendid::text as comprehendid,
       s.therapeuticarea::text as therapeuticarea,
       s.program::text as program,
       s.studyid::text as studyid,
       s.studyname::text as studyname,
       cro.cronames::text as studycronames,
       pm.previous_milestone_planned_date::date,
       pm.previous_milestone_actual_completion_date::date,
       c.current_milestone::text as current_milestone,
       c.current_milestone_planned_date::date as current_milestone_planned_date,
       null::date as current_milestone_projected_date, -- Populated by PLO Post Processing
       null::text as milestone_achievement_status, -- Populated by PLO Post Processing
       site4.first_site_activation_planned_date::date as first_site_activation_planned_date,
       site5.first_site_activation_actual_date::date as first_site_activation_actual_date,
       coalesce(e1.current_enrollment_count,0)::int as current_enrollment_count,
       e2.target_enrollment_count::int as target_enrollment_count,
       l1.lsi_planned_date::date as lsi_planned_date,
       null::date as lsi_projected_date, -- Populated by PLO Post Processing
       null::date as lsi_projected_date_50percent_patients, -- Populated by PLO Post Processing
       null::text as lsi_achievement_status, -- Populated by PLO Post Processing
       null::date as logistic_lsi_projected_date, -- Populated by PLO Post Processing
       site1.current_site_activation_count::int as current_site_activation_count,
       site2.target_site_activation_count::int as target_site_activation_count,
       site3.site_activation_planned_date::date as site_activation_planned_date,
       null::date as site_activation_projected_date, -- Populated by PLO Post Processing
       null::integer as site_activation_days_behind, -- Populated by PLO Post Processing
       null::text as study_risk_level, -- Populated by PLO Post Processing
       null::date as monte_carlo_optimistic_date, -- Populated by PLO Post Processing
       null::date as monte_carlo_realistic_date, -- Populated by PLO Post Processing
       null::date as monte_carlo_pessimistic_date, -- Populated by PLO Post Processing
       pmv.frequency::text as study_planned_monitoring_visit_frequency,
       now()::timestamp as comprehend_update_time
from study_details s
     left join current_milestone c on s.studyid = c.studyid
     left join previous_milestone pm on s.studyid = pm.studyid
     left join current_enrollemnt e1 on s.studyid = e1.studyid
     left join target_enrollment e2 on s.studyid = e2.studyid
     left join lsi_planned l1 on s.studyid = l1.studyid
     left join current_site_activation site1 on s.studyid = site1.studyid
     left join target_site_activation site2 on s.studyid = site2.studyid
     left join site_activation_planned site3 on s.studyid = site3.studyid
     left join first_site_activation_planned site4 on s.studyid = site4.studyid
     left join first_site_activation_actual site5 on s.studyid = site5.studyid
     left join cro on s.studyid = cro.studyid
     left join planned_monitoring_visit_frequency pmv on s.studyid = pmv.studyid;

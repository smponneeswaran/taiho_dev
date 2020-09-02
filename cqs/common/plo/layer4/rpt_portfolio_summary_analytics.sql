create table rpt_portfolio_summary_analytics as
WITH site_performance as (
    SELECT
        SUM(rpt_site_performance.failed_screen_count)::NUMERIC failed_screen_count,
        SUM(rpt_site_performance.subjects_screened_count)::NUMERIC subjects_screened_count,
        SUM(ae_count)::NUMERIC ae_count,
        SUM(dv_count)::NUMERIC dv_count,
        SUM(subject_days_count)::NUMERIC subject_days_count,
        studyid::TEXT
    FROM
        rpt_site_performance
    GROUP BY
        studyid
),
site_activations as (
    SELECT
        SUM(recruitmentcount)::NUMERIC site_activation_planned_to_date,
        studyid::TEXT
    FROM studyplannedrecruitment
    WHERE enddate < NOW()::DATE
        AND type = 'Planned'
        AND category = 'Site Activation'
        AND frequency = 'Monthly'
    GROUP BY studyid
),
enrollments as (
    SELECT
        SUM(recruitmentcount)::NUMERIC enrollment_planned_to_date,
        studyid::TEXT
    FROM studyplannedrecruitment
    WHERE enddate < NOW()::DATE
        AND type = 'Planned'
        AND category = 'Enrollment'
        AND frequency = 'Monthly'
    GROUP BY studyid
),
screenings as (
    SELECT
        SUM(recruitmentcount)::NUMERIC screenings_planned_to_date,
        studyid::TEXT
    FROM studyplannedrecruitment
    WHERE enddate < NOW()::DATE
        AND type = 'Planned'
        AND category = 'Screening'
        AND frequency = 'Monthly'
    GROUP BY studyid
),
total_screenings as (
    SELECT
        SUM(recruitmentcount)::NUMERIC total_screens_planned,
        studyid::TEXT
    FROM studyplannedrecruitment
    WHERE type = 'Planned'
      AND category = 'Screening'
      AND frequency = 'Monthly'
    GROUP BY studyid
)
SELECT
    study.studyid::TEXT studyid,
    study.studyname::TEXT studyname,
    study.studyphase::TEXT studyphase,
    rpt_pivotal_study_analytics.current_milestone::TEXT,
    rpt_pivotal_study_analytics.current_milestone_planned_date::DATE,
    rpt_pivotal_study_analytics.current_milestone_projected_date::DATE,
    rpt_pivotal_study_analytics.previous_milestone_planned_date::DATE,
    rpt_pivotal_study_analytics.previous_milestone_actual_completion_date::DATE,
    rpt_pivotal_study_analytics.current_site_activation_count::NUMERIC,
    site_activations.site_activation_planned_to_date::NUMERIC,
    rpt_pivotal_study_analytics.target_site_activation_count::NUMERIC,
    rpt_pivotal_study_analytics.current_enrollment_count::NUMERIC,
    enrollments.enrollment_planned_to_date::NUMERIC,
    rpt_pivotal_study_analytics.target_enrollment_count::NUMERIC,
    site_performance.subject_days_count::NUMERIC,
    site_performance.ae_count::NUMERIC,
    CASE WHEN site_performance.subject_days_count = 0 OR site_performance.subject_days_count IS null THEN 0 ELSE (site_performance.ae_count / site_performance.subject_days_count)::NUMERIC END AS ae_rate,
    site_performance.dv_count::NUMERIC,
    CASE WHEN site_performance.subject_days_count = 0 OR site_performance.subject_days_count IS null THEN 0 ELSE (site_performance.dv_count / site_performance.subject_days_count)::NUMERIC END AS pd_rate,
    site_performance.failed_screen_count::NUMERIC,
    site_performance.subjects_screened_count::NUMERIC,
    screenings.screenings_planned_to_date::NUMERIC,
    total_screenings.total_screens_planned::NUMERIC,
    CASE WHEN site_performance.subjects_screened_count = 0 or site_performance.subjects_screened_count IS null THEN 0 ELSE (site_performance.failed_screen_count / site_performance.subjects_screened_count)::NUMERIC END AS screen_failure_rate
FROM study
LEFT JOIN rpt_pivotal_study_analytics ON rpt_pivotal_study_analytics.studyid = study.studyid
LEFT JOIN site_performance ON site_performance.studyid = study.studyid
LEFT JOIN site_activations ON site_activations.studyid = study.studyid
LEFT JOIN enrollments ON enrollments.studyid = study.studyid
LEFT JOIN screenings ON screenings.studyid = study.studyid
LEFT JOIN total_screenings ON total_screenings.studyid = study.studyid;

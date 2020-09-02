/*
dimstudy

Notes: PLO with details having data combined from study, studymilestone and pivotal study analytics.

*/

CREATE TABLE dimstudy AS
WITH study_subjects AS 
                    ( SELECT ds.studyid,
                             count(1) AS study_subjects
                            FROM rpt_subject_disposition ds
                            WHERE dsterm = study_start_disposition
                            GROUP BY ds.studyid),
     planned_lpi AS
                   ( SELECT study.studyid,
                            COALESCE(studymilestone.expecteddate, (now())::date) AS planned_lpi
                            FROM study
                            LEFT JOIN studymilestone ON (study.studyid = studymilestone.studyid AND lower(studymilestone.milestonelabel) = 'all subjects enrolled'::text AND lower(studymilestone.milestonetype) = 'planned'::text)),
     study_planned_completion AS
                    ( SELECT studymilestone.studyid,
                            studymilestone.expecteddate AS study_planned_completion_date
                            FROM studymilestone
                            WHERE (lower(studymilestone.milestonetype) = 'planned'::text AND lower(studymilestone.milestonelabel) = 'study closed'::text)),
     study_subject_days AS
                    ( SELECT rpt_subject_days.studyid,
                            COALESCE(sum(rpt_subject_days.thismonthsubjectdays), (0)::bigint) AS subject_days
                            FROM rpt_subject_days
                            GROUP BY rpt_subject_days.studyid)
    

select DISTINCT study.comprehendid as comprehendid,
        study.studyid as studyid,
        study.studyname as studyname,
        study.studydescription as studydescription,
        study.studystatus as studystatus,
        study.studyphase as studyphase,
        study.studysponsor as studysponsor,
        study.therapeuticarea as therapeuticarea,
        study.program as program,
        study.medicalindication as medicalindication,
        study.studystartdate as studystartdate,
        study.studycompletiondate as studycompletiondate,
        planned_lpi.planned_lpi AS planned_lpi,
        rpsa.first_site_activation_planned_date AS first_site_activation_planned_date,
        rpsa.first_site_activation_actual_date AS first_site_activation_actual_date,
        rpsa.target_enrollment_count AS enrollment_count, 
        rpsa.current_site_activation_count AS current_count_sites_activated,
        rpsa.target_site_activation_count AS latest_planned_count_sites_activated,
        ss.study_subjects,
        COALESCE(lastactivation.actualend, (lastactivation.plannedend + curmile.delay)) AS site_activation_completion_date,
        lastactivation.plannedend AS latest_planned_date_for_last_site_activated,
        study_subject_days.subject_days,
        sed.study_planned_completion_date,
        rpsa.target_enrollment_count,
        rpsa.current_enrollment_count,
        NULL::date as site_activation_projected_date, -- This date will get populated by plo post processing script
        NULL::date as logistic_lsi_projected_date, -- This date will get populated by plo post processing script
        now()::timestamp as comprehend_update_time
        from study
        LEFT JOIN planned_lpi ON (planned_lpi.studyid = study.studyid)
        LEFT JOIN rpt_pivotal_study_analytics rpsa ON (rpsa.studyid = study.studyid)
        LEFT JOIN study_subjects ss on (ss.studyid = study.studyid)
        LEFT JOIN dimstudymilestone curmile ON (curmile.studyid = study.studyid AND curmile.iscurrent = true)
        LEFT JOIN dimstudymilestone lastactivation ON (lastactivation.studyid = study.studyid AND lower(lastactivation.milestonelabel) = 'all sites activated'::text)
        LEFT JOIN study_planned_completion sed ON (sed.studyid = study.studyid)
        LEFT JOIN study_subject_days ON (study_subject_days.studyid = study.studyid);


/*
factadverseevents

Note : PLO having ae details along with calculated values category, study_week_number and duration 
*/

CREATE TABLE factadverseevents AS
SELECT ae.siteid,
    ae.studyid,
    ae.comprehendid,
    ae.aeseq,
    d.date_dim_id AS month_trunc,
    ae.objectuniquekey,
    ae.aeser,
    ae.aerelnst,
    aesubj.category AS category,
    ae.aeverbatim,
    ae.aeterm,
    ae.aebodsys,
    ae.aestdtc,
    ae.aeendtc,
    ae.aesev,
    ae.usubjid,
    aerelwk.relative_trial_week AS study_week_number, -- fetching this field from rpt_ae_rel_week which has the expected logic to calculate realtive study week
    ((CASE WHEN ae.aeendtc is null THEN NOW() ELSE aeendtc END)::date  - (ae.aestdtc)::date + 1) AS duration,
    now()::timestamp AS comprehend_update_time
FROM ae
JOIN dimdate d ON (d.date_actual = (ae.aestdtc)::date)
JOIN rpt_ae_rate_by_subject_days aesubj ON (ae.objectuniquekey = aesubj.objectuniquekey)
LEFT JOIN rpt_ae_rel_week aerelwk on (ae.objectuniquekey = aerelwk.objectuniquekey)
WHERE ae.aestdtc IS NOT NULL;

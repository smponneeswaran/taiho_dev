/*
rpt_subject_disposition
Notes: Merges mapped ds customer data with the ds app config
*/

CREATE TABLE rpt_subject_disposition AS

-- identify starting disposition per study
-- default to min sequence if not configured
WITH start_ds AS (WITH min_ds AS (SELECT customer_event
                                    FROM app.ds_config c
                                    JOIN (SELECT MIN(ui_sequence) AS min_seq
                                            FROM app.ds_config
                                            WHERE enabled IS TRUE) m
                                    ON c.ui_sequence = m.min_seq)

                    SELECT DISTINCT s.studyid, COALESCE(c.customer_event, m.customer_event)::TEXT AS study_start_disposition
                    FROM study s
                    LEFT JOIN min_ds m ON (1=1)
                    LEFT JOIN app.ds_config c ON (c.is_starting_event IS TRUE AND c.enabled IS TRUE))

SELECT 
    d.comprehendid::TEXT AS comprehendid,
    d.studyid::TEXT AS studyid,
    s.studyname::TEXT AS studyname,
    s.program::TEXT AS program,
    s.medicalindication::TEXT AS medicalindication,
    s.studyphase::TEXT AS studyphase,
    s.therapeuticarea::TEXT AS therapeuticarea,
    s.studydescription::TEXT AS studydescription,
    s.studysponsor::TEXT AS studysponsor,
    si.siteid::TEXT AS siteid,
    si.sitename::TEXT AS sitename,
    si.sitecro::TEXT AS sitecro,
    si.sitecountry::TEXT AS sitecountry,
    si.siteregion::TEXT AS siteregion,
    d.usubjid::TEXT AS usubjid,
    d.dscat::TEXT AS dscat,
    d.dsterm::TEXT AS dsterm,
    d.dsstdtc::DATE AS dsstdtc,
    d.dsscat::TEXT AS dsscat,
    d.dsgrpid::TEXT AS dsgrpid,
    d.dsrefid::TEXT AS dsrefid,
    d.dsspid::TEXT AS dsspid,
    d.dsdecod::TEXT AS dsdecod,
    d.visit::TEXT AS visit,
    d.visitnum::NUMERIC AS visitnum,
    d.visitdy::INTEGER AS visitdy,
    d.epoch::TEXT AS epoch,
    d.dsdtc::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
    d.dsstdy::INTEGER AS dsstdy,
    c.ui_sequence::SMALLINT AS dsseq,
    c.ui_event::TEXT AS dslabel,
    c.event_id::TEXT AS dsevent,
    (CASE WHEN c.event_state_id = 'ACTIVE' THEN TRUE ELSE FALSE END)::BOOLEAN AS dsactive,
    (CASE WHEN c.event_state_id = 'WITHDRAWN' THEN TRUE ELSE FALSE END)::BOOLEAN AS dswithdrawn,
    (CASE WHEN c.event_state_id = 'COMPLETED' THEN TRUE ELSE FALSE END)::BOOLEAN AS dscompleted,
    c.is_starting_event::BOOLEAN AS dsstarting,
    c.id::UUID AS dsid,
    sd.study_start_disposition::TEXT AS study_start_disposition,
    d.objectuniquekey::TEXT AS objectuniquekey,
    NOW()::TIMESTAMP AS comprehend_update_time
FROM ds d
JOIN study s ON (d.studyid = s.studyid)
JOIN site si ON (d.studyid = si.studyid AND d.siteid = si.siteid)
JOIN app.ds_config c ON (d.dsterm = c.customer_event)
JOIN start_ds sd ON (d.studyid = sd.studyid)
WHERE c.enabled IS TRUE;

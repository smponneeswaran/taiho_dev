/*
dimdisposition

Notes: PLO providing multiple date dimensions and will used in other PLO's for roll up calculations that involves date dimensions.

*/

CREATE TABLE dimdisposition AS
SELECT ds.comprehendid::TEXT AS comprehendid,
    ds.studyid::TEXT AS studyid,
    ds.siteid::TEXT AS siteid,
    ds.usubjid::TEXT AS usubjid,
    ds.dsseq::NUMERIC AS dsseq,
    ds.dscat::TEXT AS dscat,
    ds.dsterm::TEXT AS dsterm,
    ds.dsstdtc::DATE AS dsstdtc,
    COALESCE(ds.dsscat, 'blank'::TEXT)::TEXT AS dsscat,
    ds.dsevent::TEXT AS dsevent,
    ds.dsactive::BOOLEAN AS dsactive,
    ds.dswithdrawn::BOOLEAN AS dswithdrawn,
    ds.dscompleted::BOOLEAN AS dscompleted,
    ds.objectuniquekey::TEXT AS objectuniquekey,
    CASE WHEN (ds.dswithdrawn IS TRUE) THEN '-1'::INTEGER
        ELSE 1
        END::INTEGER AS dispositionmultiplier,
    CASE WHEN (ds.dswithdrawn IS TRUE) THEN 'Negative'::TEXT
        ELSE 'Positive'::TEXT
        END::TEXT AS dispositiontype,
    CASE WHEN (latest_ds.is_latest_ds = true ) THEN 1
        ELSE 0
        END::INTEGER AS islatestsequence,
    CASE WHEN (ds.dsevent = 'ENROLLED') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectenrolled,
    CASE WHEN (ds.dsevent = 'FAILED_SCREEN') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectfailedscreen,
    CASE WHEN (ds.dsevent = 'ENROLLED') THEN 1.0
        WHEN (ds.dsevent = 'FAILED_SCREEN') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectscreened,
    CASE WHEN (ds.dsevent = 'WITHDRAWN') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectwithdrawn,
    CASE WHEN (ds.dsevent = 'CONSENTED') THEN 1.0
        ELSE (0)::NUMERIC
        END::NUMERIC AS subjectconsented,
    CASE WHEN (ds.dsevent = 'FAILED_RANDOMIZATION') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectfailedrandomization,
    CASE WHEN (ds.dsevent = 'EVALUABLE') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectevaluable,
    CASE WHEN (ds.dsevent = 'NO_CONSENT') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectconsentfailures,
    CASE WHEN (ds.dsevent = 'NOT_EVALUABLE') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectevaluablefailures,
    CASE WHEN (ds.dsevent = 'RANDOMIZED') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectrandomized,
    CASE WHEN (ds.dsevent = 'COMPLETED') THEN 1.0
        ELSE 0.0
        END::NUMERIC AS subjectcompleted,
    NOW()::TIMESTAMP AS comprehend_update_time
   FROM rpt_subject_disposition ds
   LEFT JOIN rpt_latest_ds latest_ds  ON (ds.comprehendid = latest_ds.comprehendid and latest_ds.dsseq = ds.dsseq );


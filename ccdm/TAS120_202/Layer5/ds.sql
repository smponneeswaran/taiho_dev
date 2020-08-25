/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ds_data AS (
	            --All Subjects
                SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                        1.0::NUMERIC AS dsseq, --deprecated
                        'All Subjects'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'All Subjects'::TEXT AS dsterm,
                        null::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."DM" 
				
				UNION ALL
				
				--Consented
				SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                        2.0::NUMERIC AS dsseq, --deprecated
                        'Consent'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Consented'::TEXT AS dsterm,
                        COALESCE("MinCreated" ,"RecordDate")::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."ENR"
				
				UNION ALL
				
				---Failed Screen
				SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                         2.1::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Failed Screen'::TEXT AS dsterm,
                        COALESCE("MinCreated" ,"RecordDate")::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."ENR"  where lower(trim("ENRYN"))='no' 
				
				UNION ALL
				
				---Enrollment
				SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                        3.0::NUMERIC AS dsseq, --deprecated
                        'Enrollment'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Enrolled'::TEXT AS dsterm,
                        min("ENRDAT")::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."ENR"
				
				UNION ALL
				
				---Withdrawn(EOS)
				SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                        4.1::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOSDAT"::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."EOS" where "EOSREAS" != 'End of study per 2 protocol'
				
				UNION ALL
				
				---Withdrawn(EOT)
				SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                        4.1::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Withdrawn'::TEXT AS dsterm,
                        "EOTDAT"::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."EOT"  where "EOSREAS" != 'End of study per 2 protocol'
				
				UNION ALL
				
				---Study Completion
				SELECT  "project"::TEXT AS studyid,
                        "SiteNumber"::TEXT AS siteid,
                        "Subject"::TEXT AS usubjid,
                        5.0::NUMERIC AS dsseq, --deprecated
                        'Completion'::TEXT AS dscat,
                        null::TEXT AS dsscat,
                        'Completed'::TEXT AS dsterm,
                        "EOSDAT"::DATE AS dsstdtc,  
                        null::TEXT AS dsgrpid,
                        null::TEXT AS dsrefid,
                        null::TEXT AS dsspid,
                        null::TEXT AS dsdecod,
                        null::TEXT AS visit,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::TEXT AS epoch,
                        null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
                        null::INTEGER AS dsstdy  
				FROM "tas120_202"."EOS"
				
				
				)

SELECT
        /*KEY (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid)::TEXT AS comprehendid, KEY*/
        ds.studyid::TEXT AS studyid,
        ds.siteid::TEXT AS siteid,
        ds.usubjid::TEXT AS usubjid,
        ds.dsseq::NUMERIC AS dsseq, --deprecated
        ds.dscat::TEXT AS dscat,
        ds.dsscat::TEXT AS dsscat,
        ds.dsterm::TEXT AS dsterm,
        ds.dsstdtc::DATE AS dsstdtc,
        ds.dsgrpid::TEXT AS dsgrpid,
        ds.dsrefid::TEXT AS dsrefid,
        ds.dsspid::TEXT AS dsspid,
        ds.dsdecod::TEXT AS dsdecod,
        ds.visit::TEXT AS visit,
        ds.visitnum::NUMERIC AS visitnum,
        ds.visitdy::INTEGER AS visitdy,
        ds.epoch::TEXT AS epoch,
        ds.dsdtc::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
        ds.dsstdy::INTEGER AS dsstdy
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);  

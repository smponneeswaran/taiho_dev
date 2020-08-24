/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
							SELECT  'TAS120-201'::text AS studyid,
                            'TAS120-201'::text AS studyname,
                            'A Phase 2 Study of TAS-120 in Metastatic Breast Cancers Harboring Fibroblast Growth Factor Receptor (FGFR) Amplifications'::text AS studydescription,
                            null::text AS studystatus,
                            'Phase 2'::text AS studyphASe,
                            null::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            null::text AS program,
                            null::text AS medicalindication,
                            null::date AS studystartdate,
                            null::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived 
					)

SELECT 
        /*KEY s.studyid::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.studyname::text AS studyname,
        s.studydescription::text AS studydescription,
        s.studystatus::text AS studystatus,
        s.studyphase::text AS studyphase,
        s.studysponsor::text AS studysponsor,
        s.therapeuticarea::text AS therapeuticarea,
        s.program::text AS program,
        s.medicalindication::text AS medicalindication,
        s.studystartdate::date AS studystartdate,
        s.studycompletiondate::date AS studycompletiondate,
        s.studystatusdate::date AS studystatusdate,
        s.isarchived::boolean AS isarchived 
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s;

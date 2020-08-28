/*
CCDM FormDef mapping
Notes: Standard mapping to CCDM FormDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     formdef_data AS (
                  SELECT  'TAS120_201'::text AS studyid,
                        fm."OID"::text AS formid,
                        fm."Name"::text AS formname,
                        FALSE::boolean AS isprimaryendpoint,
                        FALSE::boolean AS issecondaryendpoint,
                        CASE WHEN fe."SourceDocument" IS NULL 
                              THEN false 
                              ELSE fe."SourceDocument" 
                        END::text AS issdv,
                        "Mandatory"::boolean AS isrequired
			 from tas120_201."metadata_forms" fm
			 left join tas120_201."metadata_fields"fe
			 on fm."OID"=SUBSTRING(fe."OID", 1, (POSITION('.' in fe."OID")-1))
			 )

SELECT 
        /*KEY fd.studyid::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.formid::text AS formid,
        fd.formname::text AS formname,
        fd.isprimaryendpoint::boolean AS isprimaryendpoint,
        fd.issecondaryendpoint::boolean AS issecondaryendpoint,
        fd.issdv::boolean AS issdv,
        fd.isrequired::boolean AS isrequired
        /*KEY , (fd.studyid || '~' || fd.formid)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM formdef_data fd
JOIN included_studies st ON (fd.studyid = st.studyid)

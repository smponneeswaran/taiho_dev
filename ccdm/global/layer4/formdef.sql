/*
CCDM FormDef mapping
Notes: Standard mapping to CCDM FormDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

form_def AS (
SELECT 
        fd.studyid::text AS studyid,
        fd.formid::text AS formid,
        fd.formname::text AS formname,
        row_number() over(partition by fd.studyid,fd.formname order by fd.formid) AS RNK,
        fd.isprimaryendpoint::boolean AS isprimaryendpoint,
        fd.issecondaryendpoint::boolean AS issecondaryendpoint,
        fd.issdv::boolean AS issdv,
        fd.isrequired::boolean AS isrequired
FROM stg_formdef fd    
)

SELECT 
	/*KEY f.studyid::text AS comprehendid, KEY*/
   f.studyid,
   f.formid,
   CASE WHEN f.RNK='1' THEN f.formname
        WHEN f.RNK>'1' THEN concat(f.formname,f.RNK)
   END::text AS formname,
   f.isprimaryendpoint,
   f.issecondaryendpoint,
   f.issdv,
   f.isrequired
   /*KEY , (f.studyid || '~' || f.formid)::text AS objectuniquekey    KEY*/
   /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM form_def f
JOIN included_studies st ON (f.studyid = st.studyid);
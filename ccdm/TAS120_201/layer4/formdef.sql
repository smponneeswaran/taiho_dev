/*
CCDM FormDef mapping
Notes: Standard mapping to CCDM FormDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

formdef_data AS 
(
	SELECT  'TAS120-201'::text AS studyid,
	"OID"::text AS formid,
	"Name"::text AS formname,
	False::boolean AS isprimaryendpoint,
	False::boolean AS issecondaryendpoint,
	True::boolean AS issdv,
	True::boolean AS isrequired
	from tas120_201.metadata_forms
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
WHERE 1=2;  

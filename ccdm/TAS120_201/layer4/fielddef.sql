/*
CCDM FieldDef mapping
Notes: Standard mapping to CCDM FieldDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

fielddef_data AS 

(

	SELECT  
	'TAS120-201'::text AS studyid,
	"FormDefOID"::text AS formid,
	"VariableOID"::text AS fieldId,
	coalesce("SASLabel", "Name")::text AS fieldname,
	False::boolean AS isprimaryendpoint,
	False ::boolean AS issecondaryendpoint,
	True::boolean AS issdv,
	True::boolean  AS isrequired
	from tas120_201.metadata_fields
	
)

SELECT         
        /*KEY fd.studyid::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.formid::text AS formid,
        fd.fieldId::text AS fieldid,
        fd.fieldname::text AS fieldname,
        fd.isprimaryendpoint::boolean AS isprimaryendpoint,
        fd.issecondaryendpoint::boolean AS issecondaryendpoint,
        fd.issdv::boolean AS issdv,
        fd.isrequired::boolean  AS isrequired 
        /*KEY , (fd.studyid || '~' || fd.formid || '~' || fd.fieldId)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM fielddef_data fd
JOIN included_studies st ON (fd.studyid = st.studyid);

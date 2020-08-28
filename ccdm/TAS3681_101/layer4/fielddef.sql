/*
CCDM FieldDef mapping
Notes: Standard mapping to CCDM FieldDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    fielddef_data AS (
                SELECT  'TAS3681_101'::text AS studyid,
                        "FormDefOID"::text AS formid,
                        substr("OID", strpos("OID", '.')+1)::text AS fieldid,
                        coalesce("SASLabel", "Name")::text AS fieldname,
                        FALSE::boolean AS isprimaryendpoint,
                        FALSE::boolean AS issecondaryendpoint,
						CASE WHEN "Mandatory"::boolean = true and coalesce("SourceDocument", false)::boolean = true 
								THEN true
                                ELSE false 
						END::boolean AS issdv,
                        "Mandatory"::boolean  AS isrequired 
				from tas3681_101."metadata_fields" 
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

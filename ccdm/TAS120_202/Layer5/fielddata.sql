/*
CCDM FieldData mapping
Notes: Mapping to CCDM FieldData table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     fielddata_data AS (
                SELECT  'TAS120-202'::text AS studyid,
                        "Site"::text AS siteid,
                        "Subject"::text AS usubjid,
                        "FormDefOID"::text AS formid,
                        "RecordPosition"::integer AS formseq,
                        "VariableOID"::text AS fieldid,
                        coalesce("InstanceRepeatNumber","RecordId")::integer AS fieldseq,
                        coalesce("InstanceName","DataPageName")::text AS visit,
                        1::integer AS visitseq, /* defaulted to 1 - deprecated */
                        "FolderSeq"::integer AS log_num,
                        null::text AS datavalue,
                        null::date AS dataentrydate,
                        null::date AS datacollecteddate,
                        null::date AS sdvdate, 
                        null::text AS sourcerecordprimarykey, 
                        null::timestamp AS sourcerecorddate,
                        null::boolean AS isdeleted  /* Internal Field - leave as null */ 
						from "tas120_202"."metadata_fields")

SELECT 
        /*KEY (fd.studyid || '~' || fd.siteid || '~' || fd.usubjid)::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.siteid::text AS siteid,
        fd.usubjid::text AS usubjid,
        fd.formid::text AS formid,
        fd.formseq::integer AS formseq,
        fd.fieldid::text AS fieldid,
        fd.fieldseq::integer AS fieldseq,
        fd.visit::text AS visit,
        fd.visitseq::integer AS visitseq, 
        fd.log_num::integer AS log_num,
        fd.datavalue::text AS datavalue,
        fd.dataentrydate::date AS dataentrydate,
        fd.datacollecteddate::date AS datacollecteddate,
        fd.sdvdate::date AS sdvdate,
        fd.sourcerecordprimarykey::text AS sourcerecordprimarykey,
        fd.sourcerecorddate::timestamp AS sourcerecorddate,
        fd.isdeleted::boolean AS isdeleted 
        /*KEY , (fd.studyid || '~' || fd.siteid || '~' || fd.usubjid || '~' || '~' || fd.visit || '~' || fd.formid || '~' || fd.formseq || '~' || fd.fieldid || '~' || fd.fieldseq || '~' || fd.log_num)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM fielddata_data fd
JOIN included_subjects s ON (fd.studyid = s.studyid AND fd.siteid = s.siteid AND fd.usubjid = s.usubjid)

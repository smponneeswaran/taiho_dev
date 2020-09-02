/*
rpttab_fielddata PLO

Notes:  
     - This PLO serves as the source of truth for field data including SDV
*/

CREATE TABLE rpttab_fielddata AS
SELECT fd.comprehendid::TEXT AS comprehendid, 
       subject.sdvtier::TEXT AS sdvtier,
       study.studyid::TEXT AS studyid,
       study.studyname::TEXT AS studyname,
       study.istsdv::BOOLEAN AS istsdv,
       site.siteid::TEXT AS siteid,
       site.sitename::TEXT AS sitename,
       site.sitecountry::TEXT AS sitecountry,
       site.siteregion::TEXT AS siteregion,
       fd.usubjid::TEXT AS usubjid,
       tv.visitnum::NUMERIC AS visitnum,
       tv.visit::TEXT AS visit,
       fd.visitseq::INT AS visitseq,
       sv.svstdtc::DATE AS svstdtc,
       fd.formid::TEXT AS formid,
       fd.formseq::INT AS formseq,
       fdef.formname::TEXT AS formname,
       fd.fieldid::TEXT AS fieldid,
       fldef.fieldname::TEXT AS fieldname,
       fd.fieldseq::INT AS fieldseq,
       fd.datavalue::TEXT AS datavalue,
       fd.dataentrydate::DATE AS dataentrydate,
       fd.datacollecteddate::DATE AS datacollecteddate,
       fd.sdvdate::DATE AS sdvdate,
       fldef.isprimaryendpoint::BOOLEAN AS isprimaryendpoint,
       fldef.issecondaryendpoint::BOOLEAN AS issecondaryendpoint,
       fldef.isrequired::BOOLEAN AS isrequired,
       -- check if study level tsdv is enabled else use 100% source verification (fielddef setting)
       -- for tsdv, first check for visit-specific setting else use general setting
       -- when tsdv is enabled for the study but no config found, default to false
       (CASE WHEN study.istsdv IS TRUE THEN COALESCE(tsdv_visit.issdv, tsdv_general.issdv, FALSE)
        ELSE fldef.issdv END)::BOOLEAN AS issdv,
       fd.sourcerecordprimarykey::TEXT AS sourcerecordprimarykey,
       fd.sourcerecorddate::TIMESTAMP AS sourcerecorddate,
       fd.isdeleted::BOOLEAN AS isdeleted,
       (fd.comprehendid || '~'::TEXT || fd.visit || '~'::TEXT || fd.visitseq || '~'::TEXT || fd.formid || '~'::TEXT || fd.fieldid || '~' || fd.fieldseq)::TEXT as rpt_custom_edge1, -- edge to fielddata table
       fd.log_num::INT AS log_num,
       NOW()::TIMESTAMP as comprehend_update_time
FROM study   
JOIN site ON (study.studyid = site.studyid)
JOIN subject ON (site.comprehendid = subject.sitekey)
JOIN fielddata fd ON (subject.comprehendid = fd.comprehendid)
JOIN fielddef fldef ON (fd.studyid = fldef.studyid AND fd.formid = fldef.formid AND fd.fieldid = fldef.fieldid)
JOIN formdef fdef ON (fldef.studyid = fdef.studyid AND fldef.formid = fdef.formid)
JOIN sv ON (fd.studyid = sv.studyid AND fd.siteid = sv.siteid AND fd.usubjid = sv.usubjid AND fd.visit = sv.visit AND fd.visitseq = sv.visitseq)
JOIN tv ON (sv.studyid = tv.studyid AND sv.visit = tv.visit)
LEFT JOIN tsdv tsdv_general ON (subject.studyid = tsdv_general.studyid AND subject.sdvtier = tsdv_general.sdvtier AND fldef.formid = tsdv_general.formid AND fldef.fieldid = tsdv_general.fieldid AND tsdv_general.visit IS NULL)
LEFT JOIN tsdv tsdv_visit ON (subject.studyid = tsdv_visit.studyid AND subject.sdvtier = tsdv_visit.sdvtier AND fldef.formid = tsdv_visit.formid AND fldef.fieldid = tsdv_visit.fieldid AND tv.visit = tsdv_visit.visit);

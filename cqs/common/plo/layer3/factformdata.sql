/*
factformdata

Note : This PLO has the data combined from formdata objects and its corresponding metadata from formdef object

*/

CREATE TABLE factformdata AS
SELECT formdata.comprehendid, 
        formdata.studyid,
        formdata.siteid,
        formdata.usubjid,
        formdata.formid,
        formdata.visit,
        formdata.visitseq,
        formdata.formseq,
        formdata.dataentrydate,
        formdata.datacollecteddate,
        formdata.sdvdate,
        formdata.objectuniquekey,
        formdef.formname, 
        formdef.isprimaryendpoint, 
        formdef.issecondaryendpoint, 
        formdef.issdv, 
        formdef.isrequired, 
        formdef.objectuniquekey AS formdefuniquekey,
        now()::timestamp AS comprehend_update_time
FROM formdata 
JOIN formdef ON (formdef.studyid = formdata.studyid AND formdef.formid = formdata.formid)
WHERE formdata.datacollecteddate IS NOT NULL AND formdata.dataentrydate IS NOT NULL;

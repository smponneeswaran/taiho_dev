/*
rpt_missing_data PLO

Notes:

Revision History: 21-Jun-2016 Adam Kaus - Updated to populate fieldseq as 1 when otherwise null to support inclusion in PK
                  07-AUG-2016 Palaniraja Dhanavelu - Incremental Version
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  11-Nov-2016 Adam Kaus - Add create table statement
                  09-Feb-2017 Michelle Engler - From this point forward, the revisiion history will be maintained in github, exclusively
*/
CREATE TABLE rpt_missing_data AS

-- The Forms per Visit CTE select the distinct forms that are collected within the fielddata
--      (except those with missing data entry or data collected dates).  
--      The "INCREMENTAL_PREDICATE" is leveraged during the build process to load this table
--      incrementally based on the sourcerecorddate as populated in the base fielddata table.
with forms_per_visit as (
  select distinct
    comprehendid,
    studyid,
    siteid,
    usubjid,
    formid,
    formseq,
    visit,
    visitseq
  from
    fielddata
    /* INCREMENTAL_PREDICATE LEFT JOIN (select SourceRecordPrimaryKey from rpt_missing_data) rmd on (rmd.SourceRecordPrimaryKey::text = fielddata.SourceRecordPrimaryKey::text)
                             where (fielddata.datacollecteddate is not null and fielddata.dataentrydate is not null) AND ((fielddata.SourceRecordDate > (select coalesce(max(SourceRecordDate), '01-01-1900'::date) from rpt_missing_data)) 
                                    OR (rmd.SourceRecordPrimaryKey is null)) INCREMENTAL_PREDICATE */
  where fielddata.datacollecteddate is not null and fielddata.dataentrydate is not null
),
-- The Forms per Visit with Visit Num CTE adds the study name, site name, site country, visit number since that is not present in fielddata. It uses visitform instead of tv because we need to make 
--      sure the form was supposed to be part of the visit.  This section of code creates a dependency in that it expected TV to be completed and for the visits referenced 
--      in the fielddata table to be included in TV.
--      It also creates a dependency on SV having all the visits and visitseq numbers referenced in fielddata
--      Finally, formdef is also required in order to determine the form name. 
forms_per_visit_with_visit_num as (
  select
    forms_per_visit.*,
    study.studyname,
    study.istsdv,
    site.sitename,
    site.sitecountry,
    site.siteregion,
    sv.visitnum,
    sv.svstdtc,
    formdef.formname,
    subject.sdvtier
  from
    forms_per_visit
    join study on forms_per_visit.studyid = study.studyid
    join site on forms_per_visit.studyid= site.studyid and forms_per_visit.siteid = site.siteid
    join sv on forms_per_visit.comprehendid = sv.comprehendid and forms_per_visit.visit = sv.visit and forms_per_visit.visitseq = sv.visitseq
    join formdef on forms_per_visit.studyid = formdef.studyid and forms_per_visit.formid = formdef.formid
    join subject on forms_per_visit.comprehendid = subject.comprehendid
),
-- The fielddata shell brings together the collected forms (as determined by fielddata) and the actual fields along with the 
--      field statuses of isprimaryendpoing,issecondaryendpoing,and isrequired.
fielddata_shell as (
  select
    forms_per_visit_with_visit_num.*,
    fielddef.fieldid,
    fielddef.fieldname,
    -- check if study level tsdv is enabled else use 100% source verification (fielddef setting)
    -- for tsdv, first check for visit-specific setting else use general setting
    -- when tsdv is enabled for the study but no config found, default to false
    (CASE WHEN forms_per_visit_with_visit_num.istsdv IS TRUE THEN COALESCE(tsdv_visit.issdv, tsdv_general.issdv, FALSE)
    ELSE fielddef.issdv END)::BOOLEAN AS issdv,    
    fielddef.isprimaryendpoint,
    fielddef.issecondaryendpoint,
    fielddef.isrequired
  from
    forms_per_visit_with_visit_num
    join fielddef on forms_per_visit_with_visit_num.studyid = fielddef.studyid and forms_per_visit_with_visit_num.formid = fielddef.formid
    left join tsdv tsdv_general ON (forms_per_visit_with_visit_num.studyid = tsdv_general.studyid AND forms_per_visit_with_visit_num.sdvtier = tsdv_general.sdvtier AND fielddef.formid = tsdv_general.formid AND fielddef.fieldid = tsdv_general.fieldid AND tsdv_general.visit IS NULL)
    left join tsdv tsdv_visit ON (forms_per_visit_with_visit_num.studyid = tsdv_visit.studyid AND forms_per_visit_with_visit_num.sdvtier = tsdv_visit.sdvtier AND fielddef.formid = tsdv_visit.formid AND fielddef.fieldid = tsdv_visit.fieldid AND forms_per_visit_with_visit_num.visit = tsdv_visit.visit)
)

-- Now, given the fielddata shell that has all collected forms and all fields for the forms, join each record
--  with the fielddata table in order to flag which fields have data entry completed and which are not completed.
select
  fielddata_shell.comprehendid, fielddata_shell.studyid,
  fielddata_shell.siteid, fielddata_shell.usubjid,
  fielddata_shell.formid, fielddata_shell.formseq,
  fielddata_shell.visit, fielddata_shell.visitseq,
  fielddata_shell.studyname, fielddata_shell.sitename,
  fielddata_shell.sitecountry, fielddata_shell.siteregion,
  fielddata_shell.visitnum, fielddata_shell.svstdtc,
  fielddata_shell.formname, fielddata_shell.fieldid,
  fielddata_shell.fieldname, fielddata_shell.issdv,
  fielddata_shell.isprimaryendpoint, fielddata_shell.issecondaryendpoint, fielddata_shell.isrequired,
  case when coalesce(fielddata.datavalue,'') = '' then false::boolean else true::boolean end as completed,
  coalesce(fielddata.fieldseq, 1) as fieldseq,
  fielddata.datavalue,
  fielddata.dataentrydate,
  fielddata.datacollecteddate,
  fielddata.sdvdate,
  fielddata.SourceRecordPrimaryKey,
  fielddata.SourceRecordDate,
  fielddata.isdeleted,
  coalesce(fielddata.log_num, 1) as log_num,
  now()::timestamp as comprehend_update_time
from
  fielddata_shell
  left join fielddata on (fielddata_shell.comprehendid = fielddata.comprehendid) and (fielddata_shell.visit = fielddata.visit) and (fielddata_shell.formid = fielddata.formid)
   and (fielddata_shell.fieldid = fielddata.fieldid) and (fielddata_shell.visitseq = fielddata.visitseq) and (fielddata_shell.formseq = fielddata.formseq) 
where fielddata.datacollecteddate is not null and fielddata.dataentrydate is not null;


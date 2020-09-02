/*
rpt_query_rate_by_ecrf PLO

Notes: Query Rate by eCRF PLO lists all the queries that reference forms for which 
        collected data exists in the formdata USDM object. In addition, an isenrolled 
        flag is included in the results which may or may not be used to further filter the 
        results. Probably in the future, the isenrolled flag should be linked to the subject
        start disposition rather than being hardcoded to "enrolled". 

        Queries that do not have a formid value will be excluded from this PLO.

        Queries that reference a formid that is not included in the formdef/formdata
        tables will be excluded from this PLO.  
     
Revision History: 12-Sep-2016 Michelle Engler - Add header comment and Comprehend Update Time
                  20-Jan-2017 Michelle Engler - Add comments for tech writer

*/

create table rpt_query_rate_by_ecrf as
with

-- CTE to collect all subjects and include a flag as to whether the subject is enrolled or not
subject_extended as (
  with
  enrolled_subjects as (
    select comprehendid from rpt_subject_disposition where dsevent = 'ENROLLED'
  )
  select
         subject.comprehendid::text,
         subject.studyid::text,
         study.studyname::text,
         subject.siteid::text,
         site.sitename::text,
         site.sitecountry::text,
         site.siteregion::text,
         subject.usubjid::text,
         (subject.comprehendid in (select * from enrolled_subjects))::boolean as is_enrolled
  from
         study
         join site on study.comprehendid = site.studyid
         join subject on site.comprehendid = subject.sitekey),

-- CTE to include a count to collected forms for a study. Only the study forms that
--  have collected data will be included.
form_counts as (
    select
           formdata.studyid,
           formdata.formid,
           formdef.formname,
           count(*) as form_count
    from
           formdef
           join formdata on formdef.studyid = formdata.studyid and formdef.formid = formdata.formid
    group by
           formdata.studyid,
           formdata.formid,
           formdef.formname)

-- The main query is dependent on the the query table having a referenced formid that 
--  has collected form data present.  This causes a dependency between this PLO and the 
--  formdef and formdata forms being collected.  Also, if formdef/formdata do not include
--  all forms available for the study, the queries included will be limited to the forms
--  that are included in formdef/formdata.  In addition, the KPI may or may not be leveraging
--  the isenrolled flag in order to limit the results to include only those subjects that are 
--  enrolled.  Probably in the future, the functionality for the isenrolled flag should be considering the
--  subject start disposition rather than being hardcoded to "enrolled".
select
       q.comprehendid,
       q.studyid,
       subject.studyname,
       subject.siteid,
       subject.sitename,
       subject.sitecountry,
       subject.siteregion,
       subject.usubjid,
       fc.formid,
       fc.formname::text as formname,
       fc.form_count::integer as form_count,
       q.objectuniquekey,
       subject.is_enrolled,
       now()::timestamp as comprehend_update_time
from
       query as q
       join form_counts as fc on (q.formid = fc.formname and q.studyid = fc.studyid)
       join subject_extended as subject on q.comprehendid = subject.comprehendid;

/*
rpt_ae_rel_week PLO

Notes:  Retrieve all Adverse Events for enrolled subjects and include a relative trial week
        starting from the enrollment date.  TODO: Make this PLO start from the configurable 
        start disposition e.g. consented/enrolled
     
Revision History: 06-Jul-2016 Adam Kaus - Fixed join to AE causing duplicate records
                  31-Aug-2016 Adam Kaus - Added dsterm to where clause for filtering by enrollment
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  14-Jan-2017 Michelle Engler - Add comments for tech writer

*/

create table rpt_ae_rel_week as
select
        ae.comprehendid as comprehendid,
        ae.studyid,
        study.studyname,
        ae.siteid,
        site.sitename,
        site.sitecountry,
        site.siteregion,
        ae.usubjid,
        ae.aeterm,
        ae.aeseq,
        ae.aestdtc,
        ds.dsstdtc,
        -- In days, substract the disposition date from the ae start date, divide by 7 and round the final answer
        -- If aestart date is null or disposition start date is null, relative trial week will be null
        (round(extract(day from ae.aestdtc - ds.dsstdtc) / 7))::int as relative_trial_week,
        ae.objectuniquekey,
        now()::timestamp as comprehend_update_time
from
        study
        join site on study.studyid = site.studyid
        join subject on study.studyid = subject.studyid and site.siteid = subject.siteid
        join ae on subject.comprehendid = ae.comprehendid
        join rpt_subject_disposition ds on ae.comprehendid = ds.comprehendid and ds.dsevent = 'ENROLLED';

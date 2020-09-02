/*
rpttab_dv PLO
CDM Version: 2.6

Revision History: 23-Sep-2016 VinhHo - Adding new PLO
*/
CREATE TABLE rpttab_dv AS
SELECT subject.comprehendid,
       site.sitecro,
       study.studyid,
       study.studyname,
       site.siteid,
       site.sitename,
       site.sitecountry,
       site.siteregion,
       dv.usubjid,
       dv.visit,
       dv.formid,
       dv.dvcat,
       dv.dvterm,
       dv.dvstdtc,
       dv.dvendtc,
       dv.dvscat,
       dv.dvseq,
       dv.objectuniquekey,
       date_trunc('month', dv.dvstdtc)::date AS dvstdtc_month,
       now()::timestamp as comprehend_update_time
FROM study
JOIN site ON (study.studyid = site.studyid)
JOIN subject ON (site.comprehendid = subject.sitekey)
JOIN dv ON (subject.comprehendid = dv.comprehendid) ;


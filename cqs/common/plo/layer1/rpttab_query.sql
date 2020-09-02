/*
rpttab_query PLO
CDM Version: 2.6

Revision History: 23-Sep-2016 VinhHo - Adding new PLO
*/
CREATE TABLE rpttab_query AS
SELECT subject.comprehendid,
       site.sitecro,
       study.studyid,
       study.studyname,
       site.siteid,
       site.sitename,
       site.sitecountry,
       site.siteregion,
       query.usubjid,
       query.queryid,
       query.formid,
       query.fieldid,
       query.querytext,
       query.querytype,
       query.querystatus,
       query.queryopeneddate,
       query.queryresponsedate,
       query.querycloseddate,
       query.objectuniquekey,
       date_trunc('month', query.queryopeneddate)::date AS queryopeneddate_month,
       query.querycreator,
       now()::timestamp as comprehend_update_time
FROM study
JOIN site ON (study.studyid = site.studyid)
JOIN subject ON (site.comprehendid = subject.sitekey)
JOIN query ON (subject.comprehendid = query.comprehendid) ;


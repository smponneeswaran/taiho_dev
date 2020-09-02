/*
dimsubject

Note : This PLO has subject details along with the subject days dimension added to it

*/


CREATE TABLE dimsubject AS
SELECT subj.siteid,
    subj.usubjid,
    subj.studyid,
    subj.status,
    subj.exitdate,
    subjdays.subjectstartdisposition,
    subjdays.subjectexitdisposition,
    subjdays.subjectdaystartdt,
    subjdays.subjectdaysenddt,
    subjdays.thismonth,
    subjdays.thismonthsubjectdays,
    subjdays.cumulativesubjectdays,
    subjdays.totalsubjectdays,
    subj.objectuniquekey,
    now()::timestamp as comprehend_update_time
FROM subject subj
JOIN rpt_subject_days subjdays ON (subj.studyid = subjdays.studyid AND subj.usubjid = subjdays.usubjid AND subj.siteid = subjdays.siteid);


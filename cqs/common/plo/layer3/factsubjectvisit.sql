/*
factsubjectvisit

Note : This PLO has data from plo rpt_subject_visit_schedule with the additional detail of latest visit flag

*/


CREATE TABLE factsubjectvisit AS
WITH sv_data AS (SELECT sv.comprehendid, max(sv.visitnum) visitnum 
            FROM sv
            JOIN tv ON (sv.studyid = tv.studyid AND sv.visit = tv.visit) where tv.visitdy != 99999
            GROUP BY  sv.comprehendid)

SELECT s.comprehendid,
       	s.studyid,
        s.studyname,
        s.siteid,
        s.sitename,
        s.sitecountry,
        s.siteregion,
        s.usubjid,
        s.exitdate,
        s.visit,
        s.visitnum,
        s.visitdy,
        s.visitwindowbefore,
        s.visitwindowafter,
        s.first_visit,
        s.expectedvisitdate,
        s.windowopen,
        s.windowclosed,
        s.svmod_visitnum,
        s.svmod_visit,
        s.visit_start_dtc,
        s.visit_end_dtc,
        s.visitseq,
        s.expected,
        s.visit_days,
        s.category,
        s.schedule_compliance,
        s.trunc_month,
        CASE WHEN sv.comprehendid IS NULL THEN 0 ELSE 1 END AS latestvisit,
        now()::timestamp as comprehend_update_time
FROM rpt_subject_visit_schedule s 
LEFT JOIN sv_data sv ON (s.comprehendid = sv.comprehendid AND s.visitnum = sv.visitnum);


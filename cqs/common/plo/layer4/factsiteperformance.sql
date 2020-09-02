/*
factsiteperformance

Note : This PLO has details from rpt_site_performance plo along with the added detail of count of subject visits present in each site

*/

CREATE TABLE factsiteperformance AS
WITH site_visits AS (SELECT studyid, siteid, count(objectuniquekey) as visits 
                        FROM sv 
                        GROUP BY studyid, siteid)

SELECT rpt_site_performance.comprehendid,
    rpt_site_performance.studyid,
    rpt_site_performance.studyname,
    rpt_site_performance.siteid,
    rpt_site_performance.sitename,
    rpt_site_performance.sitecro,
    rpt_site_performance.sitecountry,
    rpt_site_performance.siteregion,
    rpt_site_performance.sitecraname,
    rpt_site_performance.subjects_screened_count,
    rpt_site_performance.failed_screen_count,
    rpt_site_performance.site_active_days_count,
    rpt_site_performance.enrolled_count,
    rpt_site_performance.withdrawn_count,
    rpt_site_performance.open_query_count,
    rpt_site_performance.ae_count,
    rpt_site_performance.dv_count,
    rpt_site_performance.subject_days_count,
    rpt_site_performance.screen_failure_rate,
    rpt_site_performance.enrollment_rate,
    rpt_site_performance.withdrawal_rate,
    rpt_site_performance.open_query_rate,
    rpt_site_performance.ae_rate,
    rpt_site_performance.pd_rate,
    visits.visits as subject_visits,
    now()::timestamp without time zone comprehend_update_time
FROM rpt_site_performance  
JOIN site_visits visits ON (rpt_site_performance.studyid = visits.studyid and rpt_site_performance.siteid = visits.siteid) ;

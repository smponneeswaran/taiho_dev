/*
factportfoliodaily

Description: site date series for foactportfoliodaily
Notes: mindate logic
    - use month start for earliest date among tables referenced in factportfoliodaily
    - if null use start of current month
    - added in protection to not go back further than 1/1/1970 to avoid data volume issues caused by discrepant dates
*/

CREATE TABLE dimsitedates AS
WITH minmaxdates AS (SELECT site.studyid, 
                            site.siteid, 
                            date_trunc('month', least(greatest(coalesce(min(mindt), current_date), '1/1/1970'::date), current_date))::date AS mindate,
                            current_date::date as maxdate
                                FROM (
                                    SELECT studyid, siteid, min(aestdtc) AS mindt FROM ae GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(dvstdtc) AS mindt FROM dv GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(siteactivationdate) AS mindt FROM site GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(dsstdtc) AS mindt FROM rpt_subject_disposition GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(enddate) AS mindt FROM siteplannedenrollment GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, least(min(querycloseddate), min(queryopeneddate), min(queryresponsedate)) AS mindt FROM query GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, least(min(issuecloseddate), min(issueopeneddate), min(issueresponsedate)) AS mindt FROM siteissue GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(svstdtc) AS mindt FROM sv GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(dataentrydate) AS mindt FROM fielddata GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(datacollecteddate) AS mindt FROM formdata GROUP BY 1, 2
                                    UNION ALL
                                    SELECT studyid, siteid, min(plannedvisitdate) AS mindt FROM sitemonitoringvisitschedule GROUP BY 1, 2) dt
                                JOIN site ON (dt.studyid = site.studyid AND dt.siteid = site.siteid)
                                GROUP BY 1, 2)

SELECT studyid::text AS studyid, 
        siteid::text AS siteid, 
        generate_series(mindate::date, maxdate, interval '1 day')::date as date_actual,
        now()::timestamp without time zone AS comprehend_update_time
FROM minmaxdates;

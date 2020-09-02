/*
factportfoliodaily

Note: PLO that has consolidated details FROM most of underlying objects to enable the portfolio level visibility 

*/

CREATE TABLE factportfoliodaily AS 
WITH subj_days AS --  CTE to fetch subjectdaystartdt, exit date from rpt_subject_days which then used in other CTE's
                           ( SELECT DISTINCT subjdays.studyid, subjdays.siteid, subjdays.comprehendid,
                                subject_start_date AS subjectdaystartdt, exit_date AS exitdate 
                                FROM rpt_subject_information subjdays),

     subjectdays_daily AS  -- CTE to calculate subectdays daily with similar logic used in cro scrorecard 
                            ( SELECT ds.date_actual, sd.studyid, sd.siteid, 
                                SUM(CASE  WHEN    /* 1. Exit date < subject start date */ 
                                                (sd.exitdate < sd.subjectdaystartdt) or
                                                /* 2. Subject start date > the current date */
                                                (sd.subjectdaystartdt > now()::date) 
                                                    THEN 0::integer

                                          WHEN    /* 3. Subject start disposition exists, but start date IS NULL */
                                                (sd.subjectdaystartdt IS null) or
                                                /* 4. Exit disposition exists, but exit date IS NULL */
                                                (maxdsseq.exit_seq IS NOT NULL AND sd.exitdate IS null) 
                                                    THEN null::integer

                                          WHEN   /* 5. Start Date IS greater than period date */
                                                sd.subjectdaystartdt > ds.date_actual or 
                                                /* 6. Exit Date IS less than period starting date */
                                               sd.exitdate < ds.date_actual 
                                                    THEN 0::integer

                                          WHEN   /* Subject Start Date ON or before period date AND END date greater than or equal to period date - return 1 */
                                               sd.subjectdaystartdt <= ds.date_actual AND 
                                               sd.exitdate IS NOT NULL AND 
                                               sd.exitdate >= (case WHEN   now()::date > ds.date_actual THEN ds.date_actual else now()::date end)  
                                                THEN 1::integer 

                                          WHEN   /* Subject Start Date after period date AND exit date IS NULL or exit date IS greater than ending date THEN ending date - subject start date + 1 */
                                               sd.subjectdaystartdt <= ds.date_actual AND 
                                               sd.exitdate IS NULL 
                                                 THEN 1::integer

                                          ELSE -999999::integer -- Error Condition

                                    end) AS subjectdays
                                FROM dimsitedates ds 
                                LEFT JOIN subj_days sd ON (ds.studyid = sd.studyid  AND ds.siteid = sd.siteid AND date_trunc('MONTH',sd.subjectdaystartdt)  <= ds.date_actual )
                                LEFT JOIN
                                /* List of Subjects with exiting disposition */
                                (SELECT d1.comprehendid, d2.dsstdtc, d2.dsseq AS exit_seq
                                  FROM (SELECT comprehendid, max(dsseq) dsseq 
                                        FROM rpt_subject_disposition
                                        WHERE dswithdrawn IS TRUE OR dscompleted IS TRUE
                                        GROUP BY comprehendid) d1, 
                                        rpt_subject_disposition d2 
                                        WHERE d1.comprehendid = d2.comprehendid AND d1.dsseq = d2.dsseq) maxdsseq ON (sd.comprehendid = maxdsseq.comprehendid)
                                GROUP BY  sd.studyid, sd.siteid, ds.date_actual ),


     ae_daily AS      -- CTE to calculate ae on daily basis
                            ( SELECT ae.aestdtc::date as date_actual, ae.studyid, ae.siteid,
                                COUNT(*) AS ae_count
                                FROM (SELECT ae.*, dt.subjectdaystartdt 
                                            FROM ae  
                                            JOIN subj_days dt ON (ae.comprehendid = dt.comprehendid)) ae
                                JOIN (SELECT studyid, min(dsstdtc)::date AS min_dt FROM rpt_subject_disposition ds GROUP BY studyid) md ON (ae.studyid = md.studyid)
                                WHERE ae.aestdtc >= greatest(coalesce(date_trunc('MONTH', ae.subjectdaystartdt), date_trunc('MONTH', md.min_dt)), '1/1/1970'::date)
                                GROUP BY ae.studyid, ae.siteid, ae.aestdtc::date ),

     dv_daily AS           -- CTE to calculate dv on daily basis
                            (SELECT dvstdtc::date AS date_actual, studyid, siteid, 
                                COUNT(*) AS dv_count
                                FROM dv
                                GROUP BY studyid, siteid, dvstdtc::date),
                        
     sitedays_daily AS --  CTE to calculate site days on a daily basis
                            ( SELECT studyid,
                                siteid,
                                monthtrunc AS date_actual,
                                site_days_count AS site_days_count
                                FROM rpt_site_days),

    subj_ennrolled_daily AS -- CTE to calculate enrolled subjects on a daily basis
                            ( SELECT  ds.studyid, ds.siteid, ds.dsstdtc::date AS date_actual,
                                coalesce(count(distinct ds.comprehendid),0)::numeric  AS enrolled_count
                                FROM rpt_subject_disposition ds
                                WHERE ds.dsevent = 'ENROLLED'
                                GROUP BY ds.studyid, ds.siteid, ds.dsstdtc::date),

    plannedenrollmentcount AS -- CTE to get daily planned enrollmnt count 
                            ( SELECT  pe.enddate AS date_actual, pe.studyid, pe.siteid, pe.enrollmentcount
                                FROM siteplannedenrollment pe 
                                WHERE lower(pe.enrollmenttype) = 'planned'), 

    disposition_daily_counts AS   -- CTE to get counts for all dispositions present in dimdisposition object AND aligining that with dimsitedates CTE to bring in the date series concept used similar to cro scrorecard 
                            ( SELECT  d.studyid, d.siteid, d.dsstdtc::date AS date_actual,
                                COALESCE(SUM(d.subjectenrolled),0) AS enrolled, 
                                COALESCE(SUM(d.subjectconsented),0) AS consented,  
                                COALESCE(SUM(d.subjectconsentfailures),0) AS consentfailures,
                                COALESCE(SUM(d.subjectscreened),0) screened, 
                                COALESCE(SUM(d.subjectfailedscreen),0) AS screenfailures, 
                                COALESCE(SUM(d.subjectrandomized),0) randomized, 
                                COALESCE(SUM(d.subjectfailedrandomization),0) AS randomizationfailures,
                                COALESCE(SUM(d.subjectwithdrawn),0) AS withdrawn,
                                COALESCE(SUM(d.subjectevaluable),0) AS evaluable, 
                                COALESCE(SUM(d.subjectevaluablefailures),0) AS evaluablefailures
                                FROM dimdisposition d
                                GROUP BY d.studyid, d.siteid, d.dsstdtc::date),

    query_open_daily_count AS (SELECT studyid, siteid, queryopeneddate AS date_actual,  
                                COUNT(*) AS open_query_count
                                FROM query q 
                                WHERE queryopeneddate IS NOT NULL 
                                group by studyid, siteid, queryopeneddate),

    query_response_daily_count AS (SELECT studyid, siteid, queryresponsedate AS date_actual,  
                                    COUNT(*) AS queryansweredcount
                                    FROM query
                                    WHERE queryopeneddate IS NOT NULL AND queryresponsedate IS NOT NULL
                                    group by studyid, siteid, queryresponsedate),

    query_closed_daily_count AS (SELECT studyid, siteid, querycloseddate AS date_actual,  
                                    SUM((q.querycloseddate - q.queryopeneddate)::numeric) AS queryresolutiondays,
                                    SUM(CASE WHEN lower(q.querystatus) = 'cancelled' THEN 1 ELSE 0 END) AS querycancelledcount,
                                    SUM(CASE WHEN queryresponsedate IS NOT NULL AND lower(q.querystatus) = 'cancelled' THEN 1 ELSE 0 END) AS queryansweredcancelledcount,
                                    SUM(CASE WHEN lower(q.querystatus) = 'closed' THEN 1 ELSE 0 END) AS queryclosedcount,
                                    SUM(CASE WHEN queryresponsedate IS NOT NULL AND lower(q.querystatus) = 'closed' THEN 1 ELSE 0 END) AS queryansweredclosedcount
                                    FROM query q
                                    WHERE queryopeneddate IS NOT NULL AND querycloseddate IS NOT NULL
                                    group by studyid, siteid, querycloseddate),

    siteissues_open_daily_count AS (SELECT studyid, siteid, issueopeneddate AS date_actual,
                                            COUNT(*) AS openedissues_count
                                    FROM siteissue si
                                    WHERE issueopeneddate IS NOT NULL 
                                    GROUP BY studyid, siteid, issueopeneddate),

    siteissues_response_daily_count AS (SELECT studyid, siteid, issueresponsedate AS date_actual,
                                            COUNT(*) AS resolvedissues_count
                                        FROM siteissue si
                                        WHERE issueopeneddate IS NOT NULL AND issueresponsedate IS NOT NULL 
                                        GROUP BY studyid, siteid, issueresponsedate),

    siteissues_closed_daily_count AS (SELECT studyid, siteid, issuecloseddate AS date_actual,
                                            COUNT(*) AS closedissues_count,
                                            SUM(CASE WHEN si.issueresponsedate IS NOT NULL THEN 1::numeric ELSE 0::numeric END) AS closedresolvedissues_count,
                                            SUM((si.issuecloseddate - si.issueopeneddate)) AS siteissueresolutiondays
                                        FROM siteissue si
                                        WHERE issueopeneddate IS NOT NULL AND issuecloseddate IS NOT NULL
                                        GROUP BY studyid, siteid, issuecloseddate),    

    subjectvisits_data AS   -- CTE to get subject visit realted data FROM factsubjectvisit object AND aligning that with dimsitedates CTE to bring in the date series concept used similar to cro scorecard 
                            ( SELECT v.studyid, v.siteid,  v.first_visit AS date_actual,
                                SUM(CASE WHEN   v.category = 'INWINDOW' THEN 1::numeric ELSE 0::numeric END) AS subjectvisitsonschedule_count, 
                                SUM(CASE WHEN   v.studyid IS NOT NULL AND v.expected = true THEN 1::numeric ELSE 0::numeric END) AS subjectvisits_count
                                FROM factsubjectvisit v
                                GROUP BY v.studyid, v.siteid,  v.first_visit) ,

    fielddata_daily_count AS  -- CTE to get missing data details FROM rpt_missing_data object aligining that with dimsitedates CTE to bring in the date series concept used similar to cro scrorecard 
                            ( SELECT md.studyid, md.siteid, md.dataentrydate AS date_actual,
                                        SUM(CASE WHEN   md.isprimaryendpoint AND md.completed = true THEN 1 ELSE 0 END) primaryendpointpopulatedrequiredfields,
                                        SUM(CASE WHEN   md.issecondaryendpoint AND  md.completed = true THEN 1 ELSE 0 END) secondaryendpointpopulatedrequiredfields,
                                        SUM(CASE WHEN   md.issdv AND  md.completed = true AND md.sdvdate IS NOT NULL THEN 1 ELSE 0 END) sdvpopulatedrequiredfields,
                                        SUM(CASE WHEN   md.completed = true THEN 1 ELSE 0 END) populatedrequiredfields
                                FROM rpt_missing_data md
                                WHERE md.isrequired = true
                                GROUP BY md.studyid, md.siteid,  md.dataentrydate),

    -- Because we are aggregating over dataentrydate, and a single form can have fields entered accross multiple dates, it can be a challenge to get an exact count of "expected" fields per date.
    -- Here we get the minimum dataentrydate per entered form and then assign all the expected field counts to that date by joining to fielddef
    fielddata_expected_daily_count AS (WITH field_dates AS (SELECT f.studyid, f.siteid, f.usubjid, s.sdvtier, st.istsdv, f.formid, f.formseq, f.visit, min(f.dataentrydate) dataentrydate
                                                                FROM fielddata f
                                                                JOIN subject s ON (f.comprehendid = s.comprehendid)
                                                                JOIN study st ON (s.studyid = st.studyid)
                                                                WHERE f.datacollecteddate IS NOT NULL AND f.dataentrydate IS NOT NULL
                                                                GROUP BY f.studyid, f.siteid, f.usubjid, s.sdvtier, st.istsdv, f.formid, f.formseq, f.visit),

                                                req_fields AS (SELECT f.studyid, f.siteid, f.usubjid, fldef.formid, fldef.fieldid, fldef.isprimaryendpoint, fldef.issecondaryendpoint, f.dataentrydate, f.visit,
                                                                    -- check if study level tsdv is enabled else use 100% source verification (fielddef setting)
                                                                    -- for tsdv, first check for visit-specific setting else use general setting
                                                                    -- when tsdv is enabled for the study but no config found, default to false
                                                                    (CASE WHEN f.istsdv IS TRUE THEN COALESCE(tsdv_visit.issdv, tsdv_general.issdv, FALSE)
                                                                     ELSE fldef.issdv END)::BOOLEAN AS issdv
                                                                    FROM field_dates f
                                                                    JOIN fielddef fldef ON (f.studyid = fldef.studyid AND f.formid = fldef.formid)
                                                                    LEFT JOIN tsdv tsdv_general ON (f.studyid = tsdv_general.studyid AND f.sdvtier = tsdv_general.sdvtier AND fldef.formid = tsdv_general.formid AND fldef.fieldid = tsdv_general.fieldid AND tsdv_general.visit IS NULL)
                                                                    LEFT JOIN tsdv tsdv_visit ON (f.studyid = tsdv_visit.studyid AND f.sdvtier = tsdv_visit.sdvtier AND fldef.formid = tsdv_visit.formid AND fldef.fieldid = tsdv_visit.fieldid AND f.visit = tsdv_visit.visit)
                                                                    WHERE fldef.isrequired = true)
                                        
                                        SELECT fd.studyid, fd.siteid, fd.dataentrydate AS date_actual, 
                                        COALESCE((SUM(CASE WHEN fd.isprimaryendpoint = true THEN 1 ELSE 0 END)), 0) AS expectedrequiredprimaryendpointfields,
                                        COALESCE((SUM(CASE WHEN fd.issecondaryendpoint = true THEN 1 ELSE 0 END)), 0)  AS expectedrequiredsecondaryendpointfields,
                                        COALESCE((SUM(CASE WHEN fd.issdv = true THEN 1 ELSE 0 END)), 0)  AS expectedrequiredsdvfields,
                                        COALESCE((COUNT(*)), 0)  AS expectedrequiredfields
                                        FROM req_fields fd
                                        GROUP BY fd.studyid, fd.siteid, fd.dataentrydate),

     formdata_daily_count AS  -- CTE to get all form data details FROM factformdata object AND aligining that with dimsitedates CTE to bring in the date series concept used similar to cro scrorecard 
                            ( SELECT fd.studyid, fd.siteid,  fd.datacollecteddate AS date_actual,
                                SUM(fd.dataentrydate::date - fd.datacollecteddate::date)::numeric AS formdatacollecteddays, 
                                SUM(case WHEN fd.studyid is not null then 1::int else 0::int end) AS collectedformscount,
                                SUM(case WHEN fd.isprimaryendpoint  THEN (fd.dataentrydate::date - fd.datacollecteddate::date)::numeric END) AS primaryendpointformdatacollecteddays,
                                COUNT(case WHEN   fd.isprimaryendpoint THEN fd.objectuniquekey end) AS primaryendpointcollectedforms
                                FROM factformdata fd 
                                WHERE fd.dataentrydate IS NOT NULL AND fd.datacollecteddate IS NOT null
                                GROUP BY fd.studyid, fd.siteid,  fd.datacollecteddate) ,
                              
     monitoring_visit_record_daily_count AS
                            ( SELECT v.studyid, v.siteid,  v.plannedvisitdate AS date_actual, 
                                SUM(case WHEN   v.visitdate IS NULL THEN 1 else 0 END) AS missedtrialvisits,
                                SUM(case WHEN   v.visitdate IS NOT NULL AND v.visitdate > plannedvisitdate THEN 1 else 0 END) AS latetrialvisits,
                                SUM(case WHEN   v.visitdate IS NOT NULL AND v.visitdate < plannedvisitdate THEN 1 else 0 END) AS earlytrialvisits,
                                COUNT(v.plannedvisitdate) AS plannedtrialvisits    
                                FROM rpt_monitoring_visit_record v
                                GROUP BY v.studyid, v.siteid,  v.plannedvisitdate ),

    open_query_age_daily_count AS
                            ( SELECT oqa.studyid, oqa.siteid,  oqa.queryopendate_my AS date_actual,
                                SUM(CASE WHEN oqa.category = '> 28 days' THEN 1 ELSE 0 END) query_oldest_age_count
                                FROM rpt_open_query_age oqa
                                GROUP BY oqa.studyid, oqa.siteid,  oqa.queryopendate_my)

--Final Consolidate query  
SELECT TO_CHAR(s.date_actual ,'yyyymmdd')::integer AS date_dim_id, 
        s.studyid::text AS studyid,
        s.siteid::text AS siteid,
        coalesce(dv.dv_count, 0)::integer AS protocoldeviations,
        coalesce(subjdays.subjectdays, 0)::integer AS subjectdays,
        coalesce(ae.ae_count, 0)::integer AS adverseevents,
        coalesce(subjenr.enrolled_count, 0)::integer AS subjectsenrolled,
        coalesce(sitedays.site_days_count, 0)::integer AS sitedays,
        coalesce(plndenr.enrollmentcount, 0)::integer AS plannedenrollmentcount,
        coalesce(disp.consented, 0)::integer AS consented,
        coalesce(disp.consentfailures, 0)::integer AS consentfailures,
        coalesce(disp.screened, 0)::integer AS screened,
        coalesce(disp.screenfailures, 0)::integer AS screenfailures,
        coalesce(disp.randomized, 0)::integer AS randomized,
        coalesce(disp.randomizationfailures, 0)::integer AS randomizationfailures,
        coalesce(disp.withdrawn, 0)::integer AS withdrawn,
        coalesce(disp.evaluable, 0)::integer AS evaluable,
        coalesce(disp.evaluablefailures, 0)::integer AS evaluablefailures,
        coalesce(qry_opn.open_query_count, 0)::integer AS openquerycount,
        coalesce(siss_open.openedissues_count, 0)::integer AS openedissues,
        coalesce(siss_response.resolvedissues_count, 0)::integer AS resolvedissues,
        coalesce(siss_closed.closedissues_count, 0)::integer AS closedissues,
        coalesce(siss_closed.closedresolvedissues_count, 0)::integer AS closedresolvedissues,
        coalesce(siss_closed.siteissueresolutiondays, 0)::integer AS siteissueresolutiondays, 
        coalesce(qry_clo.queryresolutiondays, 0)::integer AS queryresolutiondays,
        coalesce(qry_opn.open_query_count, 0)::integer AS queryopenedcount,
        coalesce(qry_resp.queryansweredcount, 0)::integer AS queryansweredcount,
        coalesce(qry_clo.querycancelledcount, 0)::integer AS querycancelledcount,
        coalesce(qry_clo.queryansweredclosedcount, 0)::integer AS queryansweredclosedcount,
        coalesce(qry_clo.queryansweredcancelledcount, 0)::integer AS queryansweredcancelledcount,
        coalesce(qry_clo.queryclosedcount, 0)::integer AS queryclosedcount,
        coalesce(sv_data.subjectvisitsonschedule_count, 0)::integer AS subjectvisitsonschedule,
        coalesce(sv_data.subjectvisits_count, 0)::integer AS subjectvisits,
        coalesce(fields.primaryendpointpopulatedrequiredfields, 0)::integer AS primaryendpointpopulatedrequiredfields,
        coalesce(exfields.expectedrequiredprimaryendpointfields, 0)::integer AS expectedrequiredprimaryendpointfields,
        coalesce(fields.secondaryendpointpopulatedrequiredfields, 0)::integer AS secondaryendpointpopulatedrequiredfields,
        coalesce(exfields.expectedrequiredsecondaryendpointfields, 0)::integer AS expectedrequiredsecondaryendpointfields,
        coalesce(fields.sdvpopulatedrequiredfields, 0)::integer AS sdvpopulatedrequiredfields,
        coalesce(exfields.expectedrequiredsdvfields, 0)::integer AS expectedrequiredsdvfields,
        coalesce(fields.populatedrequiredfields, 0)::integer AS populatedrequiredfields,
        coalesce(exfields.expectedrequiredfields, 0)::integer AS expectedrequiredfields,
        coalesce(frm_det.formdatacollecteddays, 0)::integer AS formdatacollecteddays,
        coalesce(frm_det.primaryendpointformdatacollecteddays, 0)::integer AS primaryendpointformdatacollecteddays,
        coalesce(frm_det.collectedformscount, 0)::integer AS collectedformscount,
        coalesce(frm_det.primaryendpointcollectedforms, 0)::integer AS primaryendpointcollectedforms,
        coalesce(mvr.missedtrialvisits, 0)::integer AS missedtrialvisits,
        coalesce(mvr.latetrialvisits, 0)::integer AS latetrialvisits,
        coalesce(mvr.earlytrialvisits, 0)::integer AS earlytrialvisits,
        coalesce(mvr.plannedtrialvisits, 0)::integer AS plannedtrialvisits,
        coalesce(oqa.query_oldest_age_count, 0)::integer AS query_oldest_age_count,
        now()::timestamp without time zone AS comprehend_update_time
FROM dimsitedates s
LEFT JOIN dv_daily dv ON (s.date_actual = dv.date_actual AND s.studyid = dv.studyid AND s.siteid = dv.siteid)
LEFT JOIN subjectdays_daily subjdays ON (s.date_actual = subjdays.date_actual AND s.studyid = subjdays.studyid AND s.siteid = subjdays.siteid)
LEFT JOIN ae_daily ae ON (s.date_actual = ae.date_actual AND s.studyid = ae.studyid AND s.siteid = ae.siteid)
LEFT JOIN subj_ennrolled_daily subjenr ON (s.date_actual = subjenr.date_actual AND s.studyid = subjenr.studyid AND s.siteid = subjenr.siteid)
LEFT JOIN sitedays_daily sitedays ON (s.date_actual = sitedays.date_actual AND s.studyid = sitedays.studyid AND s.siteid = sitedays.siteid)
LEFT JOIN plannedenrollmentcount plndenr ON (s.date_actual = plndenr.date_actual AND s.studyid = plndenr.studyid AND s.siteid = plndenr.siteid)
LEFT JOIN disposition_daily_counts disp ON (s.date_actual = disp.date_actual AND s.studyid = disp.studyid AND s.siteid = disp.siteid)
LEFT JOIN siteissues_open_daily_count siss_open ON (s.date_actual = siss_open.date_actual AND s.studyid = siss_open.studyid AND s.siteid = siss_open.siteid)
LEFT JOIN siteissues_response_daily_count siss_response ON (s.date_actual = siss_response.date_actual AND s.studyid = siss_response.studyid AND s.siteid = siss_response.siteid)
LEFT JOIN siteissues_closed_daily_count siss_closed ON (s.date_actual = siss_closed.date_actual AND s.studyid = siss_closed.studyid AND s.siteid = siss_closed.siteid)
LEFT JOIN query_open_daily_count qry_opn ON (s.date_actual = qry_opn.date_actual AND s.studyid = qry_opn.studyid AND s.siteid = qry_opn.siteid)
LEFT JOIN query_response_daily_count qry_resp ON (s.date_actual = qry_resp.date_actual AND s.studyid = qry_resp.studyid AND s.siteid = qry_resp.siteid)
LEFT JOIN  query_closed_daily_count qry_clo ON (s.date_actual = qry_clo.date_actual AND s.studyid = qry_clo.studyid AND s.siteid = qry_clo.siteid)
LEFT JOIN subjectvisits_data sv_data ON (s.date_actual = sv_data.date_actual AND s.studyid = sv_data.studyid AND s.siteid = sv_data.siteid)
LEFT JOIN fielddata_daily_count fields ON (s.date_actual = fields.date_actual AND s.studyid = fields.studyid AND s.siteid = fields.siteid)
LEFT JOIN fielddata_expected_daily_count exfields ON (s.date_actual = exfields.date_actual AND s.studyid = exfields.studyid AND s.siteid = exfields.siteid)
LEFT JOIN formdata_daily_count frm_det ON (s.date_actual = frm_det.date_actual AND s.studyid = frm_det.studyid AND s.siteid = frm_det.siteid)
LEFT JOIN monitoring_visit_record_daily_count mvr ON (s.date_actual = mvr.date_actual AND s.studyid = mvr.studyid AND s.siteid = mvr.siteid)
LEFT JOIN open_query_age_daily_count oqa ON (s.date_actual = oqa.date_actual AND s.studyid = oqa.studyid AND s.siteid = oqa.siteid);

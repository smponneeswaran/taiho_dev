CREATE TABLE rpt_site_metrics AS (

WITH study_data AS (
    SELECT DISTINCT studyid, studyname FROM study
),

site_data AS (
    SELECT DISTINCT studyid, siteid, sitename, sitecountry from site
),

--os_enrollment_percentage
os_enrollment_percentage AS (
    WITH no_of_subjects_enrolled AS (
        SELECT 
            count(*) AS cnt_enrol,
            studyid,
            siteid 
        FROM rpt_subject_disposition 
        WHERE dsstdtc IS NOT NULL AND dsevent = 'ENROLLED'
        GROUP BY studyid,siteid
    ),

    no_of_subjects_planned AS (
        SELECT 
            sum(b.enrollmentcount) AS pln_enrol,
            a.studyid,
            a.siteid 
        FROM site a
        LEFT JOIN siteplannedenrollment b ON a.studyid = b.studyid AND a.siteid = b.siteid
        GROUP BY a.studyid,a.siteid
    )

    SELECT 
        'os_enrollment_percentage'::TEXT AS metricid,
        no_of_subjects_enrolled.studyid::TEXT AS studyid,
        no_of_subjects_enrolled.siteid::TEXT AS siteid, 
        cnt_enrol::NUMERIC AS numerator, 
        pln_enrol::NUMERIC AS denominator
    FROM no_of_subjects_enrolled 
    JOIN no_of_subjects_planned ON (no_of_subjects_enrolled.studyid = no_of_subjects_planned.studyid AND no_of_subjects_enrolled.siteid = no_of_subjects_planned.siteid)
),

--os_enrollment_rate, 
os_enrollment_rate AS (
    SELECT 'os_enrollment_rate'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        COUNT(DISTINCT e.comprehendid)::NUMERIC AS numerator, -- enrolled subjects
        ROUND(((LEAST(current_date::DATE, s.sitedeactivationdate::DATE) - s.siteactivationdate::DATE)/30::NUMERIC),0) AS denominator -- site months
    FROM site s
    LEFT JOIN rpt_subject_disposition e ON (s.studyid = e.studyid AND s.siteid = e.siteid AND e.dsstdtc IS NOT NULL AND e.dsevent = 'ENROLLED')
    GROUP BY s.studyid, s.siteid, s.sitedeactivationdate, s.siteactivationdate
),
 
--os_screenfail_rate
os_screenfail_rate AS (
    WITH screened AS (-- screened (sum of enrolled + screen fail)
        SELECT 
            studyid, 
            siteid,
            SUM(CASE WHEN (dsevent = 'ENROLLED' AND dsstdtc IS NOT NULL) THEN 1::INTEGER 
                     WHEN (dsevent = 'FAILED_SCREEN' AND dsstdtc IS NOT NULL) THEN 1::INTEGER 
                     ELSE 0::INTEGER 
                END)::INTEGER AS screened_subjects
        FROM rpt_subject_disposition
        GROUP BY studyid, siteid
        )

    SELECT
        'os_screenfail_rate'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        COUNT(DISTINCT sf.comprehendid)::NUMERIC  AS numerator, -- screen failure subjects
        sc.screened_subjects::NUMERIC AS denominator-- screened subjects
    FROM site s --  screen failure
    LEFT JOIN rpt_subject_disposition sf ON (s.studyid = sf.studyid AND s.siteid = sf.siteid AND sf.dsstdtc IS NOT NULL AND sf.dsevent = 'FAILED_SCREEN')
    LEFT JOIN screened sc ON (s.studyid = sc.studyid AND s.siteid = sc.siteid)
    GROUP BY s.studyid, s.siteid, sc.screened_subjects
),

--os_withdrawal_rate
os_withdrawal_rate AS (
    SELECT 
        'os_withdrawal_rate'::TEXT AS metricid, 
        s.studyid::TEXT,
        s.siteid::TEXT,
        COUNT(DISTINCT w.comprehendid)::NUMERIC AS numerator, -- withdrawn subjects
        COUNT(DISTINCT e.comprehendid)::NUMERIC AS denominator -- enrolled subjects
    FROM site s -- enrolled
    LEFT JOIN rpt_subject_disposition e ON (s.studyid = e.studyid AND s.siteid = e.siteid AND e.dsstdtc IS NOT NULL AND e.dsevent = 'ENROLLED')
    LEFT JOIN rpt_subject_disposition w ON (s.studyid = w.studyid AND s.siteid = w.siteid AND w.dsstdtc IS NOT NULL AND w.dsevent = 'WITHDRAWN')
    GROUP BY s.studyid, s.siteid
),

--os_data_completeness
os_data_completeness AS (
    SELECT
        'os_data_completeness'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        SUM(CASE WHEN d.isrequired = TRUE AND d.completed = TRUE THEN 1
                ELSE 0 END)::NUMERIC AS numerator, -- required AND completed fields
        SUM(CASE WHEN d.isrequired = TRUE THEN 1
                ELSE 0 END)::NUMERIC AS denominator -- all required fields
    FROM site s
    LEFT JOIN rpt_missing_data d ON (s.studyid = d.studyid AND s.siteid = d.siteid)
    GROUP BY s.studyid, s.siteid
),

--os_dataentry_time
os_dataentry_time AS (
    SELECT 'os_dataentry_time'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        SUM(f.dataentrydate::DATE - f.datacollecteddate::DATE)::NUMERIC AS numerator, -- data collected days
        COUNT(f.objectuniquekey)::NUMERIC AS denominator -- total forms
    FROM site s 
    LEFT JOIN formdata f ON (s.studyid = f.studyid and s.siteid = f.siteid)
    WHERE f.datacollecteddate IS NOT NULL AND f.dataentrydate IS NOT NULL
    GROUP BY s.studyid, s.siteid
),

--os_query_rate
os_query_rate AS (
    WITH ds_subject_days AS (
        SELECT
            sd.studyid::TEXT,
            sd.siteid::TEXT,
            sum(sd.totalsubjectdays)::NUMERIC AS totalsubjectdays 
        FROM (
            SELECT
                DISTINCT studyid,
                siteid,
                usubjid,
                totalsubjectdays
            FROM rpt_subject_days
        ) sd
        GROUP BY sd.studyid,sd.siteid  
    )
    SELECT 'os_query_rate'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        COUNT(q.objectuniquekey)::NUMERIC AS numerator, -- total queries
        d.totalsubjectdays::NUMERIC AS denominator -- subject days
    FROM site s 
    LEFT JOIN query q ON (s.studyid = q.studyid AND s.siteid = q.siteid)
    LEFT JOIN ds_subject_days d ON (s.studyid = d.studyid AND s.siteid = d.siteid)
    GROUP BY s.studyid, s.siteid, d.totalsubjectdays
),

--os_query_resolution
os_query_resolution AS (
    SELECT
        'os_query_resolution'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        SUM(q.querycloseddate::DATE - q.queryopeneddate::DATE)::NUMERIC AS numerator, -- days to close queries,
        COUNT(CASE WHEN LOWER(q.querystatus) = 'closed' THEN q.objectuniquekey ELSE NULL END)::NUMERIC AS denominator -- total closed queries
    FROM site s
    LEFT JOIN query q ON (s.studyid = q.studyid AND s.siteid = q.siteid AND q.querycloseddate IS NOT NULL) -- only want closed queries
    GROUP BY s.studyid, s.siteid
),

--os_issue_resolution
os_issue_resolution AS (
    SELECT
        'os_issue_resolution'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        SUM( i.issuecloseddate::DATE - i.issueopeneddate::DATE)::NUMERIC AS numerator, -- days to close issues,
        COUNT( i.objectuniquekey)::NUMERIC AS denominator -- total closed issues
    FROM site s
    LEFT JOIN siteissue i ON (s.studyid = i.studyid AND s.siteid = i.siteid AND i.issuecloseddate IS NOT NULL) -- only want closed issues
    GROUP BY s.studyid, s.siteid
),

--os_deviation_rate
os_deviation_rate AS (
    WITH ds_subject_days AS (
        SELECT
            sd.studyid::TEXT,
            sd.siteid::TEXT,
            sum(sd.totalsubjectdays)::NUMERIC AS totalsubjectdays 
        FROM (
            SELECT
                DISTINCT
                studyid,
                siteid,
                usubjid,
                totalsubjectdays
            FROM rpt_subject_days
        ) sd
        GROUP BY sd.studyid,sd.siteid 
    )
    SELECT 'os_deviation_rate'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        COUNT(d.objectuniquekey)::NUMERIC AS numerator, -- total deviations
        sd.totalsubjectdays::NUMERIC AS denominator -- subject days
    FROM site s
    LEFT JOIN dv d ON (s.studyid = d.studyid AND s.siteid = d.siteid AND d.dvstdtc IS NOT NULL)
    LEFT JOIN ds_subject_days sd ON (s.studyid = sd.studyid AND s.siteid = sd.siteid)
    GROUP BY s.studyid, s.siteid, sd.totalsubjectdays
),

--os_ae_rate
os_ae_rate AS (
    WITH subject_days AS (
        WITH min_date AS (
            SELECT
                ds.studyid,
                MIN(ds.dsstdtc) AS dsmindate
            FROM rpt_subject_disposition ds
            GROUP BY ds.studyid
        )
        SELECT 
            DISTINCT sd.studyid,
            sd.sitecountry,
            sd.siteid,
            sd.usubjid,
            sd.totalsubjectdays,
            sd.subjectdaystartdt,
            GREATEST(COALESCE(DATE_TRUNC('MONTH', sd.subjectdaystartdt), DATE_TRUNC('MONTH', md.dsmindate)), '1/1/1970'::DATE)::DATE AS subjectmindate
        FROM rpt_subject_days sd LEFT JOIN min_date md ON (sd.studyid = md.studyid)
    ),
    site_days AS (
        SELECT
            sd.studyid::TEXT,
            sd.siteid::TEXT,
            sum(sd.totalsubjectdays)::NUMERIC AS totalsubjectdays 
        FROM subject_days sd
        GROUP BY sd.studyid, sd.siteid 
    )
    SELECT
        'os_ae_rate'::TEXT AS metricid,
        s.studyid::TEXT,
        s.siteid::TEXT,
        count(ae.objectuniquekey)::NUMERIC AS numerator, -- total AEs
        sd.totalsubjectdays::NUMERIC AS denominator -- subject days
    FROM site s
    LEFT JOIN site_days sd ON (s.studyid = sd.studyid AND s.siteid = sd.siteid)
    LEFT JOIN subject_days sub ON (s.studyid = sub.studyid AND s.siteid = sub.siteid)
    LEFT JOIN ae ON (sub.studyid = ae.studyid AND sub.siteid = ae.siteid 
                    AND sub.usubjid = ae.usubjid AND ae.aestdtc >= sub.subjectmindate AND ae.aestdtc::DATE <= NOW()::date) -- filter AEs by date
    GROUP BY s.studyid, s.siteid, sd.totalsubjectdays
)

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, ep.metricid::TEXT AS metricid,
    ep.numerator::NUMERIC AS numerator, ep.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || ep.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_enrollment_percentage ep
JOIN study_data s ON (ep.studyid= s.studyid)
JOIN site_data sd ON (ep.studyid= sd.studyid and ep.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, er.metricid::TEXT AS metricid,
    er.numerator::NUMERIC AS numerator, er.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || er.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_enrollment_rate er
JOIN study_data s ON (er.studyid= s.studyid)
JOIN site_data sd ON (er.studyid= sd.studyid and er.siteid = sd.siteid)

UNION ALL

SELECT
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, sr.metricid::TEXT AS metricid,
    sr.numerator::NUMERIC AS numerator, sr.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || sr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_screenfail_rate sr
JOIN study_data s ON (sr.studyid= s.studyid)
JOIN site_data sd ON (sr.studyid= sd.studyid and sr.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, wr.metricid::TEXT AS metricid,
    wr.numerator::NUMERIC AS numerator, wr.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || wr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_withdrawal_rate wr
JOIN study_data s ON (wr.studyid= s.studyid)
JOIN site_data sd ON (wr.studyid= sd.studyid and wr.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, dc.metricid::TEXT AS metricid,
    dc.numerator::NUMERIC AS numerator, dc.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || dc.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_data_completeness dc
JOIN study_data s ON (dc.studyid= s.studyid)
JOIN site_data sd ON (dc.studyid= sd.studyid and dc.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, dt.metricid::TEXT AS metricid,
    dt.numerator::NUMERIC AS numerator, dt.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || dt.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_dataentry_time dt
JOIN study_data s ON (dt.studyid= s.studyid)
JOIN site_data sd ON (dt.studyid= sd.studyid and dt.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, q.metricid::TEXT AS metricid,
    q.numerator::NUMERIC AS numerator, q.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || q.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_query_rate q
JOIN study_data s ON (q.studyid= s.studyid)
JOIN site_data sd ON (q.studyid= sd.studyid and q.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, qr.metricid::TEXT AS metricid,
    qr.numerator::NUMERIC AS numerator, qr.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || qr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_query_resolution qr
JOIN study_data s ON (qr.studyid= s.studyid)
JOIN site_data sd ON (qr.studyid= sd.studyid and qr.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, ir.metricid::TEXT AS metricid,
    ir.numerator::NUMERIC AS numerator, ir.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || ir.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_issue_resolution ir
JOIN study_data s ON (ir.studyid= s.studyid)
JOIN site_data sd ON (ir.studyid= sd.studyid and ir.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, dr.metricid::TEXT AS metricid,
    dr.numerator::NUMERIC AS numerator, dr.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || dr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_deviation_rate dr
JOIN study_data s ON (dr.studyid= s.studyid)
JOIN site_data sd ON (dr.studyid= sd.studyid and dr.siteid = sd.siteid)

UNION ALL

SELECT 
    (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, sd.siteid::text as siteid, sd.sitename::text as sitename, sd.sitecountry::TEXT AS sitecountry, ar.metricid::TEXT AS metricid,
    ar.numerator::NUMERIC AS numerator, ar.denominator::NUMERIC AS denominator, (s.studyid || '~' || sd.siteid || '~' || ar.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM os_ae_rate ar
JOIN study_data s ON (ar.studyid= s.studyid)
JOIN site_data sd ON (ar.studyid= sd.studyid and ar.siteid = sd.siteid)
);

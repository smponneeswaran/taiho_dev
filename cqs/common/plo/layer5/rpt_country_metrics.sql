CREATE TABLE rpt_country_metrics AS (

WITH study_data AS (
    SELECT DISTINCT studyid, studyname FROM study
),

--c_enrollment_percentage
c_enrollment_percentage AS (
    WITH no_of_subjects_enrolled AS (
        SELECT 
            count(*) AS cnt_enrol,
            studyid,
            sitecountry 
        FROM rpt_subject_disposition 
        WHERE dsstdtc IS NOT NULL AND dsevent = 'ENROLLED'
        GROUP BY studyid,sitecountry
    ),

    no_of_subjects_planned AS (
        SELECT 
            sum(enrollmentcount) AS pln_enrol,
            a.studyid,
            b.sitecountry 
        FROM siteplannedenrollment a
        JOIN site b ON a.studyid = b.studyid AND a.siteid = b.siteid
        GROUP BY a.studyid,sitecountry
    )

    SELECT 
        'c_enrollment_percentage'::TEXT AS metricid,
        no_of_subjects_enrolled.studyid::TEXT AS studyid,
        no_of_subjects_enrolled.sitecountry::TEXT AS sitecountry, 
        cnt_enrol::NUMERIC AS numerator, 
        pln_enrol::NUMERIC AS denominator
    FROM no_of_subjects_enrolled 
    JOIN no_of_subjects_planned ON no_of_subjects_enrolled.studyid = no_of_subjects_planned.studyid AND no_of_subjects_enrolled.sitecountry = no_of_subjects_planned.sitecountry
),

--c_enrollment_rate, 
c_enrollment_rate AS (
    WITH ds_denominator AS (
        SELECT 
            studyid,
            sitecountry,
            sum(site_months) AS site_months
        FROM (
              SELECT 
                studyid::TEXT,
                sitecountry::TEXT,
                siteid,
                round(((LEAST(current_date::DATE, sitedeactivationdate::DATE) - siteactivationdate::DATE)/30::NUMERIC),0)::NUMERIC AS site_months
              FROM site
        ) s 
        GROUP BY s.studyid, s.sitecountry
    ),
    ds_numerator AS (
        SELECT 
            studyid,
            sitecountry,
            sum(enrolled_subjects) AS enrolled_subjects 
        FROM (
            SELECT 
                studyid,
                sitecountry,
                siteid, 
                (COUNT(DISTINCT comprehendid)::NUMERIC) AS enrolled_subjects
            FROM rpt_subject_disposition 
            WHERE dsstdtc IS NOT NULL AND dsevent = 'ENROLLED'
            GROUP BY studyid, sitecountry,siteid
        ) a 
        GROUP BY a.studyid, a.sitecountry
    )
    SELECT 
        'c_enrollment_rate'::TEXT AS metricid, 
        a.studyid,
        a.sitecountry,
        b.enrolled_subjects AS numerator,
        a.site_months AS denominator
    FROM  
    ds_denominator a
    JOIN ds_numerator b ON a.studyid = b.studyid AND a.sitecountry = b.sitecountry
),
 
--c_screenfail_rate
c_screenfail_rate AS (
    WITH screened AS (-- screened (sum of enrolled + screen fail)
        SELECT 
            studyid, 
            sitecountry,
            SUM(CASE WHEN (dsevent = 'ENROLLED' AND dsstdtc IS NOT NULL) THEN 1::INTEGER 
                     WHEN (dsevent = 'FAILED_SCREEN' AND dsstdtc IS NOT NULL) THEN 1::INTEGER 
                     ELSE 0::INTEGER 
                END)::INTEGER AS screened_subjects
        FROM rpt_subject_disposition
        GROUP BY studyid, sitecountry
        )

    SELECT
        'c_screenfail_rate'::TEXT AS metricid,
        s.studyid::TEXT,
        s.sitecountry::TEXT,
        COUNT(DISTINCT sf.comprehendid)::NUMERIC  AS numerator, -- screen failure subjects
        sc.screened_subjects::NUMERIC AS denominator-- screened subjects
    FROM site s --  screen failure
    LEFT JOIN rpt_subject_disposition sf ON (s.studyid = sf.studyid AND s.sitecountry = sf.sitecountry AND sf.dsstdtc IS NOT NULL AND sf.dsevent = 'FAILED_SCREEN')
    LEFT JOIN screened sc ON (s.studyid = sc.studyid AND s.sitecountry = sc.sitecountry)
    GROUP BY s.studyid, s.sitecountry, sc.screened_subjects
),

--c_withdrawal_rate
c_withdrawal_rate AS (
    SELECT 
        'c_withdrawal_rate'::TEXT AS metricid, 
        s.studyid::TEXT,
        s.sitecountry::TEXT,
        COUNT(DISTINCT w.comprehendid)::NUMERIC AS numerator, -- withdrawn subjects
        COUNT(DISTINCT e.comprehendid)::NUMERIC AS denominator -- enrolled subjects
    FROM site s -- enrolled
    LEFT JOIN rpt_subject_disposition e ON (s.studyid = e.studyid AND s.sitecountry = e.sitecountry AND e.dsstdtc IS NOT NULL AND e.dsevent = 'ENROLLED')
    LEFT JOIN rpt_subject_disposition w ON (s.studyid = w.studyid AND s.sitecountry = w.sitecountry AND w.dsstdtc IS NOT NULL AND w.dsevent = 'WITHDRAWN')
    GROUP BY s.studyid, s.sitecountry
),

--c_data_completeness
c_data_completeness AS (
    SELECT
        'c_data_completeness'::TEXT AS metricid,
        s.studyid::TEXT,
        s.sitecountry::TEXT,
        SUM(CASE WHEN d.isrequired = TRUE AND d.completed = TRUE THEN 1
                ELSE 0 END)::NUMERIC AS numerator, -- required AND completed fields
        SUM(CASE WHEN d.isrequired = TRUE THEN 1
                ELSE 0 END)::NUMERIC AS denominator -- all required fields
    FROM site s
    LEFT JOIN rpt_missing_data d ON (s.studyid = d.studyid AND s.siteid = d.siteid AND s.sitecountry = d.sitecountry)
    GROUP BY s.studyid, s.sitecountry
),

--c_dataentry_time
c_dataentry_time AS (
  WITH site_level_dataentry_time_results AS (
    SELECT
        s.studyid,
        s.sitecountry,
        s.siteid,
        SUM(f.dataentrydate::DATE - f.datacollecteddate::DATE)::NUMERIC AS numerator, -- data collected days
        COUNT(f.objectuniquekey)::NUMERIC AS denominator -- total forms
    FROM site s 
    LEFT JOIN formdata f ON (s.studyid = f.studyid and s.siteid = f.siteid)
    WHERE f.datacollecteddate IS NOT NULL AND f.dataentrydate IS NOT NULL
    GROUP BY s.studyid, s.sitecountry, s.siteid
   )
   SELECT 
      'c_dataentry_time'::TEXT AS metricid,
      studyid::TEXT AS studyid,
      sitecountry::TEXT AS sitecountry,
      SUM(numerator)::NUMERIC AS numerator,        --rolled up site level KPI numerator value at country level
      SUM(denominator)::NUMERIC AS denominator     --rolled up site level KPI denominator value at country level
   FROM site_level_dataentry_time_results
   WHERE numerator >= 0                            --excluding sites with negative numerator values of the KPI
   GROUP BY metricid, studyid, sitecountry
),

--c_query_rate
c_query_rate AS (
    WITH ds_queries AS (
        SELECT
            studyid,
            sitecountry,
            sum(objectuniquekey) AS query_count
        FROM (
            SELECT
                s.studyid,
                s.sitecountry,
                s.siteid,
                queries.objectuniquekey
            FROM site s 
            LEFT JOIN 
                (
                SELECT
                    q.studyid,
                    q.siteid,
                COUNT(q.objectuniquekey)::numeric AS objectuniquekey
                FROM query q 
                GROUP BY studyid,siteid
            ) queries 
            ON s.studyid = queries.studyid AND s.siteid = queries.siteid
        ) a
        GROUP BY studyid, sitecountry
    ),
    ds_subject_days AS (
        SELECT
            sd.studyid::TEXT,
            sd.sitecountry::TEXT,
            sum(sd.totalsubjectdays)::NUMERIC AS totalsubjectdays 
        FROM (
            SELECT
                DISTINCT studyid,
                sitecountry,
                usubjid,
                totalsubjectdays
            FROM rpt_subject_days
        ) sd
        GROUP BY sd.studyid,sd.sitecountry  
    )
    SELECT 
        'c_query_rate'::TEXT AS metricid,
        s.studyid::TEXT AS studyid,
        s.sitecountry::TEXT AS sitecountry,
        q.query_count::NUMERIC AS numerator, -- total queries
        s.totalsubjectdays::NUMERIC AS denominator -- subject days
    FROM ds_queries q
    LEFT JOIN ds_subject_days s ON q.studyid = s.studyid AND q.sitecountry = s.sitecountry
),

--c_query_resolution
c_query_resolution AS (
  WITH site_level_query_resolution_results AS (
    SELECT
        s.studyid,
        s.sitecountry,
        s.siteid,
        SUM(q.querycloseddate::DATE - q.queryopeneddate::DATE)::NUMERIC AS numerator, -- days to close queries,
        COUNT(CASE WHEN LOWER(q.querystatus) = 'closed' THEN q.objectuniquekey ELSE NULL END)::NUMERIC AS denominator -- total closed queries
    FROM site s
    LEFT JOIN query q ON (s.studyid = q.studyid AND s.siteid = q.siteid AND q.querycloseddate IS NOT NULL) -- only want closed queries
    GROUP BY s.studyid, s.sitecountry, s.siteid
   )
   SELECT 
      'c_query_resolution'::TEXT AS metricid,
      studyid::TEXT AS studyid,
      sitecountry::TEXT AS sitecountry,
      SUM(numerator)::NUMERIC AS numerator,        --rolled up site level KPI numerator value at country level
      SUM(denominator)::NUMERIC AS denominator     --rolled up site level KPI denominator value at country level
   FROM site_level_query_resolution_results
   WHERE numerator >= 0                            --excluding sites with negative numerator values of the KPI
   GROUP BY metricid, studyid, sitecountry
),

--c_issue_resolution
c_issue_resolution AS (
  WITH site_level_issue_resolution_results AS (
    SELECT
        s.studyid,
        s.sitecountry,
        s.siteid,
        SUM( i.issuecloseddate::DATE - i.issueopeneddate::DATE)::NUMERIC AS numerator, -- days to close issues,
        COUNT( i.objectuniquekey)::NUMERIC AS denominator -- total closed issues
    FROM site s
    LEFT JOIN siteissue i ON (s.studyid = i.studyid AND s.siteid = i.siteid AND i.issuecloseddate IS NOT NULL) -- only want closed issues
    GROUP BY s.studyid, s.sitecountry, s.siteid
  )
  
    SELECT 
       'c_issue_resolution'::TEXT AS metricid,
       studyid::TEXT AS studyid,
       sitecountry::TEXT AS sitecountry,
       SUM(numerator)::NUMERIC AS numerator,     --rolled up site level KPI numerator value at country level
       SUM(denominator)::NUMERIC AS denominator  --rolled up site level KPI denominator value at country level
    FROM site_level_issue_resolution_results
    WHERE numerator >= 0                         --excluding sites with negative numerator values of the KPI
    GROUP BY metricid, studyid, sitecountry
),

--c_deviation_rate
c_deviation_rate AS (
    WITH ds_dv AS (
        SELECT
            studyid,
            sitecountry,
            sum(total_deviations) AS query_count
        FROM (
            SELECT
                s.studyid,
                s.sitecountry,
                s.siteid,
                COALESCE(dv.total_deviations,0) AS total_deviations 
            FROM site s  
            LEFT JOIN (
                SELECT
                    dv.studyid,
                    dv.siteid,
                    COUNT(dv.objectuniquekey)::numeric AS total_deviations
                FROM dv dv 
                WHERE dv.dvstdtc IS NOT NULL 
                GROUP BY studyid,siteid
            ) dv ON s.studyid = dv.studyid AND s.siteid = dv.siteid
        )a
        GROUP BY studyid, sitecountry
    ),
    ds_subject_days AS (
        SELECT
            sd.studyid::TEXT,
            sd.sitecountry::TEXT,
            sum(sd.totalsubjectdays)::NUMERIC AS totalsubjectdays 
        FROM (
            SELECT
                DISTINCT
                studyid,
                sitecountry,
                usubjid,
                totalsubjectdays
            FROM rpt_subject_days
        ) sd
        GROUP BY sd.studyid,sd.sitecountry 
    )
    SELECT 
        'c_deviation_rate'::TEXT AS metricid,
        s.studyid::TEXT AS studyid,
        s.sitecountry::TEXT AS sitecountry,
        ds_dv.query_count::NUMERIC AS numerator, -- total queries
        s.totalsubjectdays::NUMERIC AS denominator -- subject days
    FROM ds_dv 
    LEFT JOIN ds_subject_days s ON ds_dv.studyid = s.studyid AND ds_dv.sitecountry = s.sitecountry
),

--c_ae_rate
c_ae_rate AS (
  WITH site_level_ae_results AS (
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
        s.studyid,
        s.sitecountry,
        s.siteid,
        count(ae.objectuniquekey)::NUMERIC AS numerator, -- total AEs
        sd.totalsubjectdays::NUMERIC AS denominator -- subject days
    FROM site s
    LEFT JOIN site_days sd ON (s.studyid = sd.studyid AND s.siteid = sd.siteid)
    LEFT JOIN subject_days sub ON (s.studyid = sub.studyid AND s.siteid = sub.siteid)
    LEFT JOIN ae ON (sub.studyid = ae.studyid AND sub.siteid = ae.siteid 
                    AND sub.usubjid = ae.usubjid AND ae.aestdtc >= sub.subjectmindate AND ae.aestdtc::DATE <= NOW()::date) -- filter AEs by date
    GROUP BY s.studyid, s.sitecountry, s.siteid, sd.totalsubjectdays
    )
    
   SELECT 
     'c_ae_rate'::TEXT AS metricid,
     studyid::TEXT,
     sitecountry::TEXT,
     SUM(numerator)::NUMERIC AS numerator,  --rolled up site level KPI numerator value at country level
     SUM(denominator)::NUMERIC AS denominator --rolled up site level KPI denominator value at country level
   FROM site_level_ae_results
   GROUP by metricid, studyid, sitecountry
)

SELECT 
    (s.studyid || '~' || ep.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname, ep.sitecountry::TEXT AS sitecountry,ep.metricid::TEXT AS metricid,
    ep.numerator::NUMERIC AS numerator, ep.denominator::NUMERIC AS denominator, (s.studyid || '~' || ep.sitecountry || '~' || ep.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_enrollment_percentage ep
JOIN study_data s ON (ep.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || er.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , er.sitecountry::TEXT AS sitecountry, er.metricid::TEXT AS metricid,
    er.numerator::NUMERIC AS numerator, er.denominator::NUMERIC AS denominator, (s.studyid || '~' || er.sitecountry || '~' || er.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_enrollment_rate er
JOIN study_data s ON (er.studyid= s.studyid)

UNION ALL

SELECT
    (s.studyid || '~' || sr.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , sr.sitecountry::TEXT AS sitecountry, sr.metricid::TEXT AS metricid,
    sr.numerator::NUMERIC AS numerator, sr.denominator::NUMERIC AS denominator, (s.studyid || '~' || sr.sitecountry || '~' || sr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_screenfail_rate sr
JOIN study_data s ON (sr.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || wr.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , wr.sitecountry::TEXT AS sitecountry, wr.metricid::TEXT AS metricid,
    wr.numerator::NUMERIC AS numerator, wr.denominator::NUMERIC AS denominator, (s.studyid || '~' || wr.sitecountry || '~' || wr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_withdrawal_rate wr
JOIN study_data s ON (wr.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || dc.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , dc.sitecountry::TEXT AS sitecountry, dc.metricid::TEXT AS metricid,
    dc.numerator::NUMERIC AS numerator, dc.denominator::NUMERIC AS denominator, (s.studyid || '~' || dc.sitecountry || '~' || dc.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_data_completeness dc
JOIN study_data s ON (dc.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || dt.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , dt.sitecountry::TEXT AS sitecountry, dt.metricid::TEXT AS metricid,
    dt.numerator::NUMERIC AS numerator, dt.denominator::NUMERIC AS denominator, (s.studyid || '~' || dt.sitecountry || '~' || dt.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_dataentry_time dt
JOIN study_data s ON (dt.studyid= s.studyid)

UNION ALL
    SELECT 
    (s.studyid || '~' || q.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , q.sitecountry::TEXT AS sitecountry, q.metricid::TEXT AS metricid,
    q.numerator::NUMERIC AS numerator, q.denominator::NUMERIC AS denominator, (s.studyid || '~' || q.sitecountry || '~' || q.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_query_rate q
JOIN study_data s ON (q.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || qr.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , qr.sitecountry::TEXT AS sitecountry, qr.metricid::TEXT AS metricid,
    qr.numerator::NUMERIC AS numerator, qr.denominator::NUMERIC AS denominator, (s.studyid || '~' || qr.sitecountry || '~' || qr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_query_resolution qr
JOIN study_data s ON (qr.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || ir.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , ir.sitecountry::TEXT AS sitecountry, ir.metricid::TEXT AS metricid,
    ir.numerator::NUMERIC AS numerator, ir.denominator::NUMERIC AS denominator, (s.studyid || '~' || ir.sitecountry || '~' || ir.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_issue_resolution ir
JOIN study_data s ON (ir.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || dr.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , dr.sitecountry::TEXT AS sitecountry, dr.metricid::TEXT AS metricid,
    dr.numerator::NUMERIC AS numerator, dr.denominator::NUMERIC AS denominator, (s.studyid || '~' || dr.sitecountry || '~' || dr.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_deviation_rate dr
JOIN study_data s ON (dr.studyid= s.studyid)

UNION ALL

SELECT 
    (s.studyid || '~' || ae.sitecountry)::TEXT AS comprehendid, s.studyid::TEXT AS studyid, s.studyname::TEXT AS studyname , ae.sitecountry::TEXT AS sitecountry, ae.metricid::TEXT AS metricid,
    ae.numerator::NUMERIC AS numerator, ae.denominator::NUMERIC AS denominator, (s.studyid || '~' || ae.sitecountry || '~' || ae.metricid)::TEXT AS objectuniquekey, now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time
FROM c_ae_rate ae
JOIN study_data s ON (ae.studyid= s.studyid)
);

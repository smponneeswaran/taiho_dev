/*
rpt_study_risk_analytics PLO

Notes:  
    - Rates from siteplannedstatistic are converted by dividing by 100
    - actual_study_days = number of days between FIRST SUBJECT IN actual sitemilestone and SiteDeactivationDate or current date
    - PD and AE actual counts only include records that occur within study days range (consistent with other PLOs)
     
Revision History: 21-Jun-2016 Adam Kaus - Initial version
                  31-Aug-2016 Adam Kaus - Added dsterm to where clause for filtering by enrollment
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  07-Dec-2016 Adam Kaus - Modified PD and AE count logic to match the rpt_ae_rate_by_subject_days PLO (TP 18770)
                  28-Feb-2017 Cuong Pham - Update siteplannedstatistic instead of siteplanstatistic table
*/

CREATE TABLE rpt_study_risk_analytics AS
WITH earliest_study_date AS (SELECT studyid, MIN(dsstdtc)::DATE AS min_dsstdtc 
                                FROM rpt_subject_disposition 
                                GROUP BY studyid),

        subject_dates AS (SELECT ds.comprehendid, MIN(ds.dsstdtc)::DATE AS start_date 
                            FROM rpt_subject_disposition ds 
                            WHERE dsterm = study_start_disposition
                            GROUP BY ds.comprehendid) 

SELECT site.comprehendid::text AS comprehendid,
        site.sitecro::text AS cro,
        study.therapeuticarea::text AS therapeuticarea,
        study.program::text AS program,
        study.studyid::text AS studyid,
        study.studyname::text AS studyname,
        site.siteid::text AS siteid,
        site.sitename::text AS sitename,
        site.sitecountry::text AS sitecountry,
        site.siteregion::text AS siteregion,
        pdrate.statval::numeric AS expected_pd_rate,
        sfrate.statval::numeric AS expected_screen_failure_rate,
        aerate.statval::numeric AS expected_ae_rate,
        coalesce(dv.dv_count, 0)::integer AS actual_pd_count,
        coalesce(sf.sf_count, 0)::integer AS actual_screen_failure_count,
        coalesce(ae.ae_count, 0)::integer AS actual_ae_count,
        coalesce((coalesce(site.sitedeactivationdate, current_date) -  fsi_actual.expecteddate ), 0)::integer AS actual_study_days,
        coalesce(enr.enr_count, 0)::integer AS enrollment_count,
        now()::timestamp as comprehend_update_time
FROM study
JOIN site ON (study.studyid = site.studyid)
LEFT JOIN (SELECT studyid, siteid, (statval/100::numeric)::numeric statval
            FROM siteplannedstatistic 
            WHERE upper(statcat) = 'PROTOCOL_DEVIATION' and upper(statsubcat) = 'RATE') pdrate ON (site.studyid = pdrate.studyid AND site.siteid = pdrate.siteid)
LEFT JOIN (SELECT studyid, siteid, (statval/100::numeric)::numeric statval
            FROM siteplannedstatistic 
            WHERE upper(statcat) = 'SCREEN_FAILURE' and upper(statsubcat) = 'RATE') sfrate ON (site.studyid = sfrate.studyid AND site.siteid = sfrate.siteid)
LEFT JOIN (SELECT studyid, siteid, (statval/100::numeric)::numeric statval
            FROM siteplannedstatistic 
            WHERE upper(statcat) = 'AE' and upper(statsubcat) = 'RATE') aerate ON (site.studyid = aerate.studyid AND site.siteid = aerate.siteid)
LEFT JOIN (SELECT dv.studyid, dv.siteid, count(*) dv_count
            FROM (SELECT comprehendid, studyid, siteid, dvseq,
                        (date_trunc('month', dvstdtc) + interval '1 month' - interval '1 day')::date AS month_end
                    FROM dv) dv
            JOIN earliest_study_date es ON (dv.studyid = es.studyid)
            JOIN subject_dates sd ON (dv.comprehendid = sd.comprehendid)
            WHERE dv.month_end >= greatest(sd.start_date, es.min_dsstdtc) 
            AND dv.month_end <= now()::date
            GROUP BY 1, 2) dv ON (site.studyid = dv.studyid AND site.siteid = dv.siteid)
LEFT JOIN (SELECT studyid, siteid, count(*) sf_count
            FROM rpt_subject_disposition
            WHERE dsevent = 'FAILED_SCREEN'
            GROUP BY 1, 2) sf ON (site.studyid = sf.studyid AND site.siteid = sf.siteid)
LEFT JOIN (SELECT ae.studyid, ae.siteid, count(*) ae_count
            FROM (SELECT comprehendid, studyid, siteid, aeseq,
                        (date_trunc('month', aestdtc) + interval '1 month' - interval '1 day')::date AS month_end
                    FROM ae) ae
            JOIN earliest_study_date es ON (ae.studyid = es.studyid)
            JOIN subject_dates sd ON (ae.comprehendid = sd.comprehendid)
            WHERE ae.month_end >= greatest(sd.start_date, es.min_dsstdtc) 
            AND ae.month_end <= now()::date
            GROUP BY 1, 2) ae ON (site.studyid = ae.studyid AND site.siteid = ae.siteid)
LEFT JOIN (SELECT studyid, siteid, expecteddate
            FROM sitemilestone
            WHERE milestonelabel = 'FIRST SUBJECT IN' AND lower(milestonetype) = 'actual') fsi_actual ON (site.studyid = fsi_actual.studyid AND site.siteid = fsi_actual.siteid)
LEFT JOIN (SELECT studyid, siteid, count(*) enr_count
            FROM rpt_subject_disposition
            WHERE dsevent = 'ENROLLED'
            GROUP BY 1, 2) enr ON (site.studyid = enr.studyid AND site.siteid = enr.siteid);

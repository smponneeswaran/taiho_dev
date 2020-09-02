/*
rpt_subject_visit_schedule PLO

NOTE:

This will change behavior based ON the existence of a flag in the comprehendcodelist
codename = 'VISIT_COMPLIANCE_CALCULATE_ON_PREVIOUS_VISIT'
codekey = studyid

If this flag exists the acceptable windows for visits will rebaseline based on the start of the previous visit
If this flag is not present all visit window are computed off the initial first visit.

Notes:  The Subject Visit Schedule PLO lists all the subjects along with the scheduled visits (per TV settings) with an indication
          of where those visits fall in terms of subject visit compliance e.g. MISSED, IN WINDOW, OUT OF WINDOW 
     
Revision History: 16-Jun-2016 Palaniraja Dhanavelu - Updated the join condition in main query to use the field visit instead of visitnum since visitnum may not be unique.
                  21-Jun-2016 Adam Kaus - Fixed bug where join to site table did not include studyid causing duplicate records
                  31-Aug-2016 Adam Kaus - Disabling "raise notice" statements for quiet execution
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  12-Sep-2016 Palaniraja Dhanavelu - Updated the DO block to consider the subject exit date for setting the expected visit flag
                  10-Oct-2016 Michelle Engler - Updated as follows:
                                                1. The minimum sv visit date is taken from the minimum collected scheduled visit (based on visitnum ordering)
                                                2. If the minimum sv visit is not collected, then that subject is not included in the subject compliance calculations
                  16-Oct-2016 Michelle Engler - Set comparison of expected for subjects that exited study to use windowclosed date instead of window open date
                  18-Oct-2016 Adam Kaus - Reverted named function to anonymous to avoid adapter issues
                  25-Oct-2016 Palaniraja Dhanavelu - Updated the logic for setting the expected flag as follows:
                                                1. Expected = True - if the window has been closed and the window closed date of this visit is less than exit date
                                                2. Expected = False - if the window is not closed and the visit date is not collected due to the bad data or due to the visit is marked as Missed.
                  23-Jan-2017 Michelle Engler - Add comments for tech writer
                  21-Apr-2017 Michelle Engler - From this point forward, the revision history will be exclusively maintained in github

*/

CREATE TABLE rpt_subject_visit_schedule AS
-- CTE to default the 'Visit Compliance' to "hide first" when there is no matching entry is present in comprehendcodelist table for specific study 
--   Hide First: Use the first visit to determine if the visit schedule has started for a given subject and to 
--               get the visit start date for purposes of determining visit compliance AND do not include that
--               first visit as a record in the PLO
--   Show First: Use the first visit to determine if the visit schedule has started for a given subject and to 
--               get the visit start date for purposes of determining visit compliance AND include that first 
--               visit record in the PLO
WITH dflt_comprehendcode AS (SELECT 'VISIT_COMPLIANCE'::text AS codename,
                                    'hide first'::text AS codevalue
                            UNION ALL
                            SELECT 'VISIT_COMPLIANCE_CALCULATE_ON_PREVIOUS_VISIT'::text AS codename,
                                    'false'::text AS codevalue),

    -- CTE to handle the functionality of fetching study specific code values with the following scenarios
    --      1. If studyid is present in  comprehendcodelist table, then the corresponding code value for that studyid will be fetched
    --      2. if studyid is not present in comprehendcodelist table, then codevalue of  codekey 'default' will be assigned to the studyid's missing in comprehendcodelist 
    --      3. if both studyid and 'default' code key is not present in comprehendcodelist table, then the default codevalue ('hide first' for 'VISIT_COMPLIANCE') will be assigned using dflt_comprehendcode CTE
    comprehendcodelist_handler AS (WITH code_ranker AS (SELECT DISTINCT s.studyid, c.codename, c.codekey, c.codevalue, 1::int rank 
                                                        FROM  study s
                                                        JOIN comprehendcodelist c ON (s.studyid = c.codekey)
                                                        UNION ALL
                                                        SELECT DISTINCT s.studyid, c.codename, c.codekey, c.codevalue, 2::int rank 
                                                        FROM  study s
                                                        JOIN comprehendcodelist c ON (LOWER(c.codekey) = 'default')
                                                        UNION ALL
                                                        SELECT DISTINCT s.studyid, c.codename, null::text codekey, c.codevalue, 3::int rank 
                                                        FROM  study s
                                                        JOIN dflt_comprehendcode c ON (1 = 1))
                                    SELECT c1.studyid, c1.codename, c1.codekey, c1.codevalue
                                    FROM code_ranker c1
                                    JOIN (SELECT studyid, codename, MIN(rank) AS rank
                                            FROM code_ranker
                                            GROUP BY studyid, codename) c2 ON (c1.studyid = c2.studyid AND c1.codename = c2.codename AND c1.rank = c2.rank)),

    -- CTE to list all subjects and scheduled visits for a clinical trial where the subject has a collected first visit date
    subject_visits AS (SELECT subject.comprehendid,
                                subject.studyid,
                                study.studyname,
                                subject.siteid,
                                site.sitename,
                                site.sitecountry,
                                site.siteregion,
                                subject.usubjid,
                                subject.exitdate,
                                tv.visit,
                                tv.visitnum,
                                tv.visitdy,
                                tv.visitwindowbefore,
                                tv.visitwindowafter,
                                /*
                                first_visit
                                find the sv visit date for the tv where tv.visitdy = 0, we treat this as the start visit
                                the data doesn't always line up so we need to ensure there is a tv.visitnum = 0 otherwise
                                none of the visit window reporting will work
                                */
                                first_visit.first_visit
                        FROM subject
                        JOIN tv ON (subject.studyid = tv.studyid)
                        JOIN site ON (site.siteid = subject.siteid AND site.studyid = subject.studyid)
                        JOIN study ON (study.studyid = site.studyid)
                        JOIN (SELECT sv.studyid,
                                        sv.comprehendid,
                                        MIN(sv.svstdtc) AS first_visit
                                FROM sv 
                                JOIN (SELECT studyid, visitnum AS visitnum FROM tv WHERE tv.visitdy = 0 /*First visit day visit only*/) tv ON (sv.studyid = tv.studyid AND sv.visitnum = tv.visitnum )
                                WHERE sv.svstdtc IS NOT NULL /*Only completed SV records that have the first visit completed */
                                GROUP BY sv.studyid, sv.comprehendid) first_visit ON (subject.comprehendid = first_visit.comprehendid)
                        JOIN comprehendcodelist_handler clh ON (study.studyid = clh.studyid AND codename = 'VISIT_COMPLIANCE')
                        WHERE /* Excludes unscheduled visits */
                                tv.visitdy < 99999  AND
                                /* Only includes first visit if indicated to be included per the visit compliance configuration setting */
                                ((LOWER(clh.codevalue) = 'hide first' AND tv.visitdy != 0) OR
                                 (LOWER(clh.codevalue) = 'show first'))),

    -- CTE to get all the actual visit data for scheduled visits 
    sv_mod AS (SELECT sv.comprehendid,
                        sv.studyid,
                        sv.siteid,
                        sv.usubjid,
                        sv.visitnum,
                        sv.visit,
                        MIN(sv.svstdtc) visit_start_dtc,
                        MIN(sv.svendtc) visit_end_dtc
                FROM sv 
                JOIN tv ON (sv.studyid = tv.studyid AND sv.visit = tv.visit)
                JOIN comprehendcodelist_handler clh ON (sv.studyid = clh.studyid AND codename = 'VISIT_COMPLIANCE')
                WHERE tv.visitdy < 99999 AND
                        sv.svstdtc IS NOT NULL AND
                        ((LOWER(clh.codevalue) = 'hide first' AND tv.visitdy != 0) OR
                         (LOWER(clh.codevalue) = 'show first'))
                GROUP BY sv.comprehendid,
                            sv.studyid,
                            sv.siteid,
                            sv.usubjid,
                            sv.visitnum,
                            sv.visit),

    -- note the next set of CTEs slowly stack lag and conditional based on flag logic
    -- having them sequentially add the logic was easier to read/reason about
    -- than simply packing the window functions and the conditionals together
    -- this runs front to back on RGEN in about 60seconds, no perf impact
    -- we need to grab previous visit_start_dtc and previous visit visitdy
    with_lag_data AS (SELECT DISTINCT subject_visits.comprehendid,
                            subject_visits.studyid,
                            subject_visits.studyname,
                            subject_visits.siteid,
                            subject_visits.sitename,
                            subject_visits.sitecountry,
                            subject_visits.siteregion,
                            subject_visits.usubjid,
                            subject_visits.exitdate,
                            subject_visits.visit,
                            subject_visits.visitnum,
                            subject_visits.first_visit,
                            subject_visits.visitdy,
                            sv_latest_visit.visitnum visitnum_sv_latest,
                            CASE
                                -- If the previous visit has a visit date, then we use that record to identify the previous visit date/visit day (and ultimately adjust the visit window).
                                -- However, if the previous visit was missed then we go back to the original approach of using the "first visit" to calculate the visit window. In this case:
                                --      * previous_visit_start_dtc = first_visit
                                --      * previous_visitdy = 0 (the default for the first visit)
                                WHEN LAG(sv_mod.visit_start_dtc) OVER (PARTITION BY subject_visits.comprehendid ORDER BY subject_visits.visitnum, subject_visits.visitdy, sv_mod.visit_start_dtc) IS NULL
                                THEN 0
                                ELSE COALESCE(LAG(subject_visits.visitdy) OVER (PARTITION BY subject_visits.comprehendid ORDER BY subject_visits.visitnum, subject_visits.visitdy, sv_mod.visit_start_dtc), 1) END previous_visitdy,
                            COALESCE(LAG(sv_mod.visit_start_dtc) OVER (PARTITION BY subject_visits.comprehendid ORDER BY subject_visits.visitnum, subject_visits.visitdy, sv_mod.visit_start_dtc), subject_visits.first_visit) previous_visit_start_dtc,
                            subject_visits.visitwindowbefore,
                            subject_visits.visitwindowafter,
                            sv_mod.visitnum svmod_visitnum,
                            sv_mod.visit svmod_visit,
                            sv_mod.visit_start_dtc,
                            sv_mod.visit_end_dtc,
                            1::integer AS visitseq
                        FROM subject_visits
                        LEFT JOIN sv_mod ON (sv_mod.comprehendid = subject_visits.comprehendid AND sv_mod.visit = subject_visits.visit)
                        LEFT JOIN (--get the latest visit per subject by visitnum
                                    SELECT comprehendid,
                                            MAX(visitnum) AS visitnum
                                    FROM sv_mod
                                    GROUP BY comprehendid) sv_latest_visit ON (subject_visits.comprehendid = sv_latest_visit.comprehendid)
                        WHERE subject_visits.first_visit IS NOT NULL),

    --
    -- this cte selects the appropriate visitdy calculation based on flag in comprehendcodelist
    -- visitdy_rebase simply provides an offset onto the visitdy based on previous visit
    conditional_main AS (SELECT with_lag_data.comprehendid,
                                with_lag_data.studyid,
                                with_lag_data.studyname,
                                with_lag_data.siteid,
                                with_lag_data.sitename,
                                with_lag_data.sitecountry,
                                with_lag_data.siteregion,
                                with_lag_data.usubjid,
                                with_lag_data.exitdate,
                                with_lag_data.visit,
                                with_lag_data.visitnum,
                                with_lag_data.first_visit,
                                with_lag_data.visitdy,
                                with_lag_data.visitnum_sv_latest,
                                with_lag_data.previous_visitdy,
                                with_lag_data.previous_visit_start_dtc,
                                with_lag_data.visitwindowbefore,
                                with_lag_data.visitwindowafter,
                                with_lag_data.svmod_visitnum,
                                with_lag_data.svmod_visit,
                                with_lag_data.visit_start_dtc,
                                with_lag_data.visit_end_dtc,
                                with_lag_data.visitseq,
                                rebase.visitdy_rebase,
                                CASE 
                                    -- Apply the visitdy_rebase to the visitdy to make this visit window dynamic as applicable (according to the config)
                                    WHEN LOWER(cl.codevalue) = 'true' THEN with_lag_data.visitdy + rebase.visitdy_rebase
                                    ELSE with_lag_data.visitdy END visitdy_derived
                            FROM with_lag_data
                            JOIN (SELECT comprehendid, visit,
                                    CASE 
                                        -- Either the previous visit was missed or this is the second visit. Regardless, no need to offset the visitdy here - use the original.
                                        WHEN previous_visit_start_dtc = first_visit THEN 0
                                        -- First, get the day on which the prev. visit actually occurred, then adjust by the day on which it was planned to occur to get the offset.
                                        ELSE  DATE_PART('day', previous_visit_start_dtc::timestamp - first_visit::timestamp) - previous_visitdy END visitdy_rebase
                                    FROM with_lag_data) rebase ON (with_lag_data.comprehendid = rebase.comprehendid AND with_lag_data.visit = rebase.visit)
                            LEFT JOIN comprehendcodelist_handler cl ON (with_lag_data.studyid = cl.studyid AND cl.codename = 'VISIT_COMPLIANCE_CALCULATE_ON_PREVIOUS_VISIT'))

--
-- main query
SELECT comprehendid::text AS comprehendid,
        studyid::text AS studyid,
        studyname::text AS studyname,
        siteid::text AS siteid,
        sitename::text AS sitename,
        sitecountry::text AS sitecountry,
        siteregion::text AS siteregion,
        usubjid::text AS usubjid,
        exitdate::date AS exitdate,
        visit::text AS visit,
        visitnum::numeric AS visitnum,
        first_visit::date AS first_visit,
        visitdy::int AS visitdy,
        previous_visitdy::int AS previous_visitdy,
        previous_visit_start_dtc::date AS previous_visit_start_dtc,
        visitwindowbefore::int AS visitwindowbefore,
        visitwindowafter::int AS visitwindowafter,
        svmod_visitnum::numeric AS svmod_visitnum,
        svmod_visit::text AS svmod_visit,
        visit_start_dtc::date AS visit_start_dtc,
        visit_end_dtc::date AS visit_end_dtc,
        visitseq::int AS visitseq,
        visitdy_rebase::int AS visitdy_rebase,
        visitdy_derived::int AS visitdy_derived,
        /* Expected visit date is the first visit date + the visit day) */
        (first_visit::date + visitdy_derived::integer)::date AS expectedvisitdate,
        /* Window before is the first visit date + visit day minus the visit window before days  */
        (first_visit::date + visitdy_derived::integer - visitwindowbefore::integer)::date AS windowopen,
        /* Window after is the first visit date + visit day plus the visit window after days */
        (first_visit::date + visitdy_derived::integer + visitwindowafter::integer)::date AS windowclosed,
        /* 
        expected = true will be used by the reports to only use counts when expected
        expected means either the visit was collected or it is uncollected but is expected based on other visits AND/or the visit window 
        */ 
        (CASE WHEN visit_start_dtc IS NOT NULL THEN TRUE -- always expected if visit collected
             WHEN visit_start_dtc IS NULL AND visitnum < visitnum_sv_latest AND (first_visit::date + visitdy_derived::integer + visitwindowafter::integer)::date < COALESCE(exitdate, TO_DATE('3000000','J')) THEN TRUE -- expected if visit uncollected, a later visit was collected, and visit window closed prior to the exit date
             WHEN visit_start_dtc IS NULL AND (first_visit::date + visitdy_derived::integer + visitwindowafter::integer)::date < exitdate THEN TRUE -- expected if visit uncollected and visit window closed prior to exit date
             WHEN visit_start_dtc IS NULL AND now()::date <= COALESCE(exitdate, TO_DATE('3000000','J')) AND (first_visit::date + visitdy_derived::integer + visitwindowafter::integer)::date <= now()::date THEN TRUE -- expected if visit uncollected and visit window closed on or before current date if subject not already exited
             ELSE false END)::bool AS expected,
        -- leaving this days calculation in for easy verification
        DATE_PART('day', visit_start_dtc::timestamp - first_visit::timestamp)::double precision AS visit_days,
        /* 
        category {"IN WINDOW", "OUT OF WINDOW", "MISSED"} 
        we have the relative days the visit should occur at (visitdy) plus how many days
        before and after to be considered in window (visitwindowbefore/after)
        given the first_visit field (computed from tv where visitdy = 0) we can check
        against that window
        */
        (CASE WHEN visit_start_dtc IS NULL THEN 'MISSED' 
             WHEN (DATE_PART('day', visit_start_dtc::timestamp - first_visit::timestamp) <= visitdy_derived + visitwindowafter 
                AND DATE_PART('day', visit_start_dtc::timestamp - first_visit::timestamp) >= visitdy_derived - visitwindowbefore) 
                THEN 'INWINDOW' ELSE 'OUTWINDOW' END)::text AS category,

        /* We add Schedule Compliance value {"On Schedule", "Out of Window", "Missed"} for Subject Visit - Listing filter */
        (CASE WHEN visit_start_dtc IS NULL THEN 'Missed'
             WHEN (DATE_PART('day', visit_start_dtc::timestamp - first_visit::timestamp) <= visitdy_derived + visitwindowafter
                AND DATE_PART('day', visit_start_dtc::timestamp - first_visit::timestamp) >= visitdy_derived - visitwindowbefore)
                THEN 'On Schedule' ELSE 'Out of Window' END)::text AS schedule_compliance,
        DATE_TRUNC('month', visit_start_dtc)::date trunc_month,
        now()::timestamp AS comprehend_update_time
FROM conditional_main;

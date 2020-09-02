/*
rpt_subject_visit_schedule_last_visit PLO

Notes:  PLO having details of visit schedule for each subject only for the last visit of the subject

        Visit Compliance requires:
        1. The SV table to be mapped and the visit sv.field values to reference the tv.visit field values
        2. Populated sv.svstdtc values for determining visit compliance
        3. The TV table to be set with one record per study that has visitdy = 0
        . The TV table to be set with additional visits with visitdy > 0 or visitdy < 0. (Note that tv.visitdy = 99999 are unscheduled visits and are excluded from this PLO)
     
Revision History: 16-Jun-2016 Palaniraja Dhanavelu - Initial Version
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  12-Oct-2016 Michelle Engler - Change first visit logic
                  20-Oct-2016 Palaniraja Dhanavelu - Updated logic to fetch only the last visit for each subject
                  09-Dec-2016 Adam Kaus - Removed dependecy on subject first visit (TP 19432)
                  20-Jan-2017 Michelle Engler - Add comments for tech writer

TODO: This PLO is very similar to the rpt_subject_visit_schedule PLO with the main differneces being:
1. This PLO only returns the latest visit per subject 
2. This PLO is not dependent on the presence of a first visit where visitdy=0 
The net effect of #2 could be subjects in this PLO which do not exist in rpt_subject_visit_schedule. This may need to be reevaluated from a product perspective.
*/

CREATE TABLE rpt_subject_visit_schedule_last_visit AS
-- CTE to default the 'Visist Compliance' to "hide first" when there is no matching entry is present in comprehendcodelist table for specific study 
--   Hide First: Use the first visit to determine if the visit schedule has started for a given subject and to 
--               get the visit start date for purposes of determining visit compliance AND do not include that
--               first visit as a record in the PLO
--   Show First: Use the first visit to determine if the visit schedule has started for a given subject and to 
--               get the visit start date for purposes of determining visit compliance AND include that first 
--               visit record in the PLO
WITH dflt_comprehendcode AS (SELECT 'VISIT_COMPLIANCE'::text AS codename,
                                    'hide first'::text AS codevalue),

-- CTE to handle the functionality of fetching study specific code values with the following scenarios
--      1. If studyid is present in  comprehendcodelist table, then the corresponding code value for that studyid will be fetched
--      2. if studyid is not present in comprehendcodelist table, then codevalue of  codekey 'default' will be assigned to the studyid's missing in comprehendcodelist 
--      3. if both studyid and 'default' code key is not present in comprehendcodelist table, then the default codevalue ('hide first' for 'VISIT_COMPLIANCE') will be assigned using dflt_comprehendcode CTE
comprehendcodelist_handler AS (SELECT s.studyid, COALESCE(cl1.codename,cl2.codename, dflt.codename) AS codename , COALESCE(cl1.codekey,cl2.codekey) AS codekey, COALESCE(COALESCE(cl1.codevalue,cl2.codevalue), dflt.codevalue)  AS codevalue
                                FROM study s
                                LEFT JOIN comprehendcodelist cl1 ON (cl1.codename = 'VISIT_COMPLIANCE' AND s.studyid = cl1.codekey)
                                LEFT JOIN comprehendcodelist cl2 ON (cl2.codename = 'VISIT_COMPLIANCE' AND cl2.codekey = 'default')
                                LEFT JOIN dflt_comprehendcode dflt ON ( dflt.codename = 'VISIT_COMPLIANCE') ),-- condition to fetch the code value from dflt_comprehendcode  when both study and default condition are missing in comprehendcodelist
                                                       

-- CTE to determine the first scheduled visit for a subject and get the first visit date (which will be the start date
--   for the visit compliance calculations for that subject). If a subject has the first visit completed with no visit date
--   populated, this will be treated the same as if the visit did not occur for the subject.  In other words, the first visit date
--   is required for the subject visit compliance to be calculated.
first_visit AS (
        SELECT
               sv.studyid,
               sv.comprehendid,
               sv.usubjid,
               MIN(sv.svstdtc) AS first_visit
        FROM sv 
        /* The First visit is determined by the record in TV for the study that has visitdy = 0. It is expected that only
             one record per study will have visitdy=0 */
        JOIN (SELECT studyid, visitnum AS visitnum FROM tv WHERE tv.visitdy = 0 /*First visit day visit only*/) tv ON (sv.studyid = tv.studyid AND sv.visitnum = tv.visitnum )
        WHERE sv.svstdtc is not null /*Only completed SV records that have the first visit completed */
        GROUP BY sv.studyid,
                sv.comprehendid,
                sv.usubjid),

-- CTE subject_visits brings the list of subjects together with the TV schedule information and the first visit date from sv, if it exist 
subject_visits AS (
        SELECT
                subject.comprehendid,
                subject.studyid,
                study.studyname,
                subject.siteid,
                site.sitename,
                site.sitecountry,
                site.siteregion,
                subject.usubjid,
                tv.visit,
                tv.visitnum,
                tv.visitdy,
                tv.visitwindowbefore,
                tv.visitwindowafter,
                sv.first_visit
        FROM
                subject
                JOIN tv ON subject.studyid = tv.studyid /* TV must have the visits for this study that are greater than or equal to 0 and not unscheduled (99999) */
                JOIN site ON site.siteid = subject.siteid AND site.studyid = subject.studyid
                JOIN study ON study.studyid = site.studyid
                LEFT JOIN first_visit sv ON (sv.comprehendid = subject.comprehendid) /*Left Join means that the subjects will all be listed with or without the sv first visit information */
                JOIN comprehendcodelist_handler clh ON (subject.studyid = clh.studyid)                                                                         

        WHERE
                /* Excludes unscheduled visits */
                tv.visitdy < 99999  AND
                /* Only includes first visit if indicated to be included per the visit compliance configuration setting */
                ( (LOWER(clh.codevalue) = 'hide first' AND tv.visitdy != 0) OR
                  (LOWER(clh.codevalue) = 'show first'))),

sv_last_visit as ( --To select the max visit for each subject present in th SV table fetch only the latest visit for each subject
                select sv.comprehendid, 
                        sv.studyid, 
                        sv.siteid, 
                        sv.usubjid, 
                        MAX(sv.visitnum) visitnum 
                FROM sv 
                JOIN tv ON (sv.studyid = tv.studyid AND sv.visit = tv.visit) WHERE tv.visitdy != 99999
                GROUP BY  sv.comprehendid, sv.studyid, sv.siteid, sv.usubjid),

-- CTE sv_mod pulls in all DV data for scheduled visits and either shows or hide the first visit from the result
--  set based on configuration
sv_mod AS ( 
        SELECT
                sv.comprehendid,
                sv.studyid,
                sv.siteid,
                sv.usubjid,
                sv.visitnum,
                sv.visit,
                /* With the sv.visitseq being always set to 1, these min/max aggregations are 
                    legacy in order to deal with multiple visitseq's (which no longer exist) */
                MIN(sv.svstdtc) visit_start_dtc,
                MIN(sv.svendtc) visit_end_dtc
        FROM
                sv 
                JOIN tv ON (sv.studyid = tv.studyid AND sv.visit = tv.visit)
                JOIN sv_last_visit sv1 ON (sv.comprehendid = sv1.comprehendid AND sv.visitnum = sv1.visitnum)
                JOIN comprehendcodelist_handler clh ON (sv.studyid = clh.studyid)
        WHERE
                /* Excludes unscheduled visits as indicated by tv.visitdy = 99999 */
                tv.visitdy < 99999 AND 
                /* Logic to show or hide the first visit based on configuration */
                ( (LOWER(clh.codevalue) = 'hide first' and tv.visitdy != 0) OR
                  (LOWER(clh.codevalue) = 'show first'))
        GROUP BY
                sv.comprehendid,
                sv.studyid,
                sv.siteid,
                sv.usubjid,
                sv.visitnum,
                sv.visit)

--
-- start main query
SELECT DISTINCT
        subject_visits.comprehendid::text AS comprehendid,
        subject_visits.studyid::text AS studyid,
        subject_visits.studyname::text AS studyname,
        subject_visits.siteid::text AS siteid,
        subject_visits.sitename::text AS sitename,
        subject_visits.sitecountry::text AS sitecountry,
        subject_visits.siteregion::text AS siteregion,
        subject_visits.usubjid::text AS usubjid,
        subject_visits.visit::text AS visit,
        subject_visits.visitnum::numeric AS visitnum,
        subject_visits.visitdy::int AS visitdy,
        subject_visits.visitwindowbefore::int AS visitwindowbefore,
        subject_visits.visitwindowafter::int AS visitwindowafter,
        subject_visits.first_visit::date AS first_visit,
        /* Window is opened as of the first visit + visit day - the visit window before */
        /* Attempt to get the window open/closed from rpt_subject_visit_schedule where they are recalculated based on the VISIT_COMPLIANCE_CALCULATE_ON_PREVIOUS_VISIT config.  
            If this visit is NOT found in that PLO, the only reason would be because first_visit is null in which case this calculation will return null anyway*/
        COALESCE(rsvs.windowopen, (subject_visits.first_visit + ((subject_visits.visitdy - subject_visits.visitwindowbefore) * interval '1 days')))::date  AS windowopen,
        /* Window is closed as of the first visit + visit day + the visit window after */
        COALESCE(rsvs.windowclosed, (subject_visits.first_visit + ((subject_visits.visitdy + subject_visits.visitwindowafter) * interval '1 days')))::date AS windowclosed,
        sv_mod.visitnum::numeric AS svmod_visitnum,
        sv_mod.visit::text AS svmod_visit,
        sv_mod.visit_start_dtc::date AS visit_start_dtc,
        sv_mod.visit_end_dtc::date AS visit_end_dtc,
        1::integer AS visitseq, 
        /* Show the number of visit days that have occurred since the first visit and the current visit */
        DATE_PART('day', sv_mod.visit_start_dtc::timestamp - subject_visits.first_visit::timestamp)::double precision AS visit_days,
        /* Show the visit date as of the first of the month for time series grouping purposes */
        DATE_TRUNC('month', sv_mod.visit_start_dtc)::date AS trunc_month,
        now()::timestamp AS comprehend_update_time
FROM
        subject_visits
        JOIN sv_mod on (sv_mod.comprehendid = subject_visits.comprehendid AND sv_mod.visit = subject_visits.visit)
        -- joining to get the visit windows which are recalculated based on the VISIT_COMPLIANCE_CALCULATE_ON_PREVIOUS_VISIT config
        LEFT JOIN rpt_subject_visit_schedule rsvs ON (subject_visits.comprehendid = rsvs.comprehendid AND subject_visits.visit = rsvs.visit AND subject_visits.visitnum = rsvs.visitnum);

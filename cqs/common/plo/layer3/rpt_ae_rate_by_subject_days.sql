/*
rpt_ae_rate_by_subject_days PLO

Notes: AE Rate by Subject Days PLO brings the Adverse Events together with subject days for the purposes of calculating AE Rate in the app

Revision History: 21-Jun-2016 Adam Kaus - Fixed bug where join to site table did not include studyid causing duplicate records
                  27-Jun-2016 An Nguyen - Fixed bug study_total_ae_count_this_category not per category
                  31-Aug-2016 Adam Kaus - Updated where clauses to incldue dsterm for enrollment
                  02-Sep-2016 Michelle Engler - Updated to make subject days count to start from configurable enroll/consent date
                  09-Sep-2016 Michelle Engler - Fix ambiguous column error
                  12-Sep-2016 Michelle Engler - Add that cols could be null, blank, or Unknown
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  20-Sep-2016 Palaniraja Dhanavelu - Adding table reference in front of columns to accomodate sqldep requirements
                  12-Oct-2016 Adam Kaus - Added subject_days_weight_ratio column (moved calculation from frontend to PLO)
                  09-Nov-2016 Adam Kaus - Added logic to handle missing ds dates. If start date or exit date is missing then subject days is null
                  14-Nov-2016 Michelle Engler - Removed 5.1 per 18829
                  17-Nov-2016 Michelle Engler - Category catch with else incomplete data added and added greater flexibility in determining related/serious status
		              09-Jan-2017 Michelle Engler - Adding additional error condition handling for subject days to force consistency
                  09-Jan-2016 Palani Raja Dhanavelu - updated logic for study_total_subject_days field in study_subject_days CTE to make sure it has +1 day when enddate - startdate = 0 as per tp 20733
                  14-Jan-2017 Michelle Engler - Add comments for tech writer
                  14-Mar-2017 Adam Kaus - Revision history will be maintained via git history from this point forward
*/

create table rpt_ae_rate_by_subject_days as
with
    -- Get the disposition state per study used to determine the subject start date.
    -- This is configurable per study within comprehendcodelist and populated in rpt_subject_days
    study_start_disposition AS (SELECT DISTINCT studyid, subjectstartdisposition AS startdisposition
                                FROM rpt_subject_days),

-- AE Data selects the Adverse Events and puts them into categories of non serious/related, not serious/unrelated, serious/related, and serious/unrelated
--  It also sets up a time series that will list the the adverse events by month according to the time series.
--  Note that AEs will be included if they have an AE Start Date and that AE Start Date happens on or after the 1st of the month for the subject start disposition event
--  and the AE Start Date is not greater than the current date
     ae_data as
      (select
        sd.comprehendid,
        ae.aestdtc,
        ae.aeseq,
        ae.aeterm,
        ae.objectuniquekey,
        ae.aeser,
        ae.aerelnst,
        s.sitecountry,
        s.siteid,
        s.studyid,
        -- ae categories for report
        case
                when aestdtc is null or
                     nullif(aeser,'') is null or
                     aeser = 'Unknown' or
                     aeser = 'blank' or
                     nullif(aerelnst,'') is null or
                     aerelnst = 'Unknown' or
                     aerelnst = 'blank'
                        then 'incomplete data'
                when lower(aeser) in ('non-serious','no') and lower(aerelnst) in ('yes','possibly related','definitely related','probably related') then 'not serious/related'
                when lower(aeser) in ('non-serious','no') and lower(aerelnst) in ('no','unrelated') then 'not serious/unrelated'
                when lower(aeser) in ('serious','yes') and lower(aerelnst) in ('yes','possibly related','definitely related','probably related') then 'serious/related'
                when lower(aeser) in ('serious','yes') and lower(aerelnst) in ('no','unrelated') then 'serious/unrelated'
    else 'incomplete data'
        end category,
        sd.period_starting_date as month_trunc,
        sd.subject_day_this_month,
        sd.subject_start_date,
        sd.exit_date
from
    ae
    right join (
        WITH earliest_date as (
            SELECT studyid, min(dsstdtc)::date as min_dsstdtc, max(dsstdtc)::date as max_dsstdtc from rpt_subject_disposition group by studyid
        ),

        subject_dates as (
            SELECT DISTINCT comprehendid, studyid, siteid, usubjid,
                            subjectdaystartdt AS subject_start_date,
                            exitdate AS exit_date
            FROM rpt_subject_days),

        -- Generate a monthly date series per subject
        -- The start date is the first of the month for the subject start date. If the subject start date is missing
        -- then the start date is the first of the month of the earliest disposition date for the study.
        -- The end date is the last day of the current month
        date_series AS (
            SELECT comprehendid,
                    studyid,
                    siteid,
                    usubjid,
                    subject_start_date,
                    exit_date,
                    period_starting_date,
                    (period_starting_date + interval '1 month' - interval '1 day')::date AS period_ending_date
            FROM (SELECT sd.comprehendid,
                            sd.studyid,
                            sd.siteid,
                            sd.usubjid,
                            sd.subject_start_date,
                            sd.exit_date,
                            generate_series( date_trunc( 'month', greatest(coalesce(sd.subject_start_date, ed.min_dsstdtc)::date, '1/1/1970'::date) )::date,
                                            (date_trunc( 'month', now() )::date + interval '1 month' - interval '1 day')::date
                                            , interval '1 month' )::date AS period_starting_date
                    FROM subject_dates sd
                    JOIN earliest_date ed ON (sd.studyid = ed.studyid) ) a )

        -- Get the subject days per month. If no record exists in rpt_subject_days for the subject/month
        -- then default to 0 subject days
        SELECT ser.comprehendid, ser.studyid, ser.siteid, ser.usubjid, ser.subject_start_date, ser.exit_date, ser.period_starting_date, ser.period_ending_date,
                 CASE WHEN sd.comprehendid IS NULL THEN 0 ELSE sd.thismonthsubjectdays END AS subject_day_this_month
        FROM date_series ser
        LEFT JOIN rpt_subject_days sd ON (sd.comprehendid = ser.comprehendid AND sd.thismonth = ser.period_starting_date ) ) sd

    on (sd.comprehendid = ae.comprehendid and date_trunc('month', aestdtc)::date = sd.period_starting_date)
    join site s on s.siteid = sd.siteid and s.studyid = sd.studyid
),

-- Counts AEs by month where the AE Start Date is not null
ae_counts as
(
select comprehendid,
       date_trunc('month', aestdtc)::date as month_trunc, count(*) as ae_count
from ae
where aestdtc is not null
group by 1, 2
),

-- Counts Study Subjects that have started the study
study_subjects as
(select
        ds.studyid,
        count(1) study_subjects
from
        rpt_subject_disposition ds
where dsterm = study_start_disposition
group by
        ds.studyid),

-- Counts AEs for a study by category
study_ae_totals as
(select
        count(1) study_total_ae_count_this_category,
        category,
        studyid
from
        ae_data
where
        aeseq is not null
group by
        studyid,
        category),

study_subject_days as
(select studyid,
        SUM(thismonthsubjectdays) as study_total_subject_days
from rpt_subject_days
group by studyid),

-- If a subject has more than one AE in a given month, the count of the AEs is included in the subject day weight ratio and
--  is used to ensure that we are only counting the subject days for that given month one time e.g. If a subject has 5 AEs in June 2017,
--  the subject days will be listed into 5 different records (one for each AE) and the subject days weight ratio will be 5 - hence, the
--  consideration of subject days will add the subject days for the 5 records together and then divide those by 5 to ensure we are only considering
--  them once for each subject month
subject_days_weight_ratio as
(select ae_data.studyid,
        ae_data.siteid,
        ae_data.sitecountry,
        sum( ae_data.subject_day_this_month::numeric / coalesce(ae_counts.ae_count, 1)::numeric ) as sd_weight_ratio
from ae_data
left join ae_counts on (ae_data.comprehendid = ae_counts.comprehendid and ae_data.month_trunc = ae_counts.month_trunc)
group by ae_data.studyid,
         ae_data.siteid,
         ae_data.sitecountry)

-- main query
select
        ae_data.comprehendid,
        ae_data.aestdtc,
        ae_data.aeseq,
        ae_data.aeterm,
        -- In order to represent months without any AEs as well as have the edging work correctly in CQL, this random objectuniquekey is needed.
        coalesce(ae_data.objectuniquekey, random()::text || random()::text || random()::text) as objectuniquekey,
        ae_data.aeser,
        ae_data.aerelnst,
        ae_data.studyid,
        ae_data.sitecountry,
        ae_data.siteid,
        ae_data.category,
        ae_data.month_trunc,
        ae_data.subject_day_this_month,
        coalesce(ae_counts.ae_count, 1) as weight,
        coalesce(study_subjects.study_subjects, 0) as study_subjects, 
        coalesce(study_subject_days.study_total_subject_days, 0) as study_total_subject_days_this_category,
        coalesce(study_ae_totals.study_total_ae_count_this_category, 0) as study_total_ae_count_this_category,
        clh.startdisposition as subject_day_start,
        sdwr.sd_weight_ratio as subject_days_weight_ratio,
        now()::timestamp as comprehend_update_time
from
        ae_data
        left join ae_counts on (ae_data.comprehendid = ae_counts.comprehendid and ae_data.month_trunc = ae_counts.month_trunc)
        left join study_subjects on ae_data.studyid = study_subjects.studyid
        left join study_ae_totals on study_ae_totals.studyid = ae_data.studyid and study_ae_totals.category = ae_data.category
        join study_subject_days on study_subject_days.studyid = ae_data.studyid
        join study_start_disposition clh on (ae_data.studyid = clh.studyid)
        left join subject_days_weight_ratio sdwr on (ae_data.studyid = sdwr.studyid and ae_data.siteid = sdwr.siteid and coalesce(ae_data.sitecountry, 'x') = coalesce(sdwr.sitecountry, 'x'));

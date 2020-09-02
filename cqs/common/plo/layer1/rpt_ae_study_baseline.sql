/*
rpt_ae_study_baseline PLO

Notes: AE Study Baseline PLO lists the AE counts against durations of the Adverse Events
     
Revision History: 17-Aug-2016 Adam Kaus - Added last_refresh_ts column for use in test scripts
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Timeand removed last_refresh_ts
                  05-Oct-2016 Adam Kaus - update join so null aeterm records are included 
                  11-Oct-2016 Michelle Engler - Added 1 to the durations to include the current day + verified records with null aestdtc are not included in the counts
                  13-Oct-2016 Michelle Engler - Set durations as bigint due to exceeding int capacity on client data
                  17-Oct-2016 Michelle Engler - Set durations to use dates only (no times)
                  18-Nov-2016 Adam Kaus - Correction for division by 0 error
                  14-Jan-2017 Michelle Engler - Add comments for tech writer

*/

create table rpt_ae_study_baseline as
with curr_date as (select studyid, now()::date as today from study),

    term_info as (

        with study_term as (
        select
                ae.studyid,
                ae.aeterm,  
                -- Average of AE End Date (no time) if populated or the current date (no time) minus the AE Start Date + 1 days
                avg(coalesce(ae.aeendtc::date, curr_date.today::date)::timestamp - (aestdtc::date)::timestamp + interval '1 day') duration,
                count(ae.comprehendid) term_count
        from
                ae
        join curr_date on (ae.studyid = curr_date.studyid)
        where
                ae.aestdtc is not null
        group by
                ae.studyid,
                ae.aeterm),

        study_total as (
        select
                ae.studyid,
                -- This duration is actually the "average" duration.  The same calculation of aeenddate (no time) or current date (no time) if ae end date
                --      is null minus the ae start date + 1 day is used to figure out the duration at the AE level.  Then the average is 
                --      calculated of all the durations for the study
                avg(coalesce(ae.aeendtc::date, curr_date.today::date)::timestamp - (aestdtc::date)::timestamp + interval '1 day') duration,
                -- Term count is a count of the AEs where the AE Start Date is not null
                count(ae.comprehendid) term_count,
                -- If the number of distinct aeterm is greater than zero, then 
                --  take the total number of AEs (with populated ae start date) divided by
                --  the count of distinct aeterms as the average term count
                (case when count(distinct ae.aeterm) > 0 then
                    count(ae.comprehendid)::decimal / count(distinct ae.aeterm) 
                else 0 end)::numeric as avg_term_count
        from
                ae
        join curr_date on (ae.studyid = curr_date.studyid)
        where
                ae.aestdtc is not null
        group by
                ae.studyid)

        -- main query for term_info
        select  
                study_term.studyid,
                study_term.aeterm aeterm,
                study_term.duration study_avg_duration_this_term,
                study_term.term_count study_count_this_term,
                study_total.duration study_avg_duration_all_term,
                study_total.term_count study_count_all_term,
                study_total.avg_term_count
        from 
                study_term inner join study_total on study_term.studyid = study_total.studyid
) -- end term_info cte

-- main query rpt_ae_study_baseline
select
        ae.comprehendid,
        ae.studyid,
        ae.aeterm,
        -- we want seconds for use by queryEngine
        aeendtc, aestdtc,
        -- Duration is calculated as the difference from the AE End Date (not considering time), if populated, or the current date (no time) 
        --      minus the AE Start Date (not considering time) + 1 day; hence, if the AE starts and stops on the same day, the count for duration
        --      for that AE will be 24 hours. The duration value is calculated using extracting "epoch" from the interval which is a mechanism for 
        --      getting the number of seconds in an interval.
        extract(epoch from coalesce(ae.aeendtc::date, curr_date.today::date)::timestamp - (aestdtc::date)::timestamp + interval '1 day')::bigint duration,
        -- Average duration this term is an interval - epoch is used to get the number of seconds for the interval
        extract(epoch from term_info.study_avg_duration_this_term)::bigint study_avg_duration_this_term,
        term_info.study_count_this_term,
        --Average duration all term is an interval - epoch is used to get the number of seconds for the interval
        extract(epoch from term_info.study_avg_duration_all_term)::bigint study_avg_duration_all_term,
        term_info.study_count_all_term,
        term_info.avg_term_count,
        
        -- this edges into ae table
        ae.objectuniquekey,
        now()::timestamp as comprehend_update_time
from 
        ae 
inner join term_info on (ae.studyid = term_info.studyid and coalesce(ae.aeterm, 'x') = coalesce(term_info.aeterm, 'x'))
join curr_date on (ae.studyid = curr_date.studyid)
where
        ae.aestdtc is not null;


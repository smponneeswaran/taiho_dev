/*
rpt_data_entry PLO

Notes:  The data entry rate PLO calculates the data entry rate (considering the form count and average time to enter forms) 
            grouped on month intervals.  In addition, this PLO calculates that same rate for all subjects and forms in the
            study as well as for just those forms that are flagged as primary end points.
     
Revision History: 04-Aug-2016 Adam Kaus - Performance tuning; Removed study_subjects CTE, reformatted form_trend_data CTE, 
                                          removed order bys and extra joins, replaced unions with union alls
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  20-Sep-2016 Palaniraja Dhanavelu - Adding table reference in front of columns to accomodate sqldep requirements
                  09-Feb-2017 Michelle Engler - From this point forward, revision history will be tracked in github, exclusively.
*/

create table rpt_data_entry as

-- Correct form data CTE was written to work around the visitseq number increments. Since this was written,
--		the visitseq has been deprecated and is expected to always be set as 1; hence, this CTE is the same
--		as selecting all records from formdata (except for those with missing data entry or data collected dates).
with correct_fd as
(select
        formdata.comprehendid, 
        formdata.studyid, 
        formdata.siteid, 
        formdata.usubjid, 
        formdata.formid, 
        formdata.visit, 
        formdata.visitseq, 
        formdata.dataentrydate, 
        formdata.datacollecteddate, 
        formdata.sdvdate, 
        formdata.objectuniquekey
from
        (select
                comprehendid,
                formid,
                visit,
                min(visitseq)::integer minseq
        from
                formdata
        where formdata.datacollecteddate is not null and formdata.dataentrydate is not null
        group by
                comprehendid,
                formid,
                visit) minseq
        inner join formdata on (formdata.comprehendid = minseq.comprehendid
                                and formdata.formid = minseq.formid
                                and formdata.visit = minseq.visit
                                and formdata.visitseq = minseq.minseq)
        where formdata.datacollecteddate is not null and formdata.dataentrydate is not null), -- end correct_fd

entry_avg_count as
(
--  Cross Join all Subject Dates and End Points CTE calculates the average data entry rate
--		by averaging the difference between the data entry date and the data collected date
--		for a study.  In addition, this CTE collects the first month that any data was 
--		collected for the study AND groups the data entry averages by month.
--
--		For the first select, the isprimaryendpoint flag is always set to false as this calculation is to 
--		occur for all forms regardless of endpoint.
--
--		For the second select, the isprimaryendpoint flag is set to true and the same calculations are
--		joined to the formdef table for only looking at primary end point forms.
--
--		The third and forth selects are in place to make records for all subjects across all months for 
--      which there are collected forms. This is done in order to ensure that subjects that do not have
--      collected forms for a given month still have a data entry rate calculated (as 0).  The main
--      query does the logic of reducing the content of the full CTE and setting a coalesce statement
--      on data entry rate to ensure that every comprehendid is account for in the overall calculation
--      of data entry rate
--
--      Forms will be included with the following dependencies:
--          1. The form exists in formdata aka form is collected
--          2. The formid exists in formdef aka form definition is present
with crossjoin_all_subject_dates_endpoints as (
        select
                fd.studyid,
                fd.siteid,
                avg(fd.dataentrydate - fd.datacollecteddate)::numeric data_entry_avg,
                count(1)::integer form_count,
                first_month.first_month,
                date_trunc('month', fd.datacollecteddate)::date trunc_date,
                fd.comprehendid,
                false::boolean isprimaryendpoint 
        from
                correct_fd fd
                inner join 
                (select
                f.comprehendid,
                date_trunc('month', min(datacollecteddate))::date first_month
                from
                        formdata f
                where f.datacollecteddate is not null and f.dataentrydate is not null
                group by
                        f.comprehendid) first_month on first_month.comprehendid = fd.comprehendid
        group by
                fd.studyid,
                fd.siteid,
                first_month.first_month,
                date_trunc('month', fd.datacollecteddate)::date,
                fd.comprehendid
                        
        union all -- now get the same stuff but only counting primary endpoint
   
        select
                fd.studyid,
                fd.siteid,
                avg(fd.dataentrydate - fd.datacollecteddate)::numeric data_entry_avg,
                count(1)::integer form_count,
                first_month.first_month,
                date_trunc('month', fd.datacollecteddate)::date trunc_date,
                fd.comprehendid,
                true::boolean isprimaryendpoint
        from
                correct_fd fd
                inner join formdef fdef on (fd.studyid = fdef.studyid and fd.formid = fdef.formid)
                inner join 
                (select
                f.comprehendid,
                date_trunc('month', min(datacollecteddate))::date first_month
                from
                        formdata f
                where f.datacollecteddate is not null and f.dataentrydate is not null
                group by
                        f.comprehendid) first_month on first_month.comprehendid = fd.comprehendid
                       
        where
                fdef.isprimaryendpoint = true
        group by
                fd.studyid,
                fd.siteid,
                first_month.first_month,
                date_trunc('month', fd.datacollecteddate)::date,
                fd.comprehendid
        
        union all -- now cross join all subjects and visits for false primary endpoint of everything
        
        select
                sub.studyid,
                subject.siteid,
                null::numeric data_entry_avg,
                null::integer form_count,
                null::date first_month,
                sub.trunc_month trunc_date,
                subject.comprehendid,
                false::boolean isprimaryendpoint
        from
                subject, 
                (select
                        studyid,
                        date_trunc('month', datacollecteddate)::date trunc_month
                from
                        formdata
                where formdata.datacollecteddate is not null and formdata.dataentrydate is not null
                group by
                        studyid,
                        date_trunc('month', datacollecteddate)::date) sub
                where sub.studyid = subject.studyid

        union all -- now cross join all subjects and visits for true primary endpoint of everything
        
        select
                sub.studyid,
                subject.siteid,
                null::numeric data_entry_avg,
                null::integer form_count,
                null::date first_month,
                sub.trunc_month trunc_date,
                subject.comprehendid,
                true::boolean isprimaryendpoint
        from
                subject, 
                (select
                        studyid,
                        date_trunc('month', datacollecteddate)::date trunc_month
                from
                        formdata
                where formdata.datacollecteddate is not null and formdata.dataentrydate is not null
                group by
                        studyid,
                        date_trunc('month', datacollecteddate)::date) sub
                where sub.studyid = subject.studyid)
      
-- The coalesce statement along with the group by statement will ensure that all comprehendids
--      are accounted for in the data entry average and collected form_count.  
select
        studyid,
        siteid,
        coalesce(max(data_entry_avg), 0)::numeric data_entry_avg,
        coalesce(max(form_count), 0)::integer form_count,
        max(first_month)::date first_month,
        trunc_date,
        comprehendid,
        isprimaryendpoint
from
        crossjoin_all_subject_dates_endpoints
group by
        studyid,
        siteid,
        trunc_date,
        comprehendid,
        isprimaryendpoint),



-- An overall study average calculation takes the average of dataentrydate - 
--      datacollecteddate across the study
study_total as
(select 
        avg(dataentrydate - datacollecteddate)::numeric study_average,
        studyid
from
        correct_fd
group by studyid),

-- compute monthly and start->current month running averages by comprehend id
form_trend_data as
(select
        e.studyid,
        e.siteid,
        e.comprehendid,
        e.trunc_date trunc_month,  -- collected date month truncated
        e.isprimaryendpoint,
        
        e.data_entry_avg data_entry_avg_month,  -- avg days lag just for this single month
        e.form_count form_count_month,          -- form count for this single month
        
        -- avg days lag from start month to this single month
        (case when r.form_count_cumulative = 0 then 0 else (r.entry_cumulative / r.form_count_cumulative) end) as avg_entry_days_on_interval,
        
        -- count forms from start month to this single month   
        r.form_count_cumulative as form_count_on_interval
from
-- Joins the entry average count CTE to itself and does summation on the form count / data entry average separately for the flags isprimaryendpoint true and isprimaryendpoint false
--  In other words, in order to get the counts across the whole study, the sums of form count and data entry average * form count are done on records with isprimaryendpoint false
--  To get the same calculations for isprimaryendpoint true, the same summations for only those records with isprimaryrecord true are performed
        entry_avg_count e
-- Joining the entry average count cte to a sub-select on the entry cu
join (select comprehendid,
                trunc_date,
                isprimaryendpoint,
                sum(form_count) over (partition by comprehendid, isprimaryendpoint order by trunc_date) as form_count_cumulative,
                sum(data_entry_avg * form_count) over (partition by comprehendid, isprimaryendpoint order by trunc_date) as entry_cumulative
        from entry_avg_count) r  on (e.comprehendid = r.comprehendid and e.isprimaryendpoint = r.isprimaryendpoint and r.trunc_date = e.trunc_date) )
-- The main query brings all the separate counts together to the subject level such that the form count for the month, data entry average for the month, 
--      total study average, average entry days for the month, and form count on interval is pulled together for each subject and done both with
--      isprimaryendpoint true and isprimaryendpoint false
select
        ftd.studyid,
        ftd.siteid,
        ftd.comprehendid,
        ftd.trunc_month, -- collected date month trunc
        ftd.data_entry_avg_month avg_days_in_month_interval,
        form_count_month forms_in_month_interval,
        study_total.study_average,  
        ftd.avg_entry_days_on_interval,  -- average days entry from first month to current month interval (trunc_month above)
        ftd.form_count_on_interval forms_on_interval, -- count of forms on same interval as previous line
        ftd.isprimaryendpoint,
        now()::timestamp as comprehend_update_time
from
        form_trend_data ftd
        left join study_total on (ftd.studyid = study_total.studyid);



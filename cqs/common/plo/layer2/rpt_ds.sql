/*
rpt_ds

Notes: PLO that lists the disposition counts by study by dsseq by month and eliminates dispositions with null dsstdtc values  
     
Revision History: 26-Aug-2016 MDE - New PLO
                  27-Aug-2016 MDE - Add cumulative and make the plo at site level instead of study levels
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
                  21-Sep-2016 DPR - Updating the period date comparision at month and year level to the dsstdtc.
*/

create table rpt_ds as
with ds_min_max as (select studyid, min(dsstdtc) min_dsstdtc, max(dsstdtc) max_dsstdtc from rpt_subject_disposition group by studyid),

my_periods as (
            SELECT period,
            'MONTHLY'::text as period_type,
            to_date(EXTRACT(MONTH FROM intervals.interval_dt::date)::text  || '-'::text || EXTRACT(YEAR FROM intervals.interval_dt::date)::text,'MM-YYYY'::text)::date as period_starting_date,
            (to_date(EXTRACT(MONTH FROM intervals.interval_dt::date)::text  || '-'::text || EXTRACT(YEAR FROM intervals.interval_dt::date)::text,'MM-YYYY'::text)::date + interval '1 month' - interval '1 day')::date as period_ending_date
            FROM
                (
                SELECT (generate_series(to_date('01-JAN-1970','DD-MON-YYYY'), (now()::date + '100 years'::interval)::date, '1 mon'::interval))::date as interval_dt
                , 'MONTH' as period
                ORDER BY period, interval_dt) intervals 
        )

SELECT studyid::TEXT AS studyid,
        studyname::TEXT AS studyname,
        program::TEXT AS program,
        therapeuticarea::TEXT AS therapeuticarea,
        siteid::TEXT AS siteid,
        sitename::TEXT AS sitename,
        sitecro::TEXT AS sitecro,
        sitecountry::TEXT AS sitecountry,
        siteregion::TEXT AS siteregion,
        dsseq::NUMERIC AS dsseq,
        dsterm::TEXT AS dsterm,
        dscat::TEXT AS dscat,
        period_starting_date::DATE AS period_starting_date,
        period_ending_date::DATE AS period_ending_date,
        ds_count::BIGINT AS ds_count,
        SUM(ds_count) OVER(PARTITION BY studyid, siteid, dsseq ORDER BY studyid, siteid, dsseq, period_starting_date)::NUMERIC AS ds_cumulative_count,
        NOW()::TIMESTAMP AS comprehend_update_time
from 
(select study.studyid,
        study.studyname,
        study.program,
        study.therapeuticarea,
        site.siteid,
        site.sitename,
        site.sitecro,
        site.sitecountry,
        site.siteregion,
        ds.dsseq,
        ds.dsterm,
        ds.dscat,
        my_periods.period_starting_date,
        my_periods.period_ending_date,
        date_part('month', my_periods.period_starting_date) as period_month,
        date_part('year', my_periods.period_starting_date) as period_year,
        sum(CASE WHEN ds.dsstdtc >= period_starting_date and ds.dsstdtc <= period_ending_date THEN 1::integer ELSE 0::integer END) as ds_count
from study 
    join site on (study.studyid = site.studyid)
    join subject on (study.studyid = subject.studyid and site.comprehendid = subject.sitekey) 
    join rpt_subject_disposition ds on (subject.comprehendid = ds.comprehendid)
    join ds_min_max on (study.studyid = ds_min_max.studyid)
    join my_periods on (period_starting_date >= to_date(EXTRACT(MONTH FROM ds_min_max.min_dsstdtc::date)::text  || '-'::text || EXTRACT(YEAR FROM ds_min_max.min_dsstdtc::date)::text,'MM-YYYY'::text)::date
                         and period_ending_date <= to_date(EXTRACT(MONTH FROM ds_min_max.max_dsstdtc::date)::text  || '-'::text || EXTRACT(YEAR FROM ds_min_max.max_dsstdtc::date)::text,'MM-YYYY'::text)::date + interval '1 month' )
where ds.dsstdtc is not null
group by study.studyid, study.studyname, study.program, study.therapeuticarea, 
         site.siteid, site.sitename, site.sitecountry, site.siteregion, site.sitecro,
         ds.dsseq, ds.dsterm, ds.dscat, my_periods.period_starting_date, my_periods.period_ending_date
order by my_periods.period_starting_date, ds.dsseq) a;




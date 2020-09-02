/*
rpt_subject_days

Notes: Adding PLO subjects days monthly 

Revision History 8-Mar-2017 VinhHo New PLO

*/

create table rpt_subject_days as
with 
        comprehendcodelist_handler AS (SELECT DISTINCT s.studyid, r.study_start_disposition AS codevalue
                                        FROM study s
                                        LEFT JOIN rpt_subject_disposition r ON (s.studyid = r.studyid)),

    -- Subject Days calcuated accross a time series as follows:
   -- 1. If subject exit date is less than the subject start date, return 0
   -- 2. If subject start date is greater than the current date, return 0
   -- 3. If subject start disposition date (determined by comprehendcodelist SUBJECT_DAY setting) exists, but start date is null, return null
   -- 4. If subject exit disposition exists, but exit date is null, then return null
   -- 5. If Subject Start Date is greater than the period month end or the subject exit date is less than the month start, then return 0
   -- 6. If Subject started prior to the period starting date and has an exit date that is less than the month end or current date (whichever is less)
   --       then return exit date minus the month start + 1 day
   -- 7. If Subject started after or on the period starting date and exited on or before the month end or current date
   --       then return exit date minus subject start date+ 1 day
   -- 8. If subject start date is less than or equal to period month start and exit date is null or exit date is greater than the period ending date
   --       or current date (whichever is less),
   --       then return the current date or period ending date (whichever is less) minus the period month start + 1 day
   -- 9. If Subject start date is greater than or equal to the period month start and exit date is null or exit date is greater than the period ending date
   --       or current date (whichever is less), 
   --       then return current date or period ending date (whichever is less) minus the subject start date + 1
  subject_days AS (
    WITH subject_days_breakdown AS (
        WITH my_periods as (
            SELECT period,
            'MONTHLY'::text as period_type,
            to_date(EXTRACT(MONTH FROM intervals.interval_dt::date)::text  || '-'::text || EXTRACT(YEAR FROM intervals.interval_dt::date)::text,'MM-YYYY'::text)::date as month_start,
            (to_date(EXTRACT(MONTH FROM intervals.interval_dt::date)::text  || '-'::text || EXTRACT(YEAR FROM intervals.interval_dt::date)::text,'MM-YYYY'::text)::date + interval '1 month' - interval '1 day')::date as month_end
            FROM
                (SELECT (generate_series(to_date('01-JAN-1970','DD-MON-YYYY'), (now()::date + '100 years'::interval)::date, '1 mon'::interval))::date as interval_dt
                , 'MONTH' as period
                ORDER BY period, interval_dt) intervals 
        ),
        earliest_date as (
            SELECT s.studyid, COALESCE(min(ds.dsstdtc), min(s.studystartdate))::date as min_dt 
            FROM study s
            LEFT JOIN rpt_subject_disposition ds ON (s.studyid = ds.studyid) 
            GROUP BY s.studyid
        ),
        subject_dates as (select s.comprehendid, s.studyid, s.siteid, s.usubjid, 
                                    a.dsstdtc as subject_start_date,
                                    b.dsstdtc::date as exit_date, 
                                    b.dsseq as exit_seq,
                                    c.dsterm as subject_latest_status, 
                                    b.dsterm as subjectexitdisposition
                            from subject s
                            left join (select ds.comprehendid, ds.studyid, ds.siteid, ds.usubjid, ds.dsseq, min(ds.dsstdtc) dsstdtc 
                                        from rpt_subject_disposition ds 
                                        where dsterm = study_start_disposition
                                        group by 1, 2, 3, 4, 5) a on (s.comprehendid = a.comprehendid)
                            left join (select d1.comprehendid, d2.dsstdtc, d2.dsseq, d2.dsterm
                                        from (select comprehendid, max(dsseq) dsseq 
                                                from rpt_subject_disposition ds
                                                where dswithdrawn is true or dscompleted is true
                                                group by comprehendid) d1, 
                                        rpt_subject_disposition d2 
                                        where d1.comprehendid = d2.comprehendid and d1.dsseq = d2.dsseq) b on (s.comprehendid = b.comprehendid) 
                            --get dsterm -> subject_latest_status
                            left join (select d2.comprehendid, d2.dsterm
                                        from (select comprehendid, max(dsseq)::numeric as max_dsseq 
                                                from rpt_subject_disposition ds 
                                                group by comprehendid order by comprehendid) d1, rpt_subject_disposition d2 
                            where d1.comprehendid = d2.comprehendid and d1.max_dsseq = d2.dsseq order by comprehendid) c on (s.comprehendid = c.comprehendid))
      
      SELECT sd.comprehendid, month_start AS month,
      (CASE     WHEN  (exit_date < subject_start_date) -- 1. Exit date < subject start date
         or (subject_start_date > now()::date) -- 2. Subject start date > the current date
        then 0::integer
         WHEN (subject_start_date is null) -- 3. Subject start disposition exists, but start date is null
      or (exit_seq is not null and exit_date is null) -- 4. Exit disposition exists, but exit date is null
       then null::integer
          WHEN subject_start_date > month_end or exit_date < month_start then -- 5. If Subject Start Date is greater than the period month end or the subject exit date is less than the month start, then return 0
          0::integer
          WHEN subject_start_date <= month_start and exit_date is not null and exit_date <= (case when now()::date > month_end then month_end else now()::date end)  then -- 6. Subject Start Dt before or at start of period and end date before or add period ending date - return exit_date minus period starting date + 1
          (exit_date - month_start)::integer + 1::integer -- Includes the exit date in count
          WHEN subject_start_date >= month_start and exit_date is not null and exit_date <= (case when now()::date > month_end then month_end else now()::date end) then -- 7. Subject Start Dt after or on start of period and end date not null and end date before end of period, then exitdt minus subject start date + 1
          (exit_date - subject_start_date)::integer + 1::integer -- Includes the exit date in count
          WHEN subject_start_date <= month_start and (exit_date is null or exit_date > (case when now()::date > month_end then month_end else now()::date end)) then -- 8. Subject Start Dt before or on start date and exit date is null or after the period ending date, then periodending date - start_date + 1
      ((case when now()::date > month_end then month_end else now()::date end) - month_start::date)::integer + 1::integer -- Include ending date in count
          WHEN subject_start_date >= month_start and (exit_date is null or exit_date > (case when now()::date > month_end then month_end else now()::date end)) then -- 9. Subject Start Dt after or on start date and exit date is null or exit date is greater than ending date then ending date - subject start date + 1
      ((case when now()::date > month_end then month_end else now()::date end) - subject_start_date::date)::integer + 1::integer
          else -999999::integer
      end) AS subject_days,
      subject_start_date as start_date,
      exit_date,
      subject_latest_status,
      subjectexitdisposition
      FROM my_periods mp, 
          earliest_date ed,
          subject_dates sd
      where mp.period = 'MONTH'  and 
            month_end >= min_dt and 
            month_start <= now()::date and 
            coalesce(sd.subject_start_date, ed.min_dt) <= month_end and
            ed.studyid = sd.studyid
    )
    SELECT comprehendid, 
      month, 
      subject_days,
      sum(subject_days) OVER (PARTITION BY comprehendid ORDER BY month) AS subject_days_cumulative,
      sum(subject_days) OVER (PARTITION BY comprehendid ORDER BY comprehendid) as total_subject_days,
      start_date,
      exit_date,
      subject_latest_status,
      subjectexitdisposition
    FROM subject_days_breakdown
  ),
  subject_extended AS (
    SELECT subject.comprehendid::text, 
    study.studyid::text, 
    study.studyname::text,
    study.studydescription::text,
    study.studystartdate,
    study.studycompletiondate,
    site.siteid::text,
    site.sitename::text,
    site.sitecro::text,
    site.sitecountry::text,
    site.sitecreationdate,
    site.siteactivationdate,
    site.sitedeactivationdate,
    site.siteinvestigatorname::text,
    site.sitecraname::text,
    subject.usubjid::text
    FROM study, site, subject
    WHERE study.comprehendid = site.studyid AND site.comprehendid = subject.sitekey
  )
  -- start main query 
  SELECT subject.comprehendid, 
      subject.studyid, 
      subject.studyname,
      subject.studydescription,
      subject.studystartdate,
      subject.studycompletiondate,
      subject.siteid,
      subject.sitename,
      subject.sitecro,
      subject.sitecountry,
      subject.sitecreationdate,
      subject.siteactivationdate,
      subject.sitedeactivationdate,
      subject.siteinvestigatorname,
      subject.sitecraname,
      subject.usubjid, 
      sd.subject_latest_status as subjectstatus ,
      sd.exit_date as exitdate,
      clh.codevalue as subjectstartdisposition,
      sd.subjectexitdisposition,
      sd.start_date as subjectdaystartdt,
      coalesce(sd.exit_date, now()::date) as subjectdaysenddt,
      sd.month as thismonth,
      sd.subject_days::integer as thismonthsubjectdays, 
      sd.subject_days_cumulative::integer as cumulativesubjectdays,
      sd.total_subject_days::integer as totalsubjectdays,
      now()::timestamp as comprehend_update_time
  FROM subject_extended AS subject
  JOIN subject_days AS sd ON subject.comprehendid = sd.comprehendid 
  LEFT JOIN comprehendcodelist_handler as clh on (subject.studyid = clh.studyid) ;

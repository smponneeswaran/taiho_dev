/*
rpt_site_days PLO

Notes:  
     
Revision History: 26-Dec-2016 VinhHo - Adding new PLO
                 
*/

create table rpt_site_days as
with site_active as (
	SELECT  
		study_.studyid,
		site_.siteid,
		study_.studystartdate,
		study_.studycompletiondate,
		site_.sitecreationdate,
		site_.siteactivationdate,
		(case when (site_.sitedeactivationdate) is not null then (case when site_.sitedeactivationdate > current_date then current_date else site_.sitedeactivationdate end) else current_date end) as sitedaysenddt
	FROM 
		study study_  
		JOIN site site_ ON study_.comprehendid = site_.studyid
	WHERE   site_.siteactivationdate IS NOT NULL  
			AND (site_.siteactivationdate > site_.sitedeactivationdate AND site_.siteactivationdate > current_date::date) = false
	GROUP BY 1, 2, 3, 4, 5, 6, 7
	),
	condition_times as (
		SELECT 
			studyid,
			siteid, 
			min(siteactivationdate)::date as minTime, 
			max(sitedaysenddt)::date as maxTime 
		FROM site_active 
		GROUP BY 1, 2
	),
	interval_date as (
		SELECT 
			studyid,
			siteid, 
			generate_series(date_trunc('month', minTime)::date, date_trunc('month', maxTime)::date , '1 month':: interval)::date as times 
		FROM condition_times
	), 
	site_days as (
		SELECT  
			saf.studyid,
			saf.siteid,
			saf.studystartdate,
			saf.studycompletiondate,
			saf.sitecreationdate,
			saf.siteactivationdate,
			saf.sitedaysenddt,
			inter.times as monthtrunc,
			(CASE WHEN  inter.times < saf.siteactivationdate THEN saf.siteactivationdate ELSE inter.times END) AS barStart,
			(CASE WHEN  Lead(inter.times) over (partition by saf.studyid, saf.siteid  ORDER BY inter.times) IS NULL THEN saf.sitedaysenddt ELSE Lead(inter.times) over (partition by saf.studyid, saf.siteid ORDER BY inter.times) END) AS barEnd
		FROM site_active saf
		JOIN interval_date inter ON saf.studyid = inter.studyid AND saf.siteid = inter.siteid
	),
	site_days_count as (
		SELECT
			studyid,
			siteid,
			studystartdate,
			studycompletiondate,
			sitecreationdate,
			siteactivationdate,
			sitedaysenddt,
			monthtrunc,
			(barEnd - barStart)::int as site_days_count 
		FROM site_days
	),
	site_days_cumulative as (
		SELECT
			studyid,
			siteid,
			studystartdate,
			studycompletiondate,
			sitecreationdate,
			siteactivationdate,
			sitedaysenddt,
			monthtrunc,
			site_days_count, 
			sum(site_days_count) OVER (PARTITION BY studyid, siteid ORDER BY monthtrunc) AS site_days_cumulative 
		FROM site_days_count
		GROUP BY 1, 2, 3, 4, 5, 6 ,7, 8, 9	
	)
-- start main query	
 	SELECT
		studyid::text,
		siteid::text,
		studystartdate::date,
		studycompletiondate::date,
		sitecreationdate::date,
		siteactivationdate::date,
		sitedaysenddt::date,
		monthtrunc::date,
		site_days_count::bigint, 
		site_days_cumulative::bigint, 
		now()::timestamp as comprehend_update_time
	FROM site_days_cumulative;

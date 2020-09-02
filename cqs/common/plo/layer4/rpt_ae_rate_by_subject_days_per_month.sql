/*
rpt_ae_rate_by_subject_days_per_month PLO
*/
create table rpt_ae_rate_by_subject_days_per_month as
with ae_rate_by_subject_days_data_per_month as
(select studyid,
		sitecountry,
		siteid,
		month_trunc,
		SUM(coalesce(subject_day_this_month,0)::numeric/coalesce(weight,1)) subject_days_weight_ratio_per_month,
		SUM((case when aestdtc is null then 0 else 1 end)) subject_days_count
from rpt_ae_rate_by_subject_days
group by studyid,
		 sitecountry,
		 siteid, 
		 month_trunc),

ae_rate_by_subject_days_data_per_site as
(select studyid,
		sitecountry,
		siteid,
		coalesce(subject_days_weight_ratio,0) subject_days_weight_ratio_per_site
from rpt_ae_rate_by_subject_days
group by studyid,
		 sitecountry,
		 siteid,
		 subject_days_weight_ratio)

select 
	m.studyid::text,
	m.sitecountry::text,
	m.siteid::text,
	m.month_trunc::date,
	s.subject_days_weight_ratio_per_site::numeric,
	m.subject_days_weight_ratio_per_month::numeric,
	m.subject_days_count::bigint,
	now()::timestamp as comprehend_update_time
from ae_rate_by_subject_days_data_per_month m
left join ae_rate_by_subject_days_data_per_site s on s.studyid = m.studyid and s.siteid = m.siteid;

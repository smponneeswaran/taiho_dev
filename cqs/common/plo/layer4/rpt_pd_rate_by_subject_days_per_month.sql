/*
rpt_pd_rate_by_subject_days_per_month PLO
*/
create table rpt_pd_rate_by_subject_days_per_month as
with pd_rate_by_subject_days_data as
(select studyid,
		sitecountry,
		siteid,
		month_trunc,
		SUM(coalesce(subject_day_this_month,0)::numeric/coalesce(weight,1)) subject_days_weight_ratio,
		SUM((case when dvstdtc is null then 0 else 1 end)) subject_days_count
from rpt_pd_rate_by_subject_days
group by studyid,
		 sitecountry,
		 siteid, 
		 month_trunc)

select 
	studyid::text,
	sitecountry::text,
	siteid::text,
	month_trunc::date,
	subject_days_weight_ratio::numeric,
	subject_days_count::bigint,
	now()::timestamp as comprehend_update_time
from pd_rate_by_subject_days_data;

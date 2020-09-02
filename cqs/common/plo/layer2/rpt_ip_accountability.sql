/*
rpt_ip_accountability PLO

Notes: 

Revision History: 12-Sep-2016 Michelle Engler - New Header and add Comprehend Update Time
*/

create table rpt_ip_accountability as
with skeleton as (
        select distinct
                ipaccountability.comprehendid,
                ipaccountability.studyid,
                ipaccountability.siteid,
                site.sitecountry,
                site.sitename,
                date_trunc('month', ipaccountability.ipdate)::date trunc_ip_month
        from
                ipaccountability
        inner join site on (site.studyid = ipaccountability.studyid and site.siteid = ipaccountability.siteid)
),
in_transit as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) in_transit,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 1
        group by
                1, 2, 3, 5
),
lost_in_transit as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) lost_in_transit,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 1.1
        group by
                1, 2, 3, 5
),
lost_at_site as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) lost_at_site,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 3.1
        group by
                1, 2, 3, 5
),
at_site as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) at_site,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 2
        group by
                1, 2, 3, 5
),


dispensed as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) dispensed,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 3
        group by
                1, 2, 3, 5
),
returned_to_site as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) returned_to_site,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 4
        group by
                1, 2, 3, 5
),
returned_to_distribution as (
        select
                comprehendid,
                studyid,
                siteid,
                sum(ipquantity) returned_to_distribution,
                date_trunc('month', ipdate)::date trunc_ip_month
        from 
                ipaccountability
        where
                ipseq = 5
        group by
                1, 2, 3, 5
),
status_grouped as (
select 
        skeleton.*,
        coalesce(in_transit.in_transit, 0) as shipped,
        coalesce(at_site.at_site, 0) as received,
        coalesce(lost_in_transit.lost_in_transit, 0) as lost_in_transit,
        coalesce(lost_at_site.lost_at_site, 0) as lost_at_site,
        coalesce(dispensed.dispensed, 0) as dispensed,
        coalesce(returned_to_site.returned_to_site, 0) as returned_to_site,
        coalesce(returned_to_distribution.returned_to_distribution, 0) as returned_to_distribution
        
        
        
from skeleton 
left join in_transit on (skeleton.comprehendid = in_transit.comprehendid) and (skeleton.trunc_ip_month = in_transit.trunc_ip_month)
left join at_site on (skeleton.comprehendid = at_site.comprehendid) and (skeleton.trunc_ip_month = at_site.trunc_ip_month)
left join lost_in_transit on (skeleton.comprehendid = lost_in_transit.comprehendid) and (skeleton.trunc_ip_month = lost_in_transit.trunc_ip_month)
left join lost_at_site on (skeleton.comprehendid = lost_at_site.comprehendid) and (skeleton.trunc_ip_month = lost_at_site.trunc_ip_month)
left join dispensed on (skeleton.comprehendid = dispensed.comprehendid) and (skeleton.trunc_ip_month = dispensed.trunc_ip_month)
left join returned_to_site on (skeleton.comprehendid = returned_to_site.comprehendid) and (skeleton.trunc_ip_month = returned_to_site.trunc_ip_month)
left join returned_to_distribution on (skeleton.comprehendid = returned_to_distribution.comprehendid) and (skeleton.trunc_ip_month = returned_to_distribution.trunc_ip_month)
),

status_sum as (
select 
        studyid, 
        sum(shipped) total_shipped,
        sum(received) total_received,
        sum(lost_in_transit) total_lost_in_transit,
        sum(dispensed) total_dispensed, 
        sum(returned_to_site) total_returned_to_site, 
        sum(lost_at_site) total_lost_at_site, 
        sum(returned_to_distribution) total_returned_to_distribution 
from status_grouped group by studyid
),

study_enrollment as (
select
        studyid,
        count(*) study_enrolled
from rpt_subject_disposition
where
        dsevent = 'ENROLLED'
group by 
        studyid
),

total_sites_in_study as (
select studyid, count(*) site_count from site group by studyid
),

status_average as (
select status_sum.studyid,
        status_sum.total_shipped / total_sites_in_study.site_count avg_shipped,
        status_sum.total_received / total_sites_in_study.site_count avg_received,
        status_sum.total_lost_in_transit / total_sites_in_study.site_count avg_lost_in_transit,
        status_sum.total_dispensed / total_sites_in_study.site_count avg_dispensed,
        status_sum.total_returned_to_site / total_sites_in_study.site_count avg_returned_to_site,
        status_sum.total_lost_at_site / total_sites_in_study.site_count avg_lost_at_site,
        status_sum.total_returned_to_distribution / total_sites_in_study.site_count avg_returned_to_distribution,
        study_enrollment.study_enrolled total_enrolled_per_study
from status_sum
left join total_sites_in_study on (status_sum.studyid = total_sites_in_study.studyid)
left join study_enrollment on (study_enrollment.studyid = total_sites_in_study.studyid)
),

subject_enrollment as (
select subject.sitekey comprehendid, count(*) enrolled from rpt_subject_disposition ds join subject on ds.comprehendid = subject.comprehendid where ds.dsevent = 'ENROLLED' group by subject.sitekey
),
subject_enrollment_country as (
select ds.studyid, site.sitecountry, count(*) enrolled from rpt_subject_disposition ds join site on ds.studyid = site.studyid and ds.siteid = site.siteid where ds.dsevent = 'ENROLLED' group by ds.studyid, site.sitecountry
)
select 
        status_grouped.*,
        status_average.avg_shipped,
        status_average.avg_received,
        status_average.avg_lost_in_transit,
        status_average.avg_dispensed,
        status_average.avg_returned_to_site,
        status_average.avg_lost_at_site,
        status_average.avg_returned_to_distribution,
        coalesce(status_average.total_enrolled_per_study, 0) AS total_enrolled_per_study,
        coalesce(subject_enrollment.enrolled, 0) AS subjects_enrolled_at_site,
        coalesce(subject_enrollment_country.enrolled, 0) AS subjects_enrolled_at_country,
        now()::timestamp as comprehend_update_time
from status_grouped
left join status_average on status_grouped.studyid = status_average.studyid
left join subject_enrollment on status_grouped.comprehendid = subject_enrollment.comprehendid
left join subject_enrollment_country on (status_grouped.studyid = subject_enrollment_country.studyid and status_grouped.sitecountry = subject_enrollment_country.sitecountry);

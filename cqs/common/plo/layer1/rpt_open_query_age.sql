/*
rpt_open_query_age PLO

Notes:  The Open Query Age PLO groups eachopen query into an age category
     
Revision History: 12-Sep-2016 Michelle Engler - Add header comment and Comprehend Update Time
                  14-Jan-2017 Michelle Engler - Add comments for tech writer
*/

create table rpt_open_query_age as
select
        query.comprehendid as comprehendid,
        query.objectuniquekey,
        query.studyid,
        study.studyname,
        query.siteid,
        site.sitename,
        site.sitecountry,
        site.siteregion,
        query.usubjid,
        -- Queries are grouped by month, thus the query open date is converted to month for this purpose
        date_trunc('MONTH', query.queryopeneddate::date)::date as queryopendate_my,
        extract(day from now() - query.queryopeneddate) as queryage,
        case
            when extract(day from now() - query.queryopeneddate) < 7 then '< 7 days'
            when extract(day from now() - query.queryopeneddate) <= 14 then '7-14 days'
            when extract(day from now() - query.queryopeneddate) <= 28 then '15-28 days'
            else '> 28 days'
        end as category,
        now()::timestamp as comprehend_update_time
from    study
        inner join site on study.studyid = site.studyid
        inner join query on study.studyid = query.studyid and site.siteid = query.siteid
where query.querycloseddate is null -- Query Closed Date being null is how we select the open queries
;

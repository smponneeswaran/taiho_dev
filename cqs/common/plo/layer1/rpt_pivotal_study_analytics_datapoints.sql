/*
rpt_pivotal_study_analytics_datapoints PLO
CDM Version: 2.6.2
Notes:  
    The Pivotal Study Analytics Datapoints PLO provides the forecasting datapoints needed
    for building the enrollment forecast line.  It presents the datapoints in both as logrithmic and 
    Polynomial regressions. Currently, only the logrithmic datapoints are used for the 
    application.  Each datapoint is calculated every month starting from the last
    four "actual" datapoints (actual is determined with enrollment counts by month). 
    The datapoints are included up to the forecasted date (projected date that the enrollment target will be hit). In the case
    the datapoints exceed a upper threshold, they will stop calculating.  This is done 
    in order to ensure we don't end up with a huge amount of data points due to bad data
    in the system.

    - The actual datapoints will be populated by the plo post process script
     
Revision History: 06-Dec-2016 Palaniraja Dhanavelu - Initial version
                  09-Feb-2017 MDE - From this point forward, the revision history will be maintained exclusively in github
*/

create table rpt_pivotal_study_analytics_datapoints  as
with study_details as (
                select s.comprehendid, 
                       s.therapeuticarea,
                       s.program,
                       s.studyid,
                       s.studyname
                       from study s)
select 
    s.comprehendid::text as comprehendid,
    s.therapeuticarea::text as therapeuticarea,
    s.program::text as program,
    s.studyid::text as studyid,
    s.studyname::text as studyname,
    null::text as forecast_type,
    null::date as forecast_date,
    null::numeric as forecast_count,
    now()::timestamp as comprehend_update_time
from study_details s;

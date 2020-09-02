/*
rpt_portfolio_oversight PLO
CDM Version: 2.5
Notes:  
    - All the portfolio level status will be calculated and populated by the follow up python script
     
Revision History: 27-Jul-2016 Palaniraja Dhanavelu - Initial version
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
*/

create table rpt_portfolio_oversight as 
with study_site_details as (
                select distinct study.comprehendid, 
                       site.sitecro as cro,
                       study.therapeuticarea,
                       study.program,
                       study.studyid,
                       study.studyname
                       from study 
                       join site on site.studyid = study.studyid
                      )
select comprehendid::text as comprehendid,
       cro::text as cro,
       therapeuticarea::text as therapeuticarea,
       program::text as program,
       studyid::text as studyid,
       studyname::text as studyname,
       null::text as milestone_achievement_status,
       null::text as enrollment_status,
       null::text as budget_status,
       null::money as total_overbudget_amount,
       null::text as total_overbudget_unit,
       null::text as study_risk_level,
       now()::timestamp as comprehend_update_time
from study_site_details;

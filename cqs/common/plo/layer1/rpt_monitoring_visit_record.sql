/*
rpt_monitoring_visit_record PLO

Notes:  
     
Revision History: 12-Sep-2016 Michelle Engler - New header and add Comprehend Update Time
*/
create table rpt_monitoring_visit_record as
select visitschedule.comprehendid,
       visitschedule.studyid,
       study.studyname,
       site.siteregion,
       site.sitecountry,
       visitschedule.siteid,
       site.sitename,
       site.siteinvestigatorname,
       site.sitecraname,
       visitschedule.visitname,
       visit.visitdate,
       visitschedule.plannedvisitdate,
       (case when visit.visitdate is not null then (visit.visitdate - visitschedule.plannedvisitdate)::integer
             when visit.visitdate is null and visitschedule.plannedvisitdate < now()::date then (now()::date - visitschedule.plannedvisitdate)::integer
            else null end)::integer  as latevisitdays,
       (rank() over (partition by (visitschedule.studyid, visitschedule.siteid)
                    order by visitschedule.plannedvisitdate asc))::integer as visitseq,
       now()::timestamp as comprehend_update_time
from sitemonitoringvisitschedule as visitschedule
left join sitemonitoringvisit as visit on (visitschedule.studyid = visit.studyid
                                           and visitschedule.siteid = visit.siteid
                                           and visitschedule.visitname = visit.visitname)
join site on (visitschedule.studyid = site.studyid
              and visitschedule.siteid = site.siteid)
join study on (visitschedule.studyid = study.studyid);

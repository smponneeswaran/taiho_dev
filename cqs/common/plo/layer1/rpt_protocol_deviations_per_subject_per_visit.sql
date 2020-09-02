/*
rpt_protocol_deviations_per_subject_per_visit

Notes: 

    The Protocol Deviations per subject per visit PLO brings together all the DVs and relates them to 
     collected visits, when possible.  

    Note that there are no limits on the DVs thus all DVs are included
     (even those with a null dvstdtc or null dvterm). 

    This PLO will handle the case when DV.visit is not provided and/or the SV table does not have any 
     collected visits; however, the output will be less meaningful if either or both of those are missing

    A note about SV visitseq: SV.visitseq is no longer used to increment visits and is expected to always
     be set to 1 in the USDM SV table.The limit in this PLO on visitseq = 1 is not expected to have any 
     limiting effect on the output but is there due to legacy reasons.  Probably, in the future, the
     sv.visitseq column should be removed throughout the USDM with any dependencies on KPIs also removed.

Revision History: 06-Dec-2016 Adam Kaus - Initial version
                  20-Jan-2017 Michelle Engler - Add comments for tech writer

*/

CREATE TABLE rpt_protocol_deviations_per_subject_per_visit AS
    --CTE to collect the unique set of visits that are referenced by deviations
WITH dv_visits AS (SELECT DISTINCT studyid, visit FROM dv),
    
    -- CTE to join the collected visits from the SV table to the distinct
    -- list of visits referenced by DV. This does not become a limiting 
    -- factor in the PLO since the sv_dv CTE will do a full outer join 
    -- to ensure that all DVs are included in this PLO regardless of 
    --- the visit -> sv linkage
    visits AS (SELECT sv.studyid,
                        sv.siteid,
                        sv.usubjid,
                        sv.comprehendid,
                        sv.visitnum,
                        sv.visit,
                        sv.visitseq,
                        sv.svstdtc,
                        sv.svendtc,
                        sv.objectuniquekey
                FROM sv
                JOIN dv_visits ON (sv.studyid = dv_visits.studyid AND sv.visit = dv_visits.visit)
                WHERE sv.visitseq = 1),

    -- This CTE does a full outer join between visits (a data set including the DVs with collected visits) and 
    -- dv to include all records in both tables even when the join does not find a match
    -- For a dv record with no visit, the sv columns will be null
    sv_dv AS (SELECT coalesce(v.studyid, dv.studyid) AS studyid,
                        coalesce(v.siteid, dv.siteid) AS siteid,
                        coalesce(v.usubjid, dv.usubjid) AS usubjid,
                        coalesce(v.comprehendid, dv.comprehendid) AS comprehendid,
                        v.visitnum AS sv_visitnum,
                        v.visit AS sv_visit,
                        v.visitseq AS sv_visitseq,
                        v.svstdtc AS sv_svstdtc,
                        v.svendtc AS sv_svendtc,
                        v.objectuniquekey AS sv_objectuniquekey,
                        dv.dvseq AS dv_dvseq,
                        dv.dvcat AS dv_dvcat,
                        dv.dvterm AS dv_dvterm,
                        dv.formid AS dv_formid,
                        dv.visit AS dv_visit,
                        dv.dvstdtc AS dv_dvstdtc,
                        dv.dvendtc AS dv_dvendtc,
                        dv.dvscat AS dv_dvscat,
                        dv.objectuniquekey AS dv_objectuniquekey
                FROM visits v
                /* Full outer join gets all the records from visits and all the records from dv
                    joining them if possible, but including all records from both tables regardless */
                FULL OUTER JOIN dv ON (v.comprehendid = dv.comprehendid AND v.visit = dv.visit) )

SELECT sv_dv.comprehendid, 
        sv_dv.studyid,
        sv_dv.siteid,
        sv_dv.usubjid,
        study.studyname,
        study.studydescription,
        study.studystatus,
        study.studyphase,
        study.studysponsor,
        study.therapeuticarea,
        study.program,
        study.medicalindication,
        study.studystartdate,
        study.studycompletiondate,   
        site.sitename,
        site.sitecro,
        site.siteinvestigatorname,
        site.sitecraname,
        site.sitecountry,
        site.siteregion,
        site.sitecreationdate,
        site.siteactivationdate,
        site.sitedeactivationdate,
        /* This is to help with sorting - if the visitdy is specified in TV, we use 
            include it in determining a sequential sort order for use by the KPI */
        (CASE WHEN tv.visitdy IS NOT NULL THEN
            row_number() over(partition by sv_dv.comprehendid order by tv.visitdy, tv.visitnum, sv_dv.sv_visitseq, sv_dv.dv_dvseq)
        ELSE null END)::integer AS sortseq, 
        sv_dv.sv_visitnum,
        sv_dv.sv_visit,
        sv_dv.sv_visitseq,
        sv_dv.sv_svstdtc,
        sv_dv.sv_svendtc,
        sv_dv.sv_objectuniquekey,
        sv_dv.dv_dvseq,
        sv_dv.dv_dvcat,
        sv_dv.dv_dvterm,
        sv_dv.dv_formid,
        sv_dv.dv_visit,
        sv_dv.dv_dvstdtc,
        sv_dv.dv_dvendtc,
        sv_dv.dv_dvscat,
        sv_dv.dv_objectuniquekey,
        now()::timestamp as comprehend_update_time
FROM sv_dv
JOIN study ON (sv_dv.studyid = study.studyid)
JOIN site ON (sv_dv.studyid = site.studyid AND sv_dv.siteid = site.siteid)
/* Left join to TV is for getting visitdy for sorting, if possible */
LEFT JOIN tv ON (sv_dv.studyid = tv.studyid AND sv_dv.sv_visit = tv.visit);

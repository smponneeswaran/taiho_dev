/*
rpt_view_subj_count_by_last_visit PLO

Notes:  The View Subject Count by Last Visit lists all the subjects with their latest, collected, scheduled visit.  The PLO
            is aware of the Visit Compliance configuration setting and will only include subjects that have a last visit as 
            the first visit (tv.visitdy = 0) if the configuration setting is show first.
     
Revision History: 12-Sep-2016 Michelle Engler - Add header comment and Comprehend Update Time
                  09-Dec-2016 Adam Kaus - Added VISIT_COMPLIANCE configuration to maintain consistency across PLOs (TP 19432)
                  23-Jan-2017 Michelle Engler - Add comments for tech writer
*/

create table rpt_view_subj_count_by_last_visit as
with
    -- CTE to default the 'Visist Compliance' to "hide first" when there is no matching entry is present in comprehendcodelist table for specific study 
    --   Hide First: Use the first visit to determine if the visit schedule has started for a given subject and to 
    --               get the visit start date for purposes of determining visit compliance AND do not include that
    --               first visit as a record in the PLO
    --   Show First: Use the first visit to determine if the visit schedule has started for a given subject and to 
    --               get the visit start date for purposes of determining visit compliance AND include that first 
    --               visit record in the PLO
    dflt_comprehendcode as (select 'VISIT_COMPLIANCE'::text as codename,
                            'hide first'::text as codevalue),

    -- CTE to handle the functionality of fetching study specific code values with the following scenarios
    --      1. If studyid is present in  comprehendcodelist table, then the corresponding code value for that studyid will be fetched
    --      2. if studyid is not present in comprehendcodelist table, then codevalue of  codekey 'default' will be assigned to the studyid's missing in comprehendcodelist 
    --      3. if both studyid and 'default' code key is not present in comprehendcodelist table, then the default codevalue ('hide first' for 'VISIT_COMPLIANCE') will be assigned using dflt_comprehendcode CTE
    comprehendcodelist_handler as (select s.studyid, coalesce(cl1.codename,cl2.codename, dflt.codename) as codename , coalesce(cl1.codekey,cl2.codekey) as codekey, coalesce(coalesce(cl1.codevalue,cl2.codevalue), dflt.codevalue)  as codevalue
                                    from study s
                                    left join comprehendcodelist cl1 on (cl1.codename = 'VISIT_COMPLIANCE' and s.studyid = cl1.codekey)
                                    left join comprehendcodelist cl2 on (cl2.codename = 'VISIT_COMPLIANCE' and cl2.codekey = 'default')
                                    left join dflt_comprehendcode dflt on ( dflt.codename = 'VISIT_COMPLIANCE')), -- condition to fetch the code value from dflt_comprehendcode  when both study and default condition are missing in comprehendcodelist                                                          

    -- Latest SV + Max SV collects the most recent scheduled visit collected from the Subject Visits table
    --  These CTEs are aware of the configuration setting and will only bring back the first 
    --  visit (should that be the latest visit) if the setting for visit compliance is show first
    --  The definition of most recently scheduled visit collected is to use the maximum visit number
    max_sv as (select sv.comprehendid, max(sv.visitnum) as visitnum
                from sv
                join tv on (sv.studyid = tv.studyid and sv.visit = tv.visit)
                join comprehendcodelist_handler clh on (sv.studyid = clh.studyid) 
                where tv.visitdy != 99999
                and ((lower(clh.codevalue) = 'hide first' and tv.visitdy != 0) or
                        (lower(clh.codevalue) = 'show first'))
                group by sv.comprehendid),

    latest_sv as (
    select distinct
            svo.comprehendid,
            svo.studyid,
            svo.siteid,
            svo.usubjid,
            svo.visit,
            now()::timestamp as comprehend_update_time
    from
            sv svo
    join max_sv svm 
        on (svo.comprehendid = svm.comprehendid and svo.visitnum = svm.visitnum))
select
        comprehendid::text,
        studyid::text,
        siteid::text,
        usubjid::text,
        visit::text,
        comprehend_update_time::timestamp
from
        latest_sv
where
        /* latest Subject Visit records with a null visit be excluded */
        latest_sv.visit is not null;


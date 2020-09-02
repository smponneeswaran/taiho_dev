/*
dimstudymilestone

NOTE : PLO attempts to collect planned and actual milestones together, establishing start and end dates, as well as the delay

*/
CREATE TABLE dimstudymilestone AS
WITH planned_actual_current_next AS (-- CTE to fetch the actual start date and planned start date for each milestone based on the previous milestone actual and planned end dates available in studymilestone object.
                                        SELECT planned.studyid,
                                            planned.milestoneseq,
                                            planned.milestonelabel,
                                            lag(planned.expecteddate) over w1 AS planned_start_date,
                                            planned.expecteddate AS planned_next_start_date,
                                            lag(actual.expecteddate) over w1 AS  actual_start_date,
                                            actual.expecteddate AS actual_next_start_date,
                                            planned.expecteddate + ((CASE WHEN actual.iscriticalpath = true THEN lag(actual_crit.expecteddate)  over w2
                                                                          ELSE lag(actual.expecteddate) over w1 END) - 
                                                                    (CASE WHEN actual.iscriticalpath = true THEN lag(planned_crit.expecteddate) over w2 
                                                                          ELSE lag(planned.expecteddate) over w1 END)) as expected_actual_end -- Expected actual end date is calculted by adding current milestone planned end date and delayed days from previous milestone planned and actual end dates. this calculation is in sync with rpt_pivotal study analytics
                                            FROM ( SELECT studyid, milestonelabel, milestoneseq, expecteddate, iscriticalpath 
                                                    FROM studymilestone  WHERE lower(milestonetype) = 'actual'::text) actual
                                            JOIN ( SELECT studyid, milestonelabel, milestoneseq, expecteddate, iscriticalpath 
                                                    FROM studymilestone WHERE lower(studymilestone.milestonetype) = 'planned'::text ) planned ON ( actual.studyid = planned.studyid AND  actual.milestoneseq = planned.milestoneseq)
                                            LEFT JOIN ( SELECT studyid, milestoneseq, expecteddate, iscriticalpath 
                                                    FROM studymilestone  WHERE lower(milestonetype) = 'actual'::text and iscriticalpath = true) actual_crit ON ( actual.studyid = actual_crit.studyid AND  actual.milestoneseq = actual_crit.milestoneseq)
                                            LEFT JOIN ( SELECT studyid, milestonelabel, milestoneseq, expecteddate, iscriticalpath 
                                                    FROM studymilestone WHERE lower(studymilestone.milestonetype) = 'planned'::text and iscriticalpath = true ) planned_crit ON ( actual_crit.studyid = planned_crit.studyid AND  actual_crit.milestoneseq = planned_crit.milestoneseq)
                                            WINDOW w1 AS (PARTITION BY actual.studyid  ORDER BY actual.milestoneseq), -- window for all milstones
                                                   w2 AS (PARTITION BY actual_crit.studyid  ORDER BY actual_crit.milestoneseq)), -- window for critical milstones only

        -- CTE to calculate planned and actual start dates
        planned_actual_start AS ( SELECT pln_act_strt.studyid,
                                        pln_act_strt.milestoneseq,
                                        CASE WHEN (pln_act_strt.planned_start_date > pln_act_strt.planned_next_start_date) 
                                              THEN pln_act_strt.planned_next_start_date 
                                              ELSE pln_act_strt.planned_start_date END AS plannedstart,
                                        CASE WHEN (pln_act_strt.actual_start_date > pln_act_strt.actual_next_start_date) 
                                              THEN pln_act_strt.actual_next_start_date 
                                              ELSE pln_act_strt.actual_start_date END AS actualstart
                                        FROM  planned_actual_current_next pln_act_strt ) 

SELECT DISTINCT planned.comprehendid AS comprehendid,
       planned.studyid AS studyid,
       planned.milestoneseq AS milestoneseq,
       planned.milestonelabel AS milestonelabel,
       coalesce(pln_act_strt.plannedstart, next.planned_next_start_date) AS plannedstart,
       next.planned_next_start_date AS plannedend,
       (next.planned_next_start_date - pln_act_strt.plannedstart) AS plannedduration,
       coalesce(pln_act_strt.actualstart, next.actual_next_start_date) AS actualstart,
       next.actual_next_start_date AS actualend,
       (next.actual_next_start_date - pln_act_strt.actualstart) AS actualduration,
       CASE WHEN next.actual_next_start_date is null then (next.expected_actual_end - next.planned_next_start_date)
            ELSE (next.actual_next_start_date - next.planned_next_start_date) END AS delay, -- delay is calculated as diff between actual end and planned end. in case of absence of actual end, expected end date is used to calculate the difference.
       next.expected_actual_end AS expected_actual_end,
       planned.ismandatory,
       planned.iscriticalpath,
       CASE WHEN lower(next.milestonelabel) = lower(rpt_stdy_analytics.current_milestone)
            THEN true 
            ELSE false END AS iscurrent,
       orig.expecteddate AS original_expected_date,
       base.expecteddate AS baseline_expected_date,
       now()::timestamp AS comprehend_update_time
FROM studymilestone planned
JOIN planned_actual_current_next next ON (planned.studyid = next.studyid AND planned.milestoneseq = next.milestoneseq)
JOIN planned_actual_start pln_act_strt ON (next.studyid = pln_act_strt.studyid AND next.milestoneseq = pln_act_strt.milestoneseq)
JOIN rpt_pivotal_study_analytics rpt_stdy_analytics ON (rpt_stdy_analytics.comprehendid = planned.comprehendid)
LEFT JOIN studymilestone orig ON (planned.studyid = orig.studyid AND planned.milestoneseq = orig.milestoneseq AND lower(orig.milestonetype) = 'original'::text)
LEFT JOIN studymilestone base ON (planned.studyid = base.studyid AND planned.milestoneseq = base.milestoneseq AND lower(base.milestonetype) = 'baseline'::text)
WHERE lower(planned.milestonetype) = 'planned'::text;


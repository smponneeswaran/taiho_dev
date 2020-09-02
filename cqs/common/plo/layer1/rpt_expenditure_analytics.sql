/*
rpt_expenditure_analytics PLO

Notes: 
    - Expenditure plan vs. actual by study/month

Revision History: 25-Jul-2016 Adam Kaus - Initial Version
                  12-Sep-2016 Michelle Engler - Add Comprehend Update Time
*/

CREATE TABLE rpt_expenditure_analytics AS
WITH plan_exp_month AS (SELECT studyid,
                                date_trunc('MONTH', expdtc)::date AS budget_month,
                                sum(expamountstd) AS planned_amount,
                                expunitstd
                            FROM siteplannedexpenditure
                            WHERE lower(exptype) = 'planned'
                            GROUP BY studyid, date_trunc('MONTH', expdtc)::date, expunitstd),

    actual_exp_month AS (SELECT studyid,
                                date_trunc('MONTH', expdtc)::date AS budget_month,
                                sum(expamountstd) AS actual_amount,
                                expunitstd
                            FROM siteplannedexpenditure
                            WHERE lower(exptype) = 'actual'
                            GROUP BY studyid, date_trunc('MONTH', expdtc)::date, expunitstd),

    total_budget AS (SELECT studyid,
                            sum(expamountstd) AS total_amount,
                            expunitstd
                        FROM siteplannedexpenditure
                        WHERE lower(exptype) = 'planned'
                        GROUP BY studyid, expunitstd),

    budget_schedule AS (SELECT studyid,
                                expunitstd,
                                generate_series(date_trunc('MONTH', greatest(min(expdtc), '1/1/1970'::date))::date, date_trunc('MONTH', max(expdtc))::date, '1 MONTH')::date AS budget_month
                        FROM siteplannedexpenditure
                        GROUP BY studyid, expunitstd),

    budget_balance AS (SELECT sch.*,
                                coalesce(plan.planned_amount, 0) AS budget,
                                coalesce(actual.actual_amount, 0) AS spent,
                                coalesce(plan.planned_amount, 0) - coalesce(actual.actual_amount, 0)  AS balance,
                                CASE WHEN 
                                    coalesce(actual.actual_amount, 0) - coalesce(plan.planned_amount, 0) <= 0 THEN 0
                                    ELSE coalesce(actual.actual_amount, 0) - coalesce(plan.planned_amount, 0)
                                END AS overbudget,
                                budget.total_amount as total_budget,
                                sum(coalesce(actual.actual_amount, 0)) over (partition by sch.studyid order by sch.budget_month) as total_spent,
                                budget.total_amount - sum(coalesce(actual.actual_amount, 0)) over (partition by sch.studyid order by sch.budget_month) as total_balance,
                                CASE WHEN 
                                    sum(coalesce(actual.actual_amount, 0)) over (partition by sch.studyid order by sch.budget_month) -  budget.total_amount <= 0 THEN 0
                                ELSE sum(coalesce(actual.actual_amount, 0)) over (partition by sch.studyid order by sch.budget_month) -  budget.total_amount
                                END AS total_overbudget,
                                (sum(coalesce(actual.actual_amount, 0)) over (partition by sch.studyid order by sch.budget_month)) / budget.total_amount::numeric AS total_budget_used_rate
                        FROM budget_schedule sch
                        LEFT JOIN plan_exp_month plan ON (sch.studyid = plan.studyid AND sch.budget_month = plan.budget_month AND sch.expunitstd = plan.expunitstd)
                        LEFT JOIN actual_exp_month actual ON (sch.studyid = actual.studyid AND sch.budget_month = actual.budget_month AND sch.expunitstd = actual.expunitstd)
                        LEFT JOIN total_budget budget ON (sch.studyid = budget.studyid AND sch.expunitstd = budget.expunitstd) )

SELECT study.comprehendid::text AS comprehendid,
        study.therapeuticarea::text AS therapeuticarea,
        study.program::text AS program,
        study.studyid::text AS studyid,
        study.studyname::text AS studyname,
        budget.budget_month::date AS budget_month,
        budget.expunitstd::text AS expenditure_units,
        budget.budget::numeric AS planned_expenditure,
        (CASE WHEN budget_month <= current_date THEN budget.spent ELSE null END)::numeric AS actual_expenditure,
        (CASE WHEN budget_month <= current_date THEN budget.overbudget ELSE null END)::numeric AS month_overbudget,
        budget.total_budget::numeric AS total_budget,
        (CASE WHEN budget_month <= current_date THEN budget.total_spent ELSE null END)::numeric AS total_spent,
        (CASE WHEN budget_month <= current_date THEN budget.total_balance ELSE null END)::numeric AS total_budget_balance,
        (CASE WHEN budget_month <= current_date THEN budget.total_overbudget ELSE null END)::numeric AS total_overbudget,
        (CASE WHEN budget_month <= current_date THEN budget.total_budget_used_rate ELSE null END)::numeric AS total_budget_used_rate,
        now()::timestamp as comprehend_update_time
FROM study 
JOIN budget_balance budget ON(study.studyid = budget.studyid)
ORDER BY study.comprehendid, budget.budget_month;


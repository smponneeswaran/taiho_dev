-- Presentation Layer Object (PLO) Not Null Constraints
-- Note: Add to this file using alphabetical order

-- Field Data Tabular Report (rpttab_fielddata)
ALTER TABLE rpttab_fielddata ALTER comprehendid SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER studyid SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER studyname SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER siteid SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER sitename SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER sitecountry SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER siteregion SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER usubjid SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER visitnum SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER visit SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER visitseq SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER formid SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER formname SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER fieldid SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER fieldname SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER fieldseq SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER dataentrydate SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER datacollecteddate SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER isprimaryendpoint SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER issecondaryendpoint SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER isrequired SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER issdv SET NOT NULL;
ALTER TABLE rpttab_fielddata ALTER log_num SET NOT NULL;


-- Latest Subject Disposition (rpt_latest_ds)
ALTER TABLE rpt_latest_ds ALTER studyid SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER studyname SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER siteid SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER sitekey SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER sitename SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER objectuniquekey SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER dscat SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER dslabel SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER dsterm SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER dsseq SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER is_latest_ds SET NOT NULL;
ALTER TABLE rpt_latest_ds ALTER dswithdrawn SET NOT NULL;


-- rpt_ds
ALTER TABLE rpt_ds ALTER studyid SET NOT NULL;
ALTER TABLE rpt_ds ALTER studyname SET NOT NULL;
ALTER TABLE rpt_ds ALTER siteid SET NOT NULL;
ALTER TABLE rpt_ds ALTER dsseq SET NOT NULL;
ALTER TABLE rpt_ds ALTER dscat SET NOT NULL;
ALTER TABLE rpt_ds ALTER dsterm SET NOT NULL;
ALTER TABLE rpt_ds ALTER period_starting_date SET NOT NULL;
ALTER TABLE rpt_ds ALTER period_ending_date SET NOT NULL;


ALTER TABLE rpt_ae_rate_by_subject_days ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER studyid SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER siteid SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER category SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER month_trunc SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER study_subjects SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER study_total_subject_days_this_category SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER study_total_ae_count_this_category SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER objectuniquekey SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days ALTER weight SET NOT NULL;


ALTER TABLE rpt_data_entry ALTER studyid SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER siteid SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER trunc_month SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER avg_days_in_month_interval SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER forms_in_month_interval SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER study_average SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER avg_entry_days_on_interval SET NOT NULL;
ALTER TABLE rpt_data_entry ALTER isprimaryendpoint SET NOT NULL;


ALTER TABLE rpt_subject_visit_schedule ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER studyid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER studyname SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER siteid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER sitename SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER visit SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER visitnum SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER visitdy SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER expected SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule ALTER category SET NOT NULL;


ALTER TABLE rpt_subject_visit_schedule_last_visit ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule_last_visit ALTER studyid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule_last_visit ALTER siteid SET NOT NULL;
ALTER TABLE rpt_subject_visit_schedule_last_visit ALTER usubjid SET NOT NULL;


ALTER TABLE rpt_query_rate_by_subject_days ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER siteid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER studyid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER studyname SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER sitename SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER month SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER query_count SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER query_count_cumulative SET NOT NULL;
ALTER TABLE rpt_query_rate_by_subject_days ALTER open_query_count SET NOT NULL;


ALTER TABLE rpt_query_rate_by_ecrf ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER objectuniquekey SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER siteid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER studyid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER formid SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER studyname SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER sitename SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER formname SET NOT NULL;
ALTER TABLE rpt_query_rate_by_ecrf ALTER form_count SET NOT NULL;


ALTER TABLE rpt_ip_accountability ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER studyid SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER siteid SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER sitename SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER trunc_ip_month SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER shipped SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER received SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER lost_in_transit SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER lost_at_site SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER dispensed SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER returned_to_site SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER returned_to_distribution SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_shipped SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_received SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_lost_in_transit SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_lost_at_site SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_dispensed SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_returned_to_site SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER avg_returned_to_distribution SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER total_enrolled_per_study SET NOT NULL;
--ALTER TABLE rpt_ip_accountability ALTER subjects_enrolled_at_site SET NOT NULL;
ALTER TABLE rpt_ip_accountability ALTER subjects_enrolled_at_country SET NOT NULL;


ALTER TABLE rpt_ae_rel_week ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER studyid SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER studyname SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER siteid SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER sitename SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER aeseq SET NOT NULL;
--ALTER TABLE rpt_ae_rel_week ALTER aestdtc SET NOT NULL;
--ALTER TABLE rpt_ae_rel_week ALTER relative_trial_week SET NOT NULL;
ALTER TABLE rpt_ae_rel_week ALTER objectuniquekey SET NOT NULL;


ALTER TABLE rpt_view_subj_count_by_last_visit ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_view_subj_count_by_last_visit ALTER siteid SET NOT NULL;
ALTER TABLE rpt_view_subj_count_by_last_visit ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_view_subj_count_by_last_visit ALTER visit SET NOT NULL;


ALTER TABLE rpt_ae_study_baseline ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER studyid SET NOT NULL;
--ALTER TABLE rpt_ae_study_baseline ALTER aeendtc SET NOT NULL;
--ALTER TABLE rpt_ae_study_baseline ALTER aestdtc SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER duration SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER study_avg_duration_this_term SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER study_count_this_term SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER study_avg_duration_all_term SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER study_count_all_term SET NOT NULL;
ALTER TABLE rpt_ae_study_baseline ALTER avg_term_count SET NOT NULL;


ALTER TABLE rpt_open_query_age ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER objectuniquekey SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER studyid SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER studyname SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER siteid SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER sitename SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER queryage SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER queryopendate_my SET NOT NULL;
ALTER TABLE rpt_open_query_age ALTER category SET NOT NULL;


ALTER TABLE rpt_monitoring_visit_record ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER studyid SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER studyname SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER siteid SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER sitename SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER visitname SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER plannedvisitdate SET NOT NULL;
ALTER TABLE rpt_monitoring_visit_record ALTER visitseq SET NOT NULL;


ALTER TABLE rpt_missing_data ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER studyid SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER studyname SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER sitename SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER formid SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER visit SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER visitseq SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER visitnum SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER formname SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER fieldid SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER issdv SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER isprimaryendpoint SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER issecondaryendpoint SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER isrequired SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER completed SET NOT NULL;
ALTER TABLE rpt_missing_data ALTER log_num SET NOT NULL;


ALTER TABLE rpt_enrollment_analytics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_enrollment_analytics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_enrollment_analytics ALTER studyname SET NOT NULL;
ALTER TABLE rpt_enrollment_analytics ALTER currentdate SET NOT NULL;
ALTER TABLE rpt_enrollment_analytics ALTER actual_enrollment_days SET NOT NULL;
ALTER TABLE rpt_enrollment_analytics ALTER actual_enrollment_count SET NOT NULL;

ALTER TABLE rpt_resource_analytics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_resource_analytics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_resource_analytics ALTER studyname SET NOT NULL;
ALTER TABLE rpt_resource_analytics ALTER cra_count SET NOT NULL;
ALTER TABLE rpt_resource_analytics ALTER enrollment_count SET NOT NULL;

ALTER TABLE rpt_sitemilestone ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER studyid SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER siteid SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER milestoneseq SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER milestonelabel SET NOT NULL;
ALTER TABLE rpt_sitemilestone ALTER ismandatory SET NOT NULL;

ALTER TABLE rpt_study_risk_analytics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER studyname SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER siteid SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER sitename SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER siteregion SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER actual_pd_count SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER actual_screen_failure_count SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER actual_ae_count SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER actual_study_days SET NOT NULL;
ALTER TABLE rpt_study_risk_analytics ALTER enrollment_count SET NOT NULL;

ALTER TABLE rpt_studymilestone ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_studymilestone ALTER studyid SET NOT NULL;
ALTER TABLE rpt_studymilestone ALTER milestoneseq SET NOT NULL;
ALTER TABLE rpt_studymilestone ALTER milestonelabel SET NOT NULL;
ALTER TABLE rpt_studymilestone ALTER ismandatory SET NOT NULL;
ALTER TABLE rpt_studymilestone ALTER iscriticalpath SET NOT NULL;

ALTER TABLE rpt_expenditure_analytics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_expenditure_analytics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_expenditure_analytics ALTER studyname SET NOT NULL;
ALTER TABLE rpt_expenditure_analytics ALTER budget_month SET NOT NULL;
ALTER TABLE rpt_expenditure_analytics ALTER expenditure_units SET NOT NULL;
ALTER TABLE rpt_expenditure_analytics ALTER planned_expenditure SET NOT NULL;
ALTER TABLE rpt_expenditure_analytics ALTER total_budget SET NOT NULL;

ALTER TABLE rpt_pivotal_study_analytics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_pivotal_study_analytics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_pivotal_study_analytics ALTER studyname SET NOT NULL;
ALTER TABLE rpt_pivotal_study_analytics ALTER current_enrollment_count SET NOT NULL;

ALTER TABLE rpt_pivotal_study_analytics_datapoints ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_pivotal_study_analytics_datapoints ALTER studyid SET NOT NULL;

ALTER TABLE rpt_portfolio_oversight ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_portfolio_oversight ALTER studyid SET NOT NULL;
ALTER TABLE rpt_portfolio_oversight ALTER studyname SET NOT NULL;


ALTER TABLE rpt_subject_information ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_subject_information ALTER studyid SET NOT NULL;
ALTER TABLE rpt_subject_information ALTER siteid SET NOT NULL;
ALTER TABLE rpt_subject_information ALTER usubjid SET NOT NULL;


ALTER TABLE rpt_site_performance ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_site_performance ALTER studyid SET NOT NULL;
ALTER TABLE rpt_site_performance ALTER siteid SET NOT NULL;


ALTER TABLE rpt_protocol_deviations_per_subject_per_visit ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_protocol_deviations_per_subject_per_visit ALTER studyid SET NOT NULL;
ALTER TABLE rpt_protocol_deviations_per_subject_per_visit ALTER siteid SET NOT NULL;
ALTER TABLE rpt_protocol_deviations_per_subject_per_visit ALTER usubjid SET NOT NULL;

ALTER TABLE rpt_site_days ALTER studyid SET NOT NULL;
ALTER TABLE rpt_site_days ALTER siteid SET NOT NULL;


ALTER TABLE rpt_pd_rate_by_subject_days ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days ALTER studyid SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days ALTER siteid SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days ALTER objectuniquekey SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days ALTER month_trunc SET NOT NULL;

ALTER TABLE rpt_subject_days ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_subject_days ALTER studyid SET NOT NULL;
ALTER TABLE rpt_subject_days ALTER studyname SET NOT NULL;
ALTER TABLE rpt_subject_days ALTER siteid SET NOT NULL;
ALTER TABLE rpt_subject_days ALTER sitename SET NOT NULL;
ALTER TABLE rpt_subject_days ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_subject_days ALTER thismonth SET NOT NULL;

ALTER TABLE dimdate ALTER date_dim_id SET NOT NULL;
ALTER TABLE dimdate ALTER date_actual SET NOT NULL;

ALTER TABLE dimdisposition ALTER comprehendid SET NOT NULL;
ALTER TABLE dimdisposition ALTER studyid SET NOT NULL;
ALTER TABLE dimdisposition ALTER siteid SET NOT NULL;
ALTER TABLE dimdisposition ALTER usubjid SET NOT NULL;
ALTER TABLE dimdisposition ALTER dsseq SET NOT NULL;
ALTER TABLE dimdisposition ALTER dscat SET NOT NULL;
ALTER TABLE dimdisposition ALTER dsterm SET NOT NULL;
ALTER TABLE dimdisposition ALTER objectuniquekey SET NOT NULL;

ALTER TABLE dimsite ALTER comprehendid SET NOT NULL;
ALTER TABLE dimsite ALTER siteid SET NOT NULL;
ALTER TABLE dimsite ALTER studyid SET NOT NULL;
ALTER TABLE dimsite ALTER sitename SET NOT NULL;
ALTER TABLE dimsite ALTER sitecountry SET NOT NULL;
ALTER TABLE dimsite ALTER siteregion SET NOT NULL;

ALTER TABLE dimstudymilestone ALTER comprehendid SET NOT NULL;
ALTER TABLE dimstudymilestone ALTER studyid SET NOT NULL;
ALTER TABLE dimstudymilestone ALTER milestoneseq SET NOT NULL;
ALTER TABLE dimstudymilestone ALTER milestonelabel SET NOT NULL;
ALTER TABLE dimstudymilestone ALTER ismandatory SET NOT NULL;
ALTER TABLE dimstudymilestone ALTER iscriticalpath SET NOT NULL;

ALTER TABLE dimsubject ALTER siteid SET NOT NULL;
ALTER TABLE dimsubject ALTER usubjid SET NOT NULL;
ALTER TABLE dimsubject ALTER studyid SET NOT NULL;
ALTER TABLE dimsubject ALTER status SET NOT NULL;

ALTER TABLE dimvisitrequiredfields ALTER studyid SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER visitnum SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER visit SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER formid SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER primaryendpointfields SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER secondaryendpointfields SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER sdvfields SET NOT NULL;
ALTER TABLE dimvisitrequiredfields ALTER objectuniquekey SET NOT NULL;

ALTER TABLE factformdata ALTER comprehendid SET NOT NULL;
ALTER TABLE factformdata ALTER studyid SET NOT NULL;
ALTER TABLE factformdata ALTER siteid SET NOT NULL;
ALTER TABLE factformdata ALTER usubjid SET NOT NULL;
ALTER TABLE factformdata ALTER visit SET NOT NULL;
ALTER TABLE factformdata ALTER formid SET NOT NULL;
ALTER TABLE factformdata ALTER formseq SET NOT NULL;
ALTER TABLE factformdata ALTER objectuniquekey SET NOT NULL;
ALTER TABLE factformdata ALTER formdefuniquekey SET NOT NULL;

ALTER TABLE factsiteperformance ALTER comprehendid SET NOT NULL;
ALTER TABLE factsiteperformance ALTER studyid SET NOT NULL;
ALTER TABLE factsiteperformance ALTER siteid SET NOT NULL;

ALTER TABLE factsubjectvisit ALTER comprehendid SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER studyid SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER studyname SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER siteid SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER sitename SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER sitecountry SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER siteregion SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER usubjid SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER visit SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER visitnum SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER visitdy SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER expected SET NOT NULL;
ALTER TABLE factsubjectvisit ALTER category SET NOT NULL;

ALTER TABLE dimstudy ALTER comprehendid SET NOT NULL;
ALTER TABLE dimstudy ALTER studyid SET NOT NULL;
ALTER TABLE dimstudy ALTER studyname SET NOT NULL;

ALTER TABLE factadverseevents ALTER siteid SET NOT NULL;
ALTER TABLE factadverseevents ALTER studyid SET NOT NULL;
ALTER TABLE factadverseevents ALTER comprehendid SET NOT NULL;
ALTER TABLE factadverseevents ALTER month_trunc SET NOT NULL;
ALTER TABLE factadverseevents ALTER aeseq SET NOT NULL;
ALTER TABLE factadverseevents ALTER objectuniquekey SET NOT NULL;

ALTER TABLE factportfoliodaily ALTER date_dim_id SET NOT NULL;
ALTER TABLE factportfoliodaily ALTER studyid SET NOT NULL;
ALTER TABLE factportfoliodaily ALTER siteid SET NOT NULL;

ALTER TABLE kpisummary ALTER studyid SET NOT NULL;
ALTER TABLE kpisummary ALTER siteid SET NOT NULL;

ALTER TABLE dimsitedates ALTER studyid SET NOT NULL;
ALTER TABLE dimsitedates ALTER siteid SET NOT NULL;
ALTER TABLE dimsitedates ALTER date_actual SET NOT NULL;

ALTER TABLE rpt_ae_rate_by_subject_days_per_month ALTER studyid SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days_per_month ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days_per_month ALTER siteid SET NOT NULL;
ALTER TABLE rpt_ae_rate_by_subject_days_per_month ALTER month_trunc SET NOT NULL;

ALTER TABLE rpt_pd_rate_by_subject_days_per_month ALTER studyid SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days_per_month ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days_per_month ALTER siteid SET NOT NULL;
ALTER TABLE rpt_pd_rate_by_subject_days_per_month ALTER month_trunc SET NOT NULL;

ALTER TABLE rpt_oversight_metrics ALTER metricid SET NOT NULL;
ALTER TABLE rpt_oversight_metrics ALTER studyid SET NOT NULL;

ALTER TABLE rpt_subject_disposition ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER studyid SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER siteid SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER usubjid SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dsterm SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dslabel SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dsseq SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dsactive SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dswithdrawn SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dscompleted SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER dsstarting SET NOT NULL;
ALTER TABLE rpt_subject_disposition ALTER objectuniquekey SET NOT NULL;

ALTER TABLE rpt_portfolio_summary_analytics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_portfolio_summary_analytics ALTER studyname SET NOT NULL;

ALTER TABLE rpt_country_metrics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_country_metrics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_country_metrics ALTER sitecountry SET NOT NULL;
ALTER TABLE rpt_country_metrics ALTER metricid SET NOT NULL;
ALTER TABLE rpt_country_metrics ALTER objectuniquekey SET NOT NULL;

ALTER TABLE rpt_site_metrics ALTER comprehendid SET NOT NULL;
ALTER TABLE rpt_site_metrics ALTER studyid SET NOT NULL;
ALTER TABLE rpt_site_metrics ALTER siteid SET NOT NULL;
ALTER TABLE rpt_site_metrics ALTER metricid SET NOT NULL;
ALTER TABLE rpt_site_metrics ALTER objectuniquekey SET NOT NULL;

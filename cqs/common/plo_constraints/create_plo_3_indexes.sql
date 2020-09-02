-- Presentation Layer Object (PLO) Primary Keys, Unique Keys, and Indexes
-- Note: Please add to this file in alphabetical order

-- Field Data Listings (rpttab_fielddata)
ALTER TABLE rpttab_fielddata
	ADD CONSTRAINT rpttab_fielddata_pk
	PRIMARY KEY (studyid, siteid, usubjid, visit, formseq, formid, fieldid, fieldseq, log_num);
CREATE INDEX rpttab_fielddata_idx1 ON rpttab_fielddata (issdv);
CREATE INDEX rpttab_fielddata_idx2 ON rpttab_fielddata (isrequired);
CREATE INDEX rpttab_fielddata_idx3 ON rpttab_fielddata (isprimaryendpoint);
CREATE INDEX rpttab_fielddata_idx4 ON rpttab_fielddata (issecondaryendpoint);
CREATE INDEX rpttab_fielddata_idx5 ON rpttab_fielddata (studyname);

-- rpt_ae_rate_by_subject_days
ALTER TABLE rpt_ae_rate_by_subject_days ADD CONSTRAINT rpt_ae_rate_by_subject_days_pk1 PRIMARY KEY (objectuniquekey);
CREATE INDEX rpt_ae_rate_by_subject_days_idx1 ON rpt_ae_rate_by_subject_days (objectuniquekey);


--rpt_subject_visit_schedule
ALTER TABLE rpt_subject_visit_schedule ADD CONSTRAINT rpt_subject_visit_schedule_pk PRIMARY KEY (comprehendid, visit, visitnum);
CREATE INDEX rpt_subject_visit_schedule_idx1 ON rpt_subject_visit_schedule (comprehendid);


--rpt_subject_visit_schedule_last_visit
ALTER TABLE rpt_subject_visit_schedule_last_visit ADD CONSTRAINT rpt_subject_visit_schedule_last_visit_pk PRIMARY KEY (studyid, siteid, usubjid, visit, visitnum);
CREATE UNIQUE INDEX rpt_subject_visit_schedule_last_visit_uk1 ON rpt_subject_visit_schedule_last_visit (comprehendid, visit, visitnum);


-- Latest DS
--need to revisit PK and UK as definition of latest disposition may change now that duplicate dsseq values per subject are allowed
--ALTER TABLE rpt_latest_ds add primary key (studyid, siteid, usubjid, dsterm, dsseq, dsstdtc_by_month);
--CREATE UNIQUE INDEX rpt_latest_ds_uk1 ON rpt_latest_ds (comprehendid, dsterm, dsseq, dsstdtc_by_month);
CREATE INDEX rpt_latest_ds_idx1 ON rpt_latest_ds (comprehendid);
CREATE INDEX rpt_latest_ds_idx2 ON rpt_latest_ds (dsstdtc_by_month);
CREATE INDEX rpt_latest_ds_idx3 ON rpt_latest_ds (dscat);
CREATE INDEX rpt_latest_ds_idx4 ON rpt_latest_ds (dsterm);
CREATE INDEX rpt_latest_ds_idx5 ON rpt_latest_ds (dsscat);
CREATE INDEX rpt_latest_ds_idx7 ON rpt_latest_ds (sitename);
CREATE INDEX rpt_latest_ds_idx8 ON rpt_latest_ds (sitecountry);
CREATE INDEX rpt_latest_ds_idx9 ON rpt_latest_ds (siteid);

--rpt_ds
ALTER TABLE rpt_ds ADD CONSTRAINT rpt_ds_pk PRIMARY KEY (studyid, siteid, dsseq, dsterm, dscat, period_starting_date);
CREATE INDEX rpt_ds_idx1 ON rpt_ds (dscat);
CREATE INDEX rpt_ds_idx2 ON rpt_ds (dsterm);
CREATE INDEX rpt_ds_idx3 ON rpt_ds (siteid);



-- rpt_ip_accountability
ALTER TABLE rpt_ip_accountability ADD CONSTRAINT rpt_ip_accountability_pk PRIMARY KEY (studyid, siteid, trunc_ip_month);
CREATE INDEX rpt_ip_accountability_idx1 ON rpt_ip_accountability (comprehendid, siteid, trunc_ip_month);

-- Adverse Events by Relative Trial Week
ALTER TABLE rpt_ae_rel_week ADD PRIMARY KEY (studyid, siteid, usubjid, aeseq);
CREATE UNIQUE INDEX rpt_ae_rel_week_uk1 ON rpt_ae_rel_week (objectuniquekey);

-- rpt_data_entry
ALTER TABLE rpt_data_entry ADD CONSTRAINT rpt_data_entry_pk PRIMARY KEY (comprehendid, trunc_month, isprimaryendpoint);
-- need to verify composite key meets primary lookup usage
-- also may be redundant if pk constraint above creates a b-tree index
CREATE INDEX rpt_data_entry_idx1 ON rpt_data_entry (studyid, trunc_month);

-- Site Issue
ALTER TABLE rpt_view_subj_count_by_last_visit ADD CONSTRAINT rpt_view_subj_count_by_last_visit_pk PRIMARY KEY (comprehendid);
CREATE INDEX rpt_view_subj_count_by_last_visit_idx1 ON rpt_view_subj_count_by_last_visit (comprehendid);

-- rpt_ae_study_baseline
ALTER TABLE rpt_ae_study_baseline ADD CONSTRAINT rpt_ae_study_baseline_pk PRIMARY KEY (objectuniquekey);
CREATE INDEX rpt_ae_study_baseline_idx1 ON rpt_ae_study_baseline (objectuniquekey);

-- Open Query by Age
ALTER TABLE rpt_open_query_age ADD PRIMARY KEY (studyid, siteid, objectuniquekey);
CREATE UNIQUE INDEX rpt_open_query_age_uk1 ON rpt_open_query_age (objectuniquekey);

-- rpt_site_monitoring_visit_record
ALTER TABLE rpt_monitoring_visit_record ADD CONSTRAINT rpt_monitoring_visit_record_pk1 PRIMARY KEY (studyid, siteid, visitname);
CREATE UNIQUE INDEX rpt_monitoring_visit_record_idx1 ON rpt_monitoring_visit_record (comprehendid, visitname);

-- rpt_missing_data
ALTER TABLE rpt_missing_data add primary key (studyid, siteid, usubjid, visit, formseq, formid, fieldid, fieldseq, log_num);
CREATE INDEX rpt_missing_data_idx1 ON rpt_missing_data (issdv);
CREATE INDEX rpt_missing_data_idx2 ON rpt_missing_data (isrequired);
CREATE INDEX rpt_missing_data_idx3 ON rpt_missing_data (isprimaryendpoint);
CREATE INDEX rpt_missing_data_idx4 ON rpt_missing_data (issecondaryendpoint);
CREATE INDEX rpt_missing_data_idx5 ON rpt_missing_data (studyname);

--rpt_enrollment_analytics
ALTER TABLE rpt_enrollment_analytics ADD CONSTRAINT rpt_enrollment_analytics_pk PRIMARY KEY (studyid);
CREATE UNIQUE INDEX rpt_enrollment_analytics_uk1 ON rpt_enrollment_analytics (comprehendid);
CREATE INDEX rpt_enrollment_analytics_idx1 ON rpt_enrollment_analytics (therapeuticarea);
CREATE INDEX rpt_enrollment_analytics_idx2 ON rpt_enrollment_analytics (program);

--rpt_resource_analytics
CREATE INDEX rpt_resource_analytics_idx1 ON rpt_resource_analytics (comprehendid, cro);
CREATE INDEX rpt_resource_analytics_idx2 ON rpt_study_risk_analytics (program);
CREATE INDEX rpt_resource_analytics_idx3 ON rpt_study_risk_analytics (therapeuticarea);

--rpt_sitemilestone
ALTER TABLE rpt_sitemilestone ADD CONSTRAINT rpt_sitemilestone_pk PRIMARY KEY (studyid, siteid, sitecountry, milestoneseq);

--rpt_studymilestone
ALTER TABLE rpt_studymilestone ADD CONSTRAINT rpt_studymilestone_pk PRIMARY KEY (studyid, milestoneseq);

--rpt_study_risk_analytics
ALTER TABLE rpt_study_risk_analytics ADD CONSTRAINT rpt_study_risk_analytics_pk PRIMARY KEY (studyid, siteid);
CREATE INDEX rpt_study_risk_analytics_idx1 ON rpt_study_risk_analytics (comprehendid);
CREATE INDEX rpt_study_risk_analytics_idx2 ON rpt_study_risk_analytics (cro);
CREATE INDEX rpt_study_risk_analytics_idx3 ON rpt_study_risk_analytics (program);
CREATE INDEX rpt_study_risk_analytics_idx4 ON rpt_study_risk_analytics (therapeuticarea);

--rpt_expenditure_analytics
ALTER TABLE rpt_expenditure_analytics ADD CONSTRAINT rpt_expenditure_analytics_pk PRIMARY KEY (studyid, budget_month, expenditure_units);
CREATE UNIQUE INDEX rpt_expenditure_analytics_uk1 ON rpt_expenditure_analytics (comprehendid, budget_month, expenditure_units);
CREATE INDEX rpt_expenditure_analytics_idx1 ON rpt_expenditure_analytics (therapeuticarea);
CREATE INDEX rpt_expenditure_analytics_idx2 ON rpt_expenditure_analytics (program);
CREATE INDEX rpt_expenditure_analytics_idx3 ON rpt_expenditure_analytics (studyname);

--rpt_pivotal_study_analytics
ALTER TABLE rpt_pivotal_study_analytics ADD CONSTRAINT rpt_pivotal_study_analytics_pk PRIMARY KEY (studyid);
CREATE INDEX rpt_pivotal_study_analytics_idx1 ON rpt_pivotal_study_analytics (comprehendid);
CREATE INDEX rpt_pivotal_study_analytics_idx3 ON rpt_pivotal_study_analytics (program);
CREATE INDEX rpt_pivotal_study_analytics_idx4 ON rpt_pivotal_study_analytics (therapeuticarea);

--rpt_pivotal_study_analytics_datapoints
CREATE INDEX rpt_pivotal_study_analytics_datapoints_idx1 ON rpt_pivotal_study_analytics_datapoints (comprehendid);

--rpt_portfolio_oversight
CREATE UNIQUE INDEX rpt_portfolio_oversight_uk1 ON rpt_portfolio_oversight (comprehendid, cro);
CREATE INDEX rpt_portfolio_oversight_idx1 ON rpt_portfolio_oversight (therapeuticarea);
CREATE INDEX rpt_portfolio_oversight_idx2 ON rpt_portfolio_oversight (program);
CREATE INDEX rpt_portfolio_oversight_idx3 ON rpt_portfolio_oversight (studyname);

--rpt_subject_information
ALTER TABLE rpt_subject_information ADD CONSTRAINT rpt_subject_information_pk PRIMARY KEY (studyid, siteid, usubjid);
CREATE UNIQUE INDEX rpt_subject_information_uk1 ON rpt_subject_information (comprehendid);
CREATE INDEX rpt_subject_information_idx1 ON rpt_subject_information (sitecro);

--rpt_site_performance
ALTER TABLE rpt_site_performance ADD CONSTRAINT rpt_site_performance_pk PRIMARY KEY (studyid, siteid);
CREATE UNIQUE INDEX rpt_site_performance_uk1 ON rpt_site_performance (comprehendid);

--rpt_protocol_deviations_per_subject_per_visit
CREATE UNIQUE INDEX rpt_protocol_deviations_per_subject_per_visit_uk1 ON rpt_protocol_deviations_per_subject_per_visit (studyid, siteid, usubjid, sv_visit, sv_visitseq, dv_visit, dv_dvterm, dv_dvseq);
CREATE UNIQUE INDEX rpt_protocol_deviations_per_subject_per_visit_uk2 ON rpt_protocol_deviations_per_subject_per_visit (comprehendid, sv_visit, sv_visitseq, dv_visit, dv_dvterm, dv_dvseq);

--rpt_site_days
CREATE UNIQUE INDEX rpt_site_days_uk1 ON rpt_site_days (studyid, siteid, studystartdate, studycompletiondate, sitecreationdate, siteactivationdate, sitedaysenddt, monthtrunc, site_days_count);

--rpt_pd_rate_by_subject_days
CREATE UNIQUE INDEX rpt_pd_rate_by_subject_days_uk1 ON rpt_pd_rate_by_subject_days (studyid, siteid, usubjid, dvseq, dvterm);
CREATE UNIQUE INDEX rpt_pd_rate_by_subject_days_uk2 ON rpt_pd_rate_by_subject_days (comprehendid, dvseq, dvterm);
CREATE UNIQUE INDEX rpt_pd_rate_by_subject_days_uk3 ON rpt_pd_rate_by_subject_days (objectuniquekey);

--rpt_subject_days
CREATE UNIQUE INDEX rpt_subject_days_uk1 ON rpt_subject_days (studyid, siteid, usubjid, subjectdaystartdt, subjectdaysenddt, thismonth, thismonthsubjectdays);

--dimdate
ALTER TABLE dimdate ADD CONSTRAINT dimdate_pk PRIMARY KEY (date_dim_id);

--dimdisposition
ALTER TABLE dimdisposition ADD CONSTRAINT dimdisposition_pk PRIMARY KEY (studyid, siteid, usubjid, dsseq);

--dimsite
ALTER TABLE dimsite ADD CONSTRAINT dimsite_pk PRIMARY KEY (studyid, siteid);

--dimstudymilestone
ALTER TABLE dimstudymilestone ADD CONSTRAINT dimstudymilestone_pk PRIMARY KEY (studyid, milestoneseq);

--dimsubject
ALTER TABLE dimsubject ADD CONSTRAINT dimsubject_pk PRIMARY KEY (studyid, siteid, usubjid, thismonth);

--dimvisitrequiredfields
ALTER TABLE dimvisitrequiredfields ADD CONSTRAINT dimvisitrequiredfields_pk PRIMARY KEY (studyid, visit, formid);

--factformdata
ALTER TABLE factformdata ADD CONSTRAINT factformdata_pk PRIMARY KEY (studyid, siteid, usubjid, visit, formid, formseq);

--factsiteperformance
ALTER TABLE factsiteperformance ADD CONSTRAINT factsiteperformance_pk PRIMARY KEY (studyid, siteid);

--factsubjectvisit
ALTER TABLE factsubjectvisit ADD CONSTRAINT factsubjectvisit_pk PRIMARY KEY (studyid, siteid, usubjid, visit, visitnum);

--dimstudy
ALTER TABLE dimstudy ADD CONSTRAINT dimstudy_pk PRIMARY KEY (studyid);

--factadverseevents
ALTER TABLE factadverseevents ADD CONSTRAINT factadverseevents_pk PRIMARY KEY (studyid, siteid, usubjid, aeseq);

--factportfoliodaily
ALTER TABLE factportfoliodaily ADD CONSTRAINT factportfoliodaily_pk PRIMARY KEY (date_dim_id, studyid, siteid);

--kpisummary
ALTER TABLE kpisummary ADD CONSTRAINT kpisummary_pk PRIMARY KEY (studyid, siteid);

--dimssitedates
ALTER TABLE dimsitedates ADD CONSTRAINT dimsitedates_pk PRIMARY KEY (studyid, siteid, date_actual);

--rpt_ae_rate_by_subject_days_per_month
ALTER TABLE rpt_ae_rate_by_subject_days_per_month ADD CONSTRAINT rpt_ae_rate_by_subject_days_per_month_pk PRIMARY KEY (studyid, siteid, month_trunc);

--rpt_pd_rate_by_subject_days_per_month
ALTER TABLE rpt_pd_rate_by_subject_days_per_month ADD CONSTRAINT rpt_pd_rate_by_subject_days_per_month_pk PRIMARY KEY (studyid, siteid, month_trunc);

--rpt_oversight_metrics
CREATE UNIQUE INDEX rpt_oversight_metrics_uk1 ON rpt_oversight_metrics (metricid, studyid, siteid);

--rpt_subject_dispostion
ALTER TABLE rpt_subject_disposition ADD CONSTRAINT rpt_subject_disposition_pk PRIMARY KEY (studyid, siteid, usubjid, dsterm, dsseq);

CREATE UNIQUE INDEX rpt_subject_disposition_uk1 ON rpt_subject_disposition USING btree (comprehendid, dsterm, dsseq);

CREATE UNIQUE INDEX rpt_subject_disposition_uk2 ON rpt_subject_disposition USING btree (objectuniquekey);

CREATE INDEX rpt_subject_disposition_idx1 ON rpt_subject_disposition(studyid, study_start_disposition);

-- rpt_portfolio_summary_analytics
ALTER TABLE rpt_portfolio_summary_analytics ADD CONSTRAINT rpt_portfolio_summary_analytics_pk PRIMARY KEY (studyid);

--rpt_country_metrics
CREATE UNIQUE INDEX rpt_country_metrics_uk1 ON rpt_country_metrics (metricid, studyid, sitecountry);

--rpt_site_metrics
CREATE UNIQUE INDEX rpt_site_metrics_uk1 ON rpt_site_metrics (metricid, studyid, siteid);

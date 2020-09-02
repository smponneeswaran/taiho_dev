-- Foreign Keys Presentation Layer Objects
-- serial_proc=0 (enable forced serial processing for DIN Build: 1=enable 0=disable)

ALTER TABLE rpt_query_rate_by_subject_days ADD CONSTRAINT rpt_query_rate_by_subject_days_fk1 FOREIGN KEY(studyid,
 siteid,
 usubjid) REFERENCES subject(studyid,
 siteid,
 usubjid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_data_entry ADD CONSTRAINT rpt_data_entry_fk1 FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid);

ALTER TABLE rpt_query_rate_by_ecrf ADD CONSTRAINT rpt_query_rate_by_ecrf_fk1 FOREIGN KEY(objectuniquekey) REFERENCES query(objectuniquekey) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_ip_accountability ADD CONSTRAINT rpt_ip_accountability_fk1 FOREIGN KEY(comprehendid) REFERENCES site(comprehendid);

ALTER TABLE rpt_subject_visit_schedule ADD CONSTRAINT rpt_subject_visit_schedule_fk1 FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid);

ALTER TABLE rpt_subject_visit_schedule_last_visit ADD CONSTRAINT rpt_subject_visit_schedule_last_visit_fk1 FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid);

ALTER TABLE rpt_query_rate_by_subject_days ADD CONSTRAINT rpt_query_rate_by_subject_days_fk2 FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_query_rate_by_ecrf ADD CONSTRAINT rpt_query_rate_by_ecrf_fk2 FOREIGN KEY(objectuniquekey) REFERENCES query(objectuniquekey) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_latest_ds ADD CONSTRAINT rpt_latest_ds_subject_fk1 FOREIGN KEY(studyid,
 siteid,
 usubjid) REFERENCES subject(studyid,
 siteid,
 usubjid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_latest_ds ADD CONSTRAINT rpt_latest_ds_subject_fk2 FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_latest_ds ADD CONSTRAINT rpt_latest_ds_site_fk1 FOREIGN KEY(studyid,siteid) REFERENCES site(studyid, siteid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_latest_ds ADD CONSTRAINT rpt_latest_ds_site_fk2 FOREIGN KEY(sitekey) REFERENCES site(comprehendid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_latest_ds ADD CONSTRAINT rpt_latest_ds_study_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid) ON
DELETE NO ACTION ON
UPDATE NO ACTION;


ALTER TABLE rpt_ae_rel_week ADD CONSTRAINT rpt_ae_rel_week_fk1 FOREIGN KEY(objectuniquekey) REFERENCES ae(objectuniquekey);

ALTER TABLE rpt_view_subj_count_by_last_visit ADD CONSTRAINT rpt_view_subj_count_by_last_visit FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid);

ALTER TABLE rpt_ae_study_baseline ADD CONSTRAINT rpt_ae_study_baseline_fk1 FOREIGN KEY(objectuniquekey) REFERENCES ae(objectuniquekey);

ALTER TABLE rpt_open_query_age ADD CONSTRAINT rpt_open_query_age_fk1 FOREIGN KEY(objectuniquekey) REFERENCES query(objectuniquekey);

ALTER TABLE rpt_monitoring_visit_record ADD CONSTRAINT rpt_monitoring_visit_record_fk1 FOREIGN KEY(comprehendid) REFERENCES site(comprehendid);

ALTER TABLE rpt_monitoring_visit_record ADD CONSTRAINT rpt_monitoring_visit_record_fk2 FOREIGN KEY(studyid, siteid, visitname) REFERENCES sitemonitoringvisitschedule(studyid, siteid, visitname);

ALTER TABLE rpt_resource_analytics ADD CONSTRAINT rpt_resource_analytics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_sitemilestone ADD CONSTRAINT rpt_site_milestone_fk1 FOREIGN KEY(comprehendid) REFERENCES site(comprehendid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_studymilestone ADD CONSTRAINT rpt_study_milestone_fk1 FOREIGN KEY(comprehendid) REFERENCES study(comprehendid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_study_risk_analytics ADD CONSTRAINT rpt_study_risk_analytics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_study_risk_analytics ADD CONSTRAINT rpt_study_risk_analytics_fk2 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_enrollment_analytics ADD CONSTRAINT rpt_enrollment_analytics_fk1 FOREIGN KEY(studyid) REFERENCES study 
ON DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_expenditure_analytics ADD CONSTRAINT rpt_expenditure_analytics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE rpt_pivotal_study_analytics ADD CONSTRAINT rpt_pivotal_study_analytics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_pivotal_study_analytics_datapoints ADD CONSTRAINT rpt_pivotal_study_analytics_datapoints_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_portfolio_oversight ADD CONSTRAINT rpt_portfolio_oversight_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid) 
ON DELETE NO ACTION ON
UPDATE NO ACTION;

ALTER TABLE rpt_subject_information ADD CONSTRAINT rpt_subject_information_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_site_performance ADD CONSTRAINT rpt_site_performance_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION
 ON UPDATE NO ACTION;

ALTER TABLE rpt_protocol_deviations_per_subject_per_visit ADD CONSTRAINT rpt_protocol_deviations_per_subject_per_visit_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_site_days ADD CONSTRAINT rpt_site_days_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_pd_rate_by_subject_days ADD CONSTRAINT rpt_pd_rate_by_subject_days_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_subject_days ADD CONSTRAINT rpt_subject_days_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_subject_days ADD CONSTRAINT rpt_subject_days_fk2 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid) 
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimdisposition ADD CONSTRAINT dimdisposition_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimsite ADD CONSTRAINT dimsite_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimstudymilestone ADD CONSTRAINT dimstudymilestone_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimsubject ADD CONSTRAINT dimsubject_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimvisitrequiredfields ADD CONSTRAINT dimvisitrequiredfields_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE factformdata ADD CONSTRAINT factformdata_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE factsiteperformance ADD CONSTRAINT factsiteperformance_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE factsubjectvisit ADD CONSTRAINT factsubjectvisit_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimstudy ADD CONSTRAINT dimstudy_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE factadverseevents ADD CONSTRAINT factadverseevents_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE factportfoliodaily ADD CONSTRAINT factportfoliodaily_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE kpisummary ADD CONSTRAINT kpisummary_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE dimsitedates ADD CONSTRAINT dimsitedates_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_ae_rate_by_subject_days_per_month ADD CONSTRAINT rpt_ae_rate_by_subject_days_per_month_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_pd_rate_by_subject_days_per_month ADD CONSTRAINT rpt_pd_rate_by_subject_days_per_month_fk1 FOREIGN KEY(studyid, siteid) REFERENCES site(studyid, siteid)
ON DELETE NO ACTION 
ON UPDATE NO ACTION;

ALTER TABLE rpt_oversight_metrics ADD CONSTRAINT rpt_oversight_metrics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE rpt_subject_disposition ADD CONSTRAINT rpt_subject_disposition_fk1 FOREIGN KEY(studyid, siteid, usubjid) REFERENCES subject(studyid, siteid, usubjid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE rpt_subject_disposition ADD CONSTRAINT rpt_subject_disposition_fk2 FOREIGN KEY(comprehendid) REFERENCES subject(comprehendid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE rpt_portfolio_summary_analytics ADD CONSTRAINT rpt_portfolio_summary_analytics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE rpt_country_metrics ADD CONSTRAINT rpt_country_metrics_fk1 FOREIGN KEY(studyid) REFERENCES study(studyid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER TABLE rpt_site_metrics ADD CONSTRAINT rpt_site_metrics_fk1 FOREIGN KEY(comprehendid) REFERENCES site(comprehendid)
ON DELETE NO ACTION
ON UPDATE NO ACTION;

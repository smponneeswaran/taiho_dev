-- Foreign Keys on CCDM
-- serial_proc=0 (enable forced serial processing for DIN Build: 1=enable 0=disable)

ALTER TABLE ae
	ADD CONSTRAINT ae_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE dm
	ADD CONSTRAINT dm_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE dm
	ADD CONSTRAINT dm_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ds
	ADD CONSTRAINT ds_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ds
	ADD CONSTRAINT ds_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE dv
	ADD CONSTRAINT dv_tv_fk2
	FOREIGN KEY(studyid, visit)
	REFERENCES tv(studyid, visit)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE dv
	ADD CONSTRAINT dv_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE dv
	ADD CONSTRAINT dv_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE dvsite
    ADD CONSTRAINT dvs_site_fk1
    FOREIGN KEY(studyid,siteid)
    REFERENCES site (studyid,siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION;

ALTER TABLE fielddata
	ADD CONSTRAINT fielddata_tv_fk3
	FOREIGN KEY(studyid, visit)
	REFERENCES tv(studyid, visit)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE fielddata
	ADD CONSTRAINT fielddata_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE fielddata
	ADD CONSTRAINT fielddata_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE fielddata
	ADD CONSTRAINT fielddata_formddata_fk2
	FOREIGN KEY(studyid, siteid, usubjid, visit, visitseq, formid, formseq)
	REFERENCES formdata(studyid, siteid, usubjid, visit, visitseq, formid, formseq)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE fielddata
	ADD CONSTRAINT fielddata_fielddef_fk4
	FOREIGN KEY(studyid, formid, fieldid)
	REFERENCES fielddef(studyid, formid, fieldid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE fielddef
	ADD CONSTRAINT fielddef_study_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES study(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE fielddef
	ADD CONSTRAINT fielddef_formdef_fk1
	FOREIGN KEY(studyid, formid)
	REFERENCES formdef(studyid, formid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdata
	ADD CONSTRAINT formdata_tv_fk1
	FOREIGN KEY(studyid, visit)
	REFERENCES tv(studyid, visit)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdata
	ADD CONSTRAINT formdata_sv_fk2
	FOREIGN KEY(studyid, siteid, usubjid, visit, visitseq)
	REFERENCES sv(studyid, siteid, usubjid, visit, visitseq)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdata
	ADD CONSTRAINT formdata_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdata
	ADD CONSTRAINT formdata_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdata
	ADD CONSTRAINT formdata_formdef_fk1
	FOREIGN KEY(studyid, formid)
	REFERENCES formdef(studyid, formid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdef
	ADD CONSTRAINT formdef_study_fk1
	FOREIGN KEY(studyid)
	REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE formdef
	ADD CONSTRAINT formddef_study_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES study(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ie
	ADD CONSTRAINT ie_subject_fk3
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ie
	ADD CONSTRAINT ie_subject_fk2
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ie
	ADD CONSTRAINT ie_site_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ipaccountability
	ADD CONSTRAINT ipaccountability_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES site(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE ipaccountability
	ADD CONSTRAINT ipaccountability_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE query
	ADD CONSTRAINT query_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE query
	ADD CONSTRAINT query_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE site
	ADD CONSTRAINT site_study_fk2
	FOREIGN KEY(studyid)
	REFERENCES study(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE site
	ADD CONSTRAINT site_study_fk
	FOREIGN KEY(studyid)
	REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE site
	ADD CONSTRAINT site_studycro_fk
	FOREIGN KEY(studyid, croid)
	REFERENCES studycro(studyid, croid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE siteissue
	ADD CONSTRAINT siteissue_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES site(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE siteissue
	ADD CONSTRAINT siteissue_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemilestone
	ADD CONSTRAINT sitemilestone_site_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES site(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemilestone
	ADD CONSTRAINT sitemilestone_site_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemonitoringvisit
	ADD CONSTRAINT sitemonitoringvisit_site_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES site(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemonitoringvisit
	ADD CONSTRAINT sitemonitoringvisit_site_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemonitoringvisitschedule
	ADD CONSTRAINT sitemonitoringvisitschedule_site_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES site(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemonitoringvisitschedule
	ADD CONSTRAINT sitemonitoringvisitschedule_site_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE studymilestone
	ADD CONSTRAINT studymilestone_study_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES study(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE studymilestone
	ADD CONSTRAINT studymilestone_study_fk1
	FOREIGN KEY(studyid)
	REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE subject
	ADD CONSTRAINT subject_site_fk2
	FOREIGN KEY(sitekey)
	REFERENCES site(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE subject
	ADD CONSTRAINT subject_site_fk
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sv
	ADD CONSTRAINT sv_tv_fk1
	FOREIGN KEY(studyid, visit)
	REFERENCES tv(studyid, visit)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sv
	ADD CONSTRAINT sv_subject_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sv
	ADD CONSTRAINT sv_subject_fk1
	FOREIGN KEY(studyid, siteid, usubjid)
	REFERENCES subject(studyid, siteid, usubjid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE tv
	ADD CONSTRAINT tv_study_fk1
	FOREIGN KEY(studyid)
	REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE tv
	ADD CONSTRAINT tv_study_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES study(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE visitform
	ADD CONSTRAINT visitform_tv_fk1
	FOREIGN KEY(studyid, visit)
	REFERENCES tv(studyid, visit)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE visitform
	ADD CONSTRAINT visitform_study_fk2
	FOREIGN KEY(comprehendid)
	REFERENCES study(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE visitform
	ADD CONSTRAINT visitform_study_fk1
	FOREIGN KEY(studyid)
	REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE visitform
	ADD CONSTRAINT visitform_formdef_fk1
	FOREIGN KEY(studyid, formid)
	REFERENCES formdef(studyid, formid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE lb
    ADD CONSTRAINT lb_fk1
    FOREIGN KEY (comprehendid)
    REFERENCES subject(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE lb
    ADD CONSTRAINT lb_fk2
    FOREIGN KEY (studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE lb
    ADD CONSTRAINT lb_fk3
    FOREIGN KEY (studyid, siteid)
    REFERENCES site(studyid, siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE studyplannedstatistic
    ADD CONSTRAINT studyplannedstatistic_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES study(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteplannedstatistic
    ADD CONSTRAINT siteplannedstatistic_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES site(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteplannedstatistic
    ADD CONSTRAINT siteplannedstatistic_fk2
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteresource
    ADD CONSTRAINT siteresource_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES site(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteresource
    ADD CONSTRAINT siteresource_fk2
    FOREIGN KEY(studyid, siteid)
    REFERENCES site(studyid, siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE cm
    ADD CONSTRAINT cm_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES subject(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE cm
    ADD CONSTRAINT cm_fk2
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE cm
    ADD CONSTRAINT cm_fk3
    FOREIGN KEY(studyid, siteid)
    REFERENCES site(studyid, siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteplannedenrollment
    ADD CONSTRAINT siteplannedenrollment_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES site(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteplannedenrollment
    ADD CONSTRAINT siteplannedenrollment_fk2
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteplannedexpenditure
    ADD CONSTRAINT siteplannedexpenditure_fk1
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteplannedexpenditure
    ADD CONSTRAINT siteplannedexpenditure_fk2
    FOREIGN KEY(studyid, siteid)
    REFERENCES site(studyid, siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE ex
	ADD CONSTRAINT ex_subject_fk1
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE studycro

	ADD CONSTRAINT studycro_study_fk1
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE studyplannedrecruitment
    ADD CONSTRAINT studyplannedrecruitment_study_fk1
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE mh
	ADD CONSTRAINT mh_subject_fk1
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE studyplannedrecruitment
    ADD CONSTRAINT studyplannedrecruitment_study_fk2
    FOREIGN KEY(comprehendid)
    REFERENCES study(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE qs 
    ADD CONSTRAINT qs_subject_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES subject(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;
 
ALTER TABLE vs 
  ADD CONSTRAINT vs_subject_fk1
  FOREIGN KEY(comprehendid)
  REFERENCES subject(comprehendid)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION ;

ALTER TABLE eg
    ADD CONSTRAINT eg_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES subject(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE eg
    ADD CONSTRAINT eg_fk2
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE eg
    ADD CONSTRAINT eg_fk3
    FOREIGN KEY(studyid, siteid)
    REFERENCES site(studyid, siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION;

ALTER TABLE pe
    ADD CONSTRAINT pe_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES subject(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE pe
    ADD CONSTRAINT pe_fk2
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE pe
    ADD CONSTRAINT pe_fk3
    FOREIGN KEY(studyid, siteid)
    REFERENCES site(studyid, siteid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE rslt 
	ADD CONSTRAINT rslt_subject_fk1
	FOREIGN KEY(comprehendid)
	REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE sitemonitoringvisitreport
	ADD CONSTRAINT sitemonitoringvisitreport_site_fk1
	FOREIGN KEY(studyid, siteid)
	REFERENCES site(studyid, siteid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE comprehendcustommetric
    ADD CONSTRAINT comprehendcustommetric_fk1
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE pc
    ADD CONSTRAINT pc_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE pp
    ADD CONSTRAINT pp_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES subject(comprehendid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE tsdv
    ADD CONSTRAINT tsdv_fk1
    FOREIGN KEY(studyid)
    REFERENCES study(studyid)
	ON DELETE NO ACTION
	ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituredata
    ADD CONSTRAINT siteexpendituredata_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES site(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituredata
    ADD CONSTRAINT siteexpendituredata_fk2
    FOREIGN KEY(event_category_id)
    REFERENCES app.cost_category(event_category_id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituredata
    ADD CONSTRAINT siteexpendituredata_fk3
    FOREIGN KEY(event_category_name)
    REFERENCES app.cost_category(event_category_name)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituredata
    ADD CONSTRAINT siteexpendituredata_fk4
    FOREIGN KEY(event_subcategory_id)
    REFERENCES app.cost_subcategory(event_subcategory_id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituresummary
    ADD CONSTRAINT siteexpendituresummary_fk1
    FOREIGN KEY(comprehendid)
    REFERENCES site(comprehendid)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituresummary
    ADD CONSTRAINT siteexpendituresummary_fk2
    FOREIGN KEY(event_category_id)
    REFERENCES app.cost_category(event_category_id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

ALTER TABLE siteexpendituresummary
    ADD CONSTRAINT siteexpendituresummary_fk3
    FOREIGN KEY(event_category_name)
    REFERENCES app.cost_category(event_category_name)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION ;

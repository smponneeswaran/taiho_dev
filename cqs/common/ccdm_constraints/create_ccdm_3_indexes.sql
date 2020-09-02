-- Create CQS Indexes on CCDM

CREATE INDEX ae_idx1
	ON ae USING btree (comprehendid);


CREATE INDEX ae_idx2
	ON ae USING btree (aestdtc);


CREATE INDEX ae_idx3
	ON ae USING btree (aeendtc);


CREATE INDEX ae_idx4
	ON ae USING btree (aeterm);


CREATE INDEX ae_idx5
	ON ae USING btree (aesev);


CREATE INDEX ae_idx6
	ON ae USING btree (aeser);


CREATE INDEX ae_idx7
	ON ae USING btree (aerelnst);


ALTER TABLE ae ADD CONSTRAINT ae_pkey
	PRIMARY KEY (studyid, siteid, usubjid, aeseq);


CREATE UNIQUE INDEX ae_uk1
	ON ae USING btree (comprehendid, aeseq);


CREATE UNIQUE INDEX ae_uk2
    ON ae USING btree (objectuniquekey);

CREATE INDEX comprehendtermmap_idx1
	ON comprehendtermmap USING btree (tablename, columnname);


ALTER TABLE comprehendtermmap ADD CONSTRAINT comprehendtermmap_pkey
	PRIMARY KEY (tablename, columnname, studyid, comprehendterm);


CREATE INDEX dm_idx1
	ON dm USING btree (dmdtc);


ALTER TABLE dm ADD CONSTRAINT dm_pkey
	PRIMARY KEY (studyid, siteid, usubjid);


CREATE UNIQUE INDEX dm_uk1
	ON dm USING btree (comprehendid, visitnum, visit);

CREATE UNIQUE INDEX dm_uk2
    ON dm USING btree (objectuniquekey);

CREATE INDEX ds_idx1
	ON ds USING btree (comprehendid);


CREATE INDEX ds_idx2
	ON ds USING btree (dsstdtc);


CREATE INDEX ds_idx3
	ON ds USING btree (dscat);


CREATE INDEX ds_idx4
	ON ds USING btree (dsterm);


CREATE INDEX ds_idx5
	ON ds USING btree (dsscat);


ALTER TABLE ds ADD CONSTRAINT ds_pkey
	PRIMARY KEY (studyid, siteid, usubjid, dsterm);


CREATE UNIQUE INDEX ds_uk1
	ON ds USING btree (comprehendid, dsterm);

CREATE UNIQUE INDEX ds_uk2
    ON ds USING btree (objectuniquekey);

CREATE INDEX dv_idx1
	ON dv USING btree (comprehendid);


CREATE INDEX dv_idx2
	ON dv USING btree (dvcat);


CREATE INDEX dv_idx3
	ON dv USING btree (dvterm);


CREATE INDEX dv_idx4
	ON dv USING btree (dvstdtc);


CREATE INDEX dv_idx5
	ON dv USING btree (dvscat);


ALTER TABLE dv ADD CONSTRAINT dv_pkey
	PRIMARY KEY (studyid, siteid, usubjid, dvseq);


CREATE UNIQUE INDEX dv_uk1
	ON dv USING btree (comprehendid, visit, formid, dvseq);

CREATE UNIQUE INDEX dv_uk2
    ON dv USING btree (objectuniquekey);

CREATE UNIQUE INDEX dvs_uk1
    ON dvsite USING btree (comprehendid,dvseq);


CREATE UNIQUE INDEX dvs_uk2
    ON dvsite USING btree (objectuniquekey);

CREATE INDEX dvs_idx1
    ON dvsite USING btree (dvcat);


ALTER TABLE dvsite ADD CONSTRAINT dvs_pkey
    PRIMARY KEY (dvseq,siteid,studyid);

ALTER TABLE fielddata ADD CONSTRAINT fielddata_pkey
	PRIMARY KEY (studyid, siteid, usubjid, visit, visitseq, formid, formseq, fieldid, fieldseq, log_num);


CREATE UNIQUE INDEX fielddata_uk1
    ON fielddata USING btree (comprehendid, visit, formid, formseq, fieldid, fieldseq, log_num);

CREATE UNIQUE INDEX fielddata_uk2
    ON fielddata USING btree (objectuniquekey);

CREATE INDEX fielddef_idx1
	ON fielddef USING btree (comprehendid);


CREATE INDEX fielddef_idx2
	ON fielddef USING btree (formid);


CREATE INDEX fielddef_idx3
	ON fielddef USING btree (fieldid);


CREATE INDEX fielddef_idx4
	ON fielddef USING btree (issdv);


CREATE INDEX fielddef_idx5
	ON fielddef USING btree (isrequired);


CREATE INDEX fielddef_idx6
	ON fielddef USING btree (isprimaryendpoint);


CREATE INDEX fielddef_idx7
	ON fielddef USING btree (issecondaryendpoint);

CREATE INDEX fielddef_idx8
	ON fielddef USING btree (studyid, formid);

ALTER TABLE fielddef ADD CONSTRAINT fielddef_pkey
	PRIMARY KEY (studyid, formid, fieldid);


CREATE UNIQUE INDEX fielddef_uk1
	ON fielddef USING btree (comprehendid, formid, fieldid);

CREATE UNIQUE INDEX fielddef_uk2
    ON fielddef USING btree (objectuniquekey);

CREATE UNIQUE INDEX fielddef_uk3
    ON fielddef USING btree (studyid, formid, fieldid);

ALTER TABLE formdata ADD CONSTRAINT formdata_pkey
	PRIMARY KEY (studyid, siteid, usubjid, visit, visitseq, formid, formseq);


CREATE UNIQUE INDEX formdata_uk1
	ON formdata USING btree (comprehendid, visit, visitseq, formid, formseq);

CREATE UNIQUE INDEX formdata_uk2
    ON formdata USING btree (objectuniquekey);

CREATE INDEX formdata_idx1
    ON formdata USING btree (comprehendid, formid, visit, visitseq);

CREATE INDEX formdef_idx1
	ON formdef USING btree (comprehendid);


CREATE INDEX formdef_idx2
	ON formdef USING btree (formid);


CREATE INDEX formdef_idx3
	ON formdef USING btree (formname);


CREATE INDEX formdef_idx4
	ON formdef USING btree (issdv);


CREATE INDEX formdef_idx5
	ON formdef USING btree (isprimaryendpoint);


CREATE INDEX formdef_idx6
	ON formdef USING btree (issecondaryendpoint);

CREATE INDEX formdef_idx7
    ON formdef USING btree (studyid, formid);

ALTER TABLE formdef ADD CONSTRAINT formdef_pkey
	PRIMARY KEY (studyid, formid);


CREATE UNIQUE INDEX formdef_uk
	ON formdef USING btree (comprehendid, formid);


CREATE UNIQUE INDEX formdef_uk2
	ON formdef USING btree (studyid, formname);


CREATE UNIQUE INDEX formdef_uk3
	ON formdef USING btree (comprehendid, formname);

CREATE UNIQUE INDEX formdef_uk4
    ON formdef USING btree (objectuniquekey);

CREATE INDEX ie_idx1
	ON ie USING btree (visit, visitnum);


CREATE INDEX ie_idx2
	ON ie USING btree (ietest);


CREATE INDEX ie_idx3
	ON ie USING btree (ietestcd);


CREATE INDEX ie_idx4
	ON ie USING btree (iecat);


ALTER TABLE ie ADD CONSTRAINT ie_pkey
	PRIMARY KEY (studyid, siteid, usubjid, ieseq);


CREATE UNIQUE INDEX ie_uk1
	ON ie USING btree (comprehendid, ieseq);

CREATE UNIQUE INDEX ie_uk2
    ON ie USING btree (objectuniquekey);

ALTER TABLE ipaccountability ADD CONSTRAINT ipaccountability_pk
	PRIMARY KEY (studyid, siteid, ipname, ipdate, ipseq, ipstate);

CREATE UNIQUE INDEX ipaccountability_uk1
	ON ipaccountability USING btree (comprehendid, ipname, ipdate, ipseq, ipstate);

CREATE UNIQUE INDEX ipaccountability_uk2
    ON ipaccountability USING btree (objectuniquekey);

CREATE INDEX query_idx1
	ON query USING btree (querytype);


CREATE INDEX query_idx2
	ON query USING btree (formid);


CREATE INDEX query_idx3
	ON query USING btree (fieldid);


CREATE INDEX query_idx4
	ON query USING btree (querystatus);


CREATE INDEX query_idx5
	ON query USING btree (queryopeneddate);


CREATE INDEX query_idx6
	ON query USING btree (querycloseddate);


CREATE INDEX query_idx7
	ON query USING btree (queryresponsedate);


CREATE INDEX query_idx8
	ON query USING btree (comprehendid);


ALTER TABLE query ADD CONSTRAINT query_pkey
	PRIMARY KEY (studyid, siteid, usubjid, queryid);


CREATE UNIQUE INDEX query_uk1
	ON query USING btree (comprehendid, queryid);

CREATE UNIQUE INDEX query_uk2
    ON query USING btree (objectuniquekey);

CREATE INDEX site_idx1
	ON site USING btree (sitename);


CREATE INDEX site_idx2
	ON site USING btree (sitecountry);


CREATE INDEX site_idx3
	ON site USING btree (siteregion);


ALTER TABLE site ADD CONSTRAINT site_pkey
	PRIMARY KEY (studyid, siteid);


CREATE UNIQUE INDEX site_uk1
	ON site USING btree (comprehendid);

CREATE INDEX siteissue_idx1
	ON siteissue USING btree (issuetype);


CREATE INDEX siteissue_idx2
	ON siteissue USING btree (issuestatus);


CREATE INDEX siteissue_idx3
	ON siteissue USING btree (issueopeneddate);


CREATE INDEX siteissue_idx4
	ON siteissue USING btree (issuecloseddate);


CREATE INDEX siteissue_idx5
	ON siteissue USING btree (issueresponsedate);


ALTER TABLE siteissue ADD CONSTRAINT siteissue_pkey
	PRIMARY KEY (studyid, siteid, issueid);


CREATE UNIQUE INDEX siteissue_uk
	ON siteissue USING btree (comprehendid, issueid);

CREATE UNIQUE INDEX siteissue_uk1
    ON siteissue USING btree (objectuniquekey);

CREATE INDEX sitemilestone_idx1
	ON sitemilestone USING btree (comprehendid);


CREATE INDEX sitemilestone_idx2
	ON sitemilestone USING btree (milestonetype);


CREATE INDEX sitemilestone_idx3
	ON sitemilestone USING btree (milestonelabel);


CREATE INDEX sitemilestone_idx4
	ON sitemilestone USING btree (ismandatory);


ALTER TABLE sitemilestone ADD CONSTRAINT sitemilestone_pkey
	PRIMARY KEY (studyid, siteid, milestonelabel, milestonetype);


CREATE UNIQUE INDEX sitemilestone_uk
	ON sitemilestone USING btree (comprehendid, milestonelabel, milestonetype);


CREATE UNIQUE INDEX sitemilestone_uk1
	ON sitemilestone USING btree (comprehendid, milestonelabel, milestonetype);

CREATE UNIQUE INDEX sitemilestone_uk2
    ON sitemilestone USING btree (objectuniquekey);

CREATE INDEX sitemonitoringvisit_idx1
	ON sitemonitoringvisit USING btree (comprehendid);


ALTER TABLE sitemonitoringvisit ADD CONSTRAINT sitemonitoringvisit_pkey
	PRIMARY KEY (studyid, siteid, visitname);


CREATE UNIQUE INDEX sitemonitoringvisit_uk1
	ON sitemonitoringvisit USING btree (comprehendid, visitname, visitdate);

CREATE UNIQUE INDEX sitemonitoringvisit_uk2
    ON sitemonitoringvisit USING btree (objectuniquekey);

CREATE INDEX sitemonitoringvisitschedule_idx1
	ON sitemonitoringvisitschedule USING btree (comprehendid);


CREATE INDEX sitemonitoringvisitschedule_idx2
	ON sitemonitoringvisitschedule USING btree (visitname);


ALTER TABLE sitemonitoringvisitschedule ADD CONSTRAINT sitemonitoringvisitschedule_pkey
	PRIMARY KEY (studyid, siteid, visitname);


CREATE UNIQUE INDEX sitemonitoringvisitschedule_uk1
	ON sitemonitoringvisitschedule USING btree (comprehendid, visitname, plannedvisitdate);

CREATE UNIQUE INDEX sitemonitoringvisitschedule_uk2
    ON sitemonitoringvisitschedule USING btree (objectuniquekey);

CREATE INDEX study_idx1
	ON study USING btree (studyname);


ALTER TABLE study ADD CONSTRAINT study_pkey
	PRIMARY KEY (studyid);


CREATE UNIQUE INDEX study_uk1
	ON study USING btree (comprehendid);


CREATE INDEX studymilestone_idx1
	ON studymilestone USING btree (comprehendid);


CREATE INDEX studymilestone_idx2
	ON studymilestone USING btree (milestonelabel);


CREATE INDEX studymilestone_idx3
	ON studymilestone USING btree (milestonetype);


CREATE INDEX studymilestone_idx4
	ON studymilestone USING btree (iscriticalpath);


ALTER TABLE studymilestone ADD CONSTRAINT studymilestone_pkey
	PRIMARY KEY (studyid, milestonelabel, milestonetype);


CREATE UNIQUE INDEX studymilestone_uk1
	ON studymilestone USING btree (comprehendid, milestonelabel, milestonetype);

CREATE UNIQUE INDEX studymilestone_uk2
    ON studymilestone USING btree (objectuniquekey);


CREATE INDEX subject_idx1
	ON subject USING btree (sitekey);


CREATE INDEX subject_idx2
	ON subject USING btree (exitdate);


ALTER TABLE subject ADD CONSTRAINT subject_pkey
	PRIMARY KEY (studyid, siteid, usubjid);


CREATE UNIQUE INDEX subject_uk1
	ON subject USING btree (comprehendid);

CREATE INDEX sv_idx1
	ON sv USING btree (studyid, siteid);

ALTER TABLE sv ADD CONSTRAINT sv_pkey
	PRIMARY KEY (studyid, siteid, usubjid, visit, visitseq);


CREATE UNIQUE INDEX sv_uk1
	ON sv USING btree (comprehendid, visitnum, visit, visitseq);


CREATE UNIQUE INDEX sv_uk3
    ON sv USING btree (objectuniquekey);

CREATE INDEX tv_idx1
	ON tv USING btree (comprehendid);


CREATE INDEX tv_idx2
	ON tv USING btree (visitnum);


CREATE INDEX tv_idx3
	ON tv USING btree (visit);


ALTER TABLE tv ADD CONSTRAINT tv_pkey
	PRIMARY KEY (studyid, visit);


CREATE UNIQUE INDEX tv_uk1
	ON tv USING btree (comprehendid, visitnum, visit);


CREATE UNIQUE INDEX tv_uk2
	ON tv USING btree (studyid, visit);

CREATE UNIQUE INDEX tv_uk3
    ON tv USING btree (objectuniquekey);

CREATE INDEX visitform_idx1
	ON visitform USING btree (visit);


CREATE INDEX visitform_idx2
	ON visitform USING btree (formid);


CREATE INDEX visitform_idx3
	ON visitform USING btree (isrequired);


ALTER TABLE visitform ADD CONSTRAINT visitform_pkey
	PRIMARY KEY (studyid, visit, formid);


CREATE UNIQUE INDEX visitform_uk1
	ON visitform USING btree (comprehendid, visitnum, visit, formid);


CREATE UNIQUE INDEX visitform_uk2
	ON visitform USING btree (objectuniquekey);

CREATE INDEX lb_idx1
    ON lb USING btree (comprehendid);

CREATE INDEX lb_idx2
    ON lb USING btree (visit);

CREATE INDEX lb_idx3
    ON lb USING btree (lbdtc);

CREATE INDEX lb_idx4
    ON lb USING btree (lbtestcd);

CREATE INDEX lb_idx5
    ON lb USING btree (lbtest);

CREATE INDEX lb_idx6
    ON lb USING btree (lbcat);

CREATE INDEX lb_idx7
    ON lb USING btree (lbscat);

CREATE INDEX lb_idx8
    ON lb USING btree (lbspec);

CREATE INDEX lb_idx9
    ON lb USING btree (lbmethod);

CREATE INDEX lb_idx10
    ON lb USING btree (lbstat);

CREATE INDEX lb_idx11
    ON lb USING btree (lbreasnd);

CREATE INDEX lb_idx12
    ON lb USING btree (lborresu);

ALTER TABLE lb ADD CONSTRAINT lb_pkey
	PRIMARY KEY (studyid, siteid, usubjid, lbseq);

CREATE UNIQUE INDEX lb_uk1
    ON lb USING btree (comprehendid, lbseq);

CREATE UNIQUE INDEX lb_uk2
    ON lb USING btree (objectuniquekey);

CREATE INDEX studyplannedstatistic_idx1
    ON studyplannedstatistic USING btree (comprehendid);

CREATE INDEX studyplannedstatistic_idx2
    ON studyplannedstatistic USING btree (statcat);

CREATE INDEX studyplannedstatistic_idx3
    ON studyplannedstatistic USING btree (statsubcat);

ALTER TABLE studyplannedstatistic ADD CONSTRAINT studyplannedstatistic_pkey
	PRIMARY KEY (studyid, statcat);

CREATE UNIQUE INDEX studyplannedstatistic_uk1
    ON studyplannedstatistic USING btree (comprehendid, statcat);

CREATE UNIQUE INDEX studyplannedstatistic_uk2
    ON studyplannedstatistic USING btree (objectuniquekey);

CREATE INDEX siteplannedstatistic_idx1
    ON siteplannedstatistic USING btree (comprehendid);

CREATE INDEX siteplannedstatistic_idx2
    ON siteplannedstatistic USING btree (statcat);

CREATE INDEX siteplannedstatistic_idx3
    ON siteplannedstatistic USING btree (statsubcat);

ALTER TABLE siteplannedstatistic ADD CONSTRAINT siteplannedstatistic_pkey
	PRIMARY KEY (studyid, siteid, statcat);

CREATE UNIQUE INDEX siteplannedstatistic_uk1
    ON siteplannedstatistic USING btree (comprehendid, statcat);

CREATE UNIQUE INDEX siteplannedstatistic_uk2
    ON siteplannedstatistic USING btree (objectuniquekey);

CREATE INDEX siteresource_idx1
    ON siteresource USING btree (comprehendid);

CREATE INDEX siteresource_idx2
    ON siteresource USING btree (resourceType);

CREATE INDEX siteresource_idx3
    ON siteresource USING btree (resourceName);

CREATE INDEX siteresource_idx4
    ON siteresource USING btree (resourceStDtc);

CREATE INDEX siteresource_idx5
    ON siteresource USING btree (resourceEndDtc);

ALTER TABLE siteresource ADD CONSTRAINT siteresource_pkey
	PRIMARY KEY (studyid, siteid, resourceseq);

CREATE UNIQUE INDEX siteresource_uk1
    ON siteresource USING btree (comprehendid, resourceseq);

CREATE UNIQUE INDEX siteresource_uk2
    ON siteresource USING btree (objectuniquekey);

CREATE INDEX cm_idx1
    ON cm using btree (comprehendid);

CREATE INDEX cm_idx2
    ON cm using btree (cmtrt);

CREATE INDEX cm_idx3
    ON cm using btree (cmmodify);

CREATE INDEX cm_idx4
    ON cm using btree (cmdecod);

CREATE INDEX cm_idx5
    ON cm using btree (cmcat);

CREATE INDEX cm_idx6
    ON cm using btree (cmscat);

CREATE INDEX cm_idx7
    ON cm using btree (cmindc);

CREATE INDEX cm_idx8
    ON cm using btree (cmstdtc);

CREATE INDEX cm_idx9
    ON cm using btree (cmendtc);

ALTER TABLE cm ADD CONSTRAINT cm_pkey
	PRIMARY KEY (studyid, siteid, usubjid, cmseq);

CREATE UNIQUE INDEX cm_uk1
    ON cm USING btree (comprehendid, cmseq);

CREATE UNIQUE INDEX cm_uk2
    ON cm USING btree (objectuniquekey);

CREATE INDEX siteplannedexpenditure_idx1
    ON siteplannedexpenditure USING btree (comprehendid);

CREATE INDEX siteplannedexpenditure_idx2
    ON siteplannedexpenditure USING btree (expcat);

CREATE INDEX siteplannedexpenditure_idx3
    ON siteplannedexpenditure USING btree (expdtc);

ALTER TABLE siteplannedexpenditure ADD CONSTRAINT siteplannedexpenditure_pkey
	PRIMARY KEY (studyid, siteid, exptype, expcat, expseq);

CREATE UNIQUE INDEX siteplannedexpenditure_uk1
    ON siteplannedexpenditure USING btree (comprehendid, exptype, expcat, expseq);

CREATE UNIQUE INDEX siteplannedexpenditure_uk2
    ON siteplannedexpenditure USING btree (objectuniquekey);

CREATE INDEX siteplannedenrollment_idx1
    ON siteplannedenrollment USING btree (comprehendid);

ALTER TABLE siteplannedenrollment ADD CONSTRAINT siteplannedenrollment_pkey
	PRIMARY KEY (studyid, siteid, frequency, enddate, enrollmenttype);

CREATE UNIQUE INDEX siteplannedenrollment_uk1
    ON siteplannedenrollment USING btree (comprehendid, frequency, enddate, enrollmenttype);

CREATE UNIQUE INDEX siteplannedenrollment_uk2
    ON siteplannedenrollment USING btree (objectuniquekey);

ALTER TABLE ex ADD CONSTRAINT ex_pkey
	PRIMARY KEY (studyid, siteid, usubjid, exseq);

CREATE UNIQUE INDEX ex_uk1
    ON ex USING btree (objectuniquekey);

ALTER TABLE studycro ADD CONSTRAINT studycro_pkey
	PRIMARY KEY (studyid, croid);

CREATE UNIQUE INDEX studycro_uk1
    ON studycro USING btree (objectuniquekey);

ALTER TABLE mh ADD CONSTRAINT mh_pkey
	PRIMARY KEY (studyid, siteid, usubjid, mhseq);

CREATE UNIQUE INDEX mh_uk1
    ON mh USING btree (objectuniquekey);

CREATE INDEX studyplannedrecruitment_idx1
    ON studyplannedrecruitment USING btree (comprehendid);

ALTER TABLE studyplannedrecruitment ADD CONSTRAINT studyplannedrecruitment_pkey
	PRIMARY KEY (studyid, category, frequency, enddate, type);

CREATE UNIQUE INDEX studyplannedrecruitment_uk1
    ON studyplannedrecruitment USING btree (comprehendid, category, frequency, enddate, type);

CREATE UNIQUE INDEX studyplannedrecruitment_uk2
    ON studyplannedrecruitment USING btree (objectuniquekey);


CREATE INDEX qs_idx1
    ON qs USING btree (comprehendid);

ALTER TABLE qs ADD CONSTRAINT qs_pkey
	PRIMARY KEY (studyid, siteid, usubjid, qsseq);

CREATE UNIQUE INDEX qs_uk1
    ON qs USING btree (objectuniquekey);

CREATE INDEX vs_idx1
    ON vs USING btree (comprehendid);

ALTER TABLE vs ADD CONSTRAINT vs_pkey
	PRIMARY KEY (studyid, siteid, usubjid, vsseq);

CREATE UNIQUE INDEX vs_uk1
    ON vs USING btree (objectuniquekey);

CREATE INDEX eg_idx1
    ON eg using btree (comprehendid);

ALTER TABLE eg ADD CONSTRAINT eg_pkey
	PRIMARY KEY (studyid, siteid, usubjid, egseq);

CREATE UNIQUE INDEX eg_uk1
    ON eg USING btree (comprehendid, egseq);

CREATE UNIQUE INDEX eg_uk2
    ON eg USING btree (objectuniquekey);

CREATE INDEX pe_idx1
    ON pe using btree (comprehendid);

ALTER TABLE pe ADD CONSTRAINT pe_pkey
	PRIMARY KEY (studyid, siteid, usubjid, peseq);

CREATE UNIQUE INDEX pe_uk1
    ON pe USING btree (comprehendid, peseq);

CREATE UNIQUE INDEX pe_uk2
    ON pe USING btree (objectuniquekey);


ALTER TABLE pc ADD CONSTRAINT pc_pkey
	PRIMARY KEY (studyid, siteid, usubjid, pcseq);

CREATE UNIQUE INDEX pc_uk1
    ON pc USING btree (comprehendid, pcseq);

CREATE UNIQUE INDEX pc_uk2
    ON pc USING btree (objectuniquekey);


ALTER TABLE pp ADD CONSTRAINT pp_pkey
	PRIMARY KEY (studyid, siteid, usubjid, ppseq);

CREATE UNIQUE INDEX pp_uk1
    ON pp USING btree (comprehendid, ppseq);

CREATE UNIQUE INDEX pp_uk2
    ON pp USING btree (objectuniquekey);

ALTER TABLE rslt ADD CONSTRAINT rslt_pkey
	PRIMARY KEY (studyid, siteid, usubjid, rsltseq);

CREATE UNIQUE INDEX rslt_uk1
    ON rslt USING btree (objectuniquekey);

CREATE INDEX sitemonitoringvisitreport_idx1
	ON sitemonitoringvisitreport USING btree (comprehendid);

ALTER TABLE sitemonitoringvisitreport ADD CONSTRAINT sitemonitoringvisitreport_pkey
	PRIMARY KEY (studyid, siteid, visitname);

CREATE UNIQUE INDEX sitemonitoringvisitreport_uk1
	ON sitemonitoringvisitreport USING btree (comprehendid, visitname);

CREATE UNIQUE INDEX sitemonitoringvisitreport_uk2
    ON sitemonitoringvisitreport USING btree (objectuniquekey);

CREATE UNIQUE INDEX comprehendcustommetric_uk1
    ON comprehendcustommetric USING btree(metricid, studyid, siteid);

CREATE UNIQUE INDEX tsdv_uk1
    ON tsdv USING btree (studyid, sdvtier, formid, fieldid, visit);

CREATE INDEX tsdv_idx1
    ON tsdv USING btree (studyid, sdvtier, formid, fieldid);

CREATE INDEX dq_audit_idx1 
	ON dq_audit USING btree (studyid, source_system, src_schema, src_table, dq_run_id);	
	
CREATE INDEX dq_bad_data_idx1 
	ON dq_bad_data USING btree (studyid, source_system, src_schema, src_table, dq_run_id);	
	
ALTER TABLE tva ADD CONSTRAINT tva_pkey
	PRIMARY KEY (studyid, armcd);
	
ALTER TABLE se ADD CONSTRAINT se_pkey
	PRIMARY KEY (studyid, siteid, usubjid, seseq, sestdtc, sestdat);
	
ALTER TABLE ta ADD CONSTRAINT ta_pkey
	PRIMARY KEY (studyid, armcd, taetord);

ALTER TABLE te ADD CONSTRAINT te_pkey
	PRIMARY KEY (studyid, etcd);
	



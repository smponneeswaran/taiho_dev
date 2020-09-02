-- Not null script

ALTER TABLE ae ALTER comprehendid SET NOT NULL;
ALTER TABLE ae ALTER studyid SET NOT NULL;
ALTER TABLE ae ALTER siteid SET NOT NULL;
ALTER TABLE ae ALTER usubjid SET NOT NULL;
ALTER TABLE ae ALTER aeseq SET NOT NULL;
ALTER TABLE ae ALTER objectuniquekey SET NOT NULL;


ALTER TABLE ds ALTER studyid SET NOT NULL;
ALTER TABLE ds ALTER siteid SET NOT NULL;
ALTER TABLE ds ALTER usubjid SET NOT NULL;
ALTER TABLE ds ALTER comprehendid SET NOT NULL;
ALTER TABLE ds ALTER dscat SET NOT NULL;
ALTER TABLE ds ALTER dsterm SET NOT NULL;
ALTER TABLE ds ALTER objectuniquekey SET NOT NULL;


ALTER TABLE dv ALTER studyid SET NOT NULL;
ALTER TABLE dv ALTER siteid SET NOT NULL;
ALTER TABLE dv ALTER usubjid SET NOT NULL;
ALTER TABLE dv ALTER comprehendid SET NOT NULL;
ALTER TABLE dv ALTER dvcat SET NOT NULL;
ALTER TABLE dv ALTER dvseq SET NOT NULL;
ALTER TABLE dv ALTER objectuniquekey SET NOT NULL;

ALTER TABLE dvsite ALTER studyid SET NOT NULL;
ALTER TABLE dvsite ALTER siteid SET NOT NULL;
ALTER TABLE dvsite ALTER comprehendid SET NOT NULL;
ALTER TABLE dvsite ALTER dvseq SET NOT NULL;
ALTER TABLE dvsite ALTER objectuniquekey SET NOT NULL;


ALTER TABLE query ALTER studyid SET NOT NULL;
ALTER TABLE query ALTER siteid SET NOT NULL;
ALTER TABLE query ALTER usubjid SET NOT NULL;
ALTER TABLE query ALTER comprehendid SET NOT NULL;
ALTER TABLE query ALTER querystatus SET NOT NULL;
ALTER TABLE query ALTER queryopeneddate SET NOT NULL;
ALTER TABLE query ALTER queryid SET NOT NULL;
ALTER TABLE query ALTER objectuniquekey SET NOT NULL;


ALTER TABLE site ALTER comprehendid SET NOT NULL;
ALTER TABLE site ALTER siteid SET NOT NULL;
ALTER TABLE site ALTER studyid SET NOT NULL;
ALTER TABLE site ALTER sitename SET NOT NULL;
ALTER TABLE site ALTER sitecountry SET NOT NULL;
ALTER TABLE site ALTER siteregion SET NOT NULL;


ALTER TABLE subject ALTER comprehendid SET NOT NULL;
ALTER TABLE subject ALTER siteid SET NOT NULL;
ALTER TABLE subject ALTER studyid SET NOT NULL;
ALTER TABLE subject ALTER usubjid SET NOT NULL;
ALTER TABLE subject ALTER siteKey SET NOT NULL;
ALTER TABLE subject ALTER status SET NOT NULL;
ALTER TABLE subject ALTER objectuniquekey SET NOT NULL;


ALTER TABLE tv ALTER comprehendid SET NOT NULL;
ALTER TABLE tv ALTER studyid SET NOT NULL;
ALTER TABLE tv ALTER visitnum SET NOT NULL;
ALTER TABLE tv ALTER visit SET NOT NULL;
ALTER TABLE tv ALTER objectuniquekey SET NOT NULL;


ALTER TABLE sv ALTER comprehendid SET NOT NULL;
ALTER TABLE sv ALTER siteid SET NOT NULL;
ALTER TABLE sv ALTER studyid SET NOT NULL;
ALTER TABLE sv ALTER usubjid SET NOT NULL;
ALTER TABLE sv ALTER visitnum SET NOT NULL;
ALTER TABLE sv ALTER visitseq SET NOT NULL;
ALTER TABLE sv ALTER visit SET NOT NULL;
ALTER TABLE sv ALTER objectuniquekey SET NOT NULL;


ALTER TABLE visitform ALTER comprehendid SET NOT NULL;
ALTER TABLE visitform ALTER studyid SET NOT NULL;
ALTER TABLE visitform ALTER visitnum SET NOT NULL;
ALTER TABLE visitform ALTER visit SET NOT NULL;
ALTER TABLE visitform ALTER formid SET NOT NULL;
ALTER TABLE visitform ALTER isrequired SET NOT NULL;


ALTER TABLE sitemilestone ALTER comprehendid SET NOT NULL;
ALTER TABLE sitemilestone ALTER siteid SET NOT NULL;
ALTER TABLE sitemilestone ALTER studyid SET NOT NULL;
ALTER TABLE sitemilestone ALTER milestoneseq SET NOT NULL;
ALTER TABLE sitemilestone ALTER milestonelabel SET NOT NULL;
ALTER TABLE sitemilestone ALTER milestonetype SET NOT NULL;
ALTER TABLE sitemilestone ALTER ismandatory SET NOT NULL;
ALTER TABLE sitemilestone ALTER objectuniquekey SET NOT NULL;


ALTER TABLE sitemonitoringvisit ALTER comprehendid SET NOT NULL;
ALTER TABLE sitemonitoringvisit ALTER siteid SET NOT NULL;
ALTER TABLE sitemonitoringvisit ALTER studyid SET NOT NULL;
ALTER TABLE sitemonitoringvisit ALTER visitname SET NOT NULL;
ALTER TABLE sitemonitoringvisit ALTER visitdate SET NOT NULL;
ALTER TABLE sitemonitoringvisit ALTER objectuniquekey SET NOT NULL;


ALTER TABLE sitemonitoringvisitschedule ALTER comprehendid SET NOT NULL;
ALTER TABLE sitemonitoringvisitschedule ALTER siteid SET NOT NULL;
ALTER TABLE sitemonitoringvisitschedule ALTER studyid SET NOT NULL;
ALTER TABLE sitemonitoringvisitschedule ALTER visitname SET NOT NULL;
ALTER TABLE sitemonitoringvisitschedule ALTER plannedvisitdate SET NOT NULL;
ALTER TABLE sitemonitoringvisitschedule ALTER objectuniquekey SET NOT NULL;


ALTER TABLE study ALTER comprehendid SET NOT NULL;
ALTER TABLE study ALTER studyid SET NOT NULL;
ALTER TABLE study ALTER studyname SET NOT NULL;


ALTER TABLE studymilestone ALTER comprehendid SET NOT NULL;
ALTER TABLE studymilestone ALTER studyid SET NOT NULL;
ALTER TABLE studymilestone ALTER milestoneseq SET NOT NULL;
ALTER TABLE studymilestone ALTER milestonelabel SET NOT NULL;
ALTER TABLE studymilestone ALTER milestonetype SET NOT NULL;
ALTER TABLE studymilestone ALTER ismandatory SET NOT NULL;
ALTER TABLE studymilestone ALTER iscriticalpath SET NOT NULL;
ALTER TABLE studymilestone ALTER objectuniquekey SET NOT NULL;


ALTER TABLE formdef ALTER comprehendid SET NOT NULL;
ALTER TABLE formdef ALTER studyid SET NOT NULL;
ALTER TABLE formdef ALTER formid SET NOT NULL;
ALTER TABLE formdef ALTER formname SET NOT NULL;
ALTER TABLE formdef ALTER isprimaryendpoint SET NOT NULL;
ALTER TABLE formdef ALTER issecondaryendpoint SET NOT NULL;
ALTER TABLE formdef ALTER issdv SET NOT NULL;
ALTER TABLE formdef ALTER isrequired SET NOT NULL;
ALTER TABLE formdef ALTER objectuniquekey SET NOT NULL;


ALTER TABLE fielddef ALTER comprehendid SET NOT NULL;
ALTER TABLE fielddef ALTER studyid SET NOT NULL;
ALTER TABLE fielddef ALTER formid SET NOT NULL;
ALTER TABLE fielddef ALTER fieldid SET NOT NULL;
ALTER TABLE fielddef ALTER fieldname SET NOT NULL;
ALTER TABLE fielddef ALTER isprimaryendpoint SET NOT NULL;
ALTER TABLE fielddef ALTER issecondaryendpoint SET NOT NULL;
ALTER TABLE fielddef ALTER issdv SET NOT NULL;
ALTER TABLE fielddef ALTER isrequired SET NOT NULL;
ALTER TABLE fielddef ALTER objectuniquekey SET NOT NULL;


ALTER TABLE fielddata ALTER comprehendid SET NOT NULL;
ALTER TABLE fielddata ALTER studyid SET NOT NULL;
ALTER TABLE fielddata ALTER siteid SET NOT NULL;
ALTER TABLE fielddata ALTER usubjid SET NOT NULL;
ALTER TABLE fielddata ALTER formid SET NOT NULL;
ALTER TABLE fielddata ALTER fieldid SET NOT NULL;
ALTER TABLE fielddata ALTER fieldseq SET NOT NULL;
ALTER TABLE fielddata ALTER visit SET NOT NULL;
ALTER TABLE fielddata ALTER visitseq SET NOT NULL;
ALTER TABLE fielddata ALTER objectuniquekey SET NOT NULL;
ALTER TABLE fielddata ALTER log_num SET NOT NULL;


ALTER TABLE formdata ALTER comprehendid SET NOT NULL;
ALTER TABLE formdata ALTER studyid SET NOT NULL;
ALTER TABLE formdata ALTER siteid SET NOT NULL;
ALTER TABLE formdata ALTER usubjid SET NOT NULL;
ALTER TABLE formdata ALTER formid SET NOT NULL;
ALTER TABLE formdata ALTER formseq SET NOT NULL;
ALTER TABLE formdata ALTER visit SET NOT NULL;
ALTER TABLE formdata ALTER visitseq SET NOT NULL;
ALTER TABLE formdata ALTER objectuniquekey SET NOT NULL;


ALTER TABLE ipaccountability ALTER comprehendid SET NOT NULL;
ALTER TABLE ipaccountability ALTER studyid SET NOT NULL;
ALTER TABLE ipaccountability ALTER siteid SET NOT NULL;
ALTER TABLE ipaccountability ALTER ipname SET NOT NULL;
ALTER TABLE ipaccountability ALTER ipquantity SET NOT NULL;
ALTER TABLE ipaccountability ALTER ipunit SET NOT NULL;
ALTER TABLE ipaccountability ALTER ipdate SET NOT NULL;
ALTER TABLE ipaccountability ALTER ipseq SET NOT NULL;
ALTER TABLE ipaccountability ALTER objectuniquekey SET NOT NULL;


ALTER TABLE siteissue ALTER comprehendid SET NOT NULL;
ALTER TABLE siteissue ALTER studyid SET NOT NULL;
ALTER TABLE siteissue ALTER siteid SET NOT NULL;
ALTER TABLE siteissue ALTER issueid SET NOT NULL;
ALTER TABLE siteissue ALTER issueopeneddate SET NOT NULL;
ALTER TABLE siteissue ALTER objectuniquekey SET NOT NULL;


ALTER TABLE comprehendtermmap ALTER tablename SET NOT NULL;
ALTER TABLE comprehendtermmap ALTER columnname SET NOT NULL;
ALTER TABLE comprehendtermmap ALTER studyid SET NOT NULL;
ALTER TABLE comprehendtermmap ALTER comprehendterm SET NOT NULL;
ALTER TABLE comprehendtermmap ALTER originalterm SET NOT NULL;


ALTER TABLE ie ALTER comprehendid SET NOT NULL;
ALTER TABLE ie ALTER studyid SET NOT NULL;
ALTER TABLE ie ALTER siteid SET NOT NULL;
ALTER TABLE ie ALTER usubjid SET NOT NULL;
ALTER TABLE ie ALTER ieseq SET NOT NULL;
ALTER TABLE ie ALTER objectuniquekey SET NOT NULL;


ALTER TABLE dm ALTER comprehendid SET NOT NULL;
ALTER TABLE dm ALTER studyid SET NOT NULL;
ALTER TABLE dm ALTER siteid SET NOT NULL;
ALTER TABLE dm ALTER usubjid SET NOT NULL;
ALTER TABLE dm ALTER visitnum SET NOT NULL;
ALTER TABLE dm ALTER visit SET NOT NULL;
ALTER TABLE dm ALTER objectuniquekey SET NOT NULL;

ALTER TABLE lb ALTER comprehendid SET NOT NULL;
ALTER TABLE lb ALTER studyid SET NOT NULL;
ALTER TABLE lb ALTER siteid SET NOT NULL;
ALTER TABLE lb ALTER usubjid SET NOT NULL;
ALTER TABLE lb ALTER visit SET NOT NULL;
ALTER TABLE lb ALTER lbseq SET NOT NULL;
ALTER TABLE lb ALTER lbtestcd SET NOT NULL;
ALTER TABLE lb ALTER lbtest SET NOT NULL;
ALTER TABLE lb ALTER objectuniquekey SET NOT NULL;

ALTER TABLE studyplannedstatistic ALTER comprehendid SET NOT NULL;
ALTER TABLE studyplannedstatistic ALTER studyid SET NOT NULL;
ALTER TABLE studyplannedstatistic ALTER statCat SET NOT NULL;
ALTER TABLE studyplannedstatistic ALTER statVal SET NOT NULL;
ALTER TABLE studyplannedstatistic ALTER statUnit SET NOT NULL;
ALTER TABLE studyplannedstatistic ALTER objectUniqueKey SET NOT NULL;

ALTER TABLE siteplannedstatistic ALTER comprehendid SET NOT NULL;
ALTER TABLE siteplannedstatistic ALTER studyid SET NOT NULL;
ALTER TABLE siteplannedstatistic ALTER siteid SET NOT NULL;
ALTER TABLE siteplannedstatistic ALTER statCat SET NOT NULL;
ALTER TABLE siteplannedstatistic ALTER statVal SET NOT NULL;
ALTER TABLE siteplannedstatistic ALTER statUnit SET NOT NULL;
ALTER TABLE siteplannedstatistic ALTER objectUniqueKey SET NOT NULL;

ALTER TABLE SiteResource ALTER comprehendid SET NOT NULL;
ALTER TABLE SiteResource ALTER studyid SET NOT NULL;
ALTER TABLE SiteResource ALTER siteid SET NOT NULL;
ALTER TABLE SiteResource ALTER resourcetype SET NOT NULL;
ALTER TABLE SiteResource ALTER resourceseq SET NOT NULL;
ALTER TABLE SiteResource ALTER resourcename SET NOT NULL;
ALTER TABLE SiteResource ALTER objectuniquekey SET NOT NULL;

ALTER TABLE cm ALTER comprehendid SET NOT NULL;
ALTER TABLE cm ALTER studyid SET NOT NULL;
ALTER TABLE cm ALTER siteid SET NOT NULL;
ALTER TABLE cm ALTER usubjid SET NOT NULL;
ALTER TABLE cm ALTER cmseq SET NOT NULL;
ALTER TABLE cm ALTER objectuniquekey SET NOT NULL;

ALTER TABLE siteplannedexpenditure ALTER comprehendid SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER studyid SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER siteid SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER exptype SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expCat SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expLabel SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expdtc SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expSeq SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expAmount SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expUnit SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expAmountStd SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER expUnitStd SET NOT NULL;
ALTER TABLE siteplannedexpenditure ALTER objectuniquekey SET NOT NULL;

ALTER TABLE siteplannedenrollment ALTER comprehendid SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER studyid SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER siteid SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER frequency SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER endDate SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER enrollmentType SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER enrollmentcount SET NOT NULL;
ALTER TABLE siteplannedenrollment ALTER objectUniqueKey SET NOT NULL;

ALTER TABLE ex ALTER comprehendid SET NOT NULL;
ALTER TABLE ex ALTER studyid SET NOT NULL;
ALTER TABLE ex ALTER siteid SET NOT NULL;
ALTER TABLE ex ALTER usubjid SET NOT NULL;
ALTER TABLE ex ALTER exseq SET NOT NULL;
ALTER TABLE ex ALTER objectuniquekey SET NOT NULL;

ALTER TABLE studycro ALTER comprehendid SET NOT NULL;
ALTER TABLE studycro ALTER crokey SET NOT NULL;
ALTER TABLE studycro ALTER studyid SET NOT NULL;
ALTER TABLE studycro ALTER croid SET NOT NULL;
ALTER TABLE studycro ALTER objectuniquekey SET NOT NULL;

ALTER TABLE mh ALTER comprehendid SET NOT NULL;
ALTER TABLE mh ALTER studyid SET NOT NULL;
ALTER TABLE mh ALTER siteid SET NOT NULL;
ALTER TABLE mh ALTER usubjid SET NOT NULL;
ALTER TABLE mh ALTER mhseq SET NOT NULL;
ALTER TABLE mh ALTER objectuniquekey SET NOT NULL;

ALTER TABLE studyplannedrecruitment ALTER comprehendid SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER studyid SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER category SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER frequency SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER enddate SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER type SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER recruitmentcount SET NOT NULL;
ALTER TABLE studyplannedrecruitment ALTER objectuniquekey SET NOT NULL;

ALTER TABLE qs ALTER comprehendid SET NOT NULL;
ALTER TABLE qs ALTER studyid SET NOT NULL;
ALTER TABLE qs ALTER siteid SET NOT NULL;
ALTER TABLE qs ALTER usubjid SET NOT NULL;
ALTER TABLE qs ALTER qsseq SET NOT NULL;
ALTER TABLE qs ALTER objectuniquekey SET NOT NULL;

ALTER TABLE vs ALTER comprehendid SET NOT NULL;
ALTER TABLE vs ALTER studyid SET NOT NULL;
ALTER TABLE vs ALTER siteid SET NOT NULL;
ALTER TABLE vs ALTER usubjid SET NOT NULL;
ALTER TABLE vs ALTER vsseq SET NOT NULL;
ALTER TABLE vs ALTER objectuniquekey SET NOT NULL;

ALTER TABLE eg ALTER comprehendid SET NOT NULL;
ALTER TABLE eg ALTER studyid SET NOT NULL;
ALTER TABLE eg ALTER siteid SET NOT NULL;
ALTER TABLE eg ALTER usubjid SET NOT NULL;
ALTER TABLE eg ALTER egseq SET NOT NULL;
ALTER TABLE eg ALTER objectuniquekey SET NOT NULL;

ALTER TABLE pe ALTER comprehendid SET NOT NULL;
ALTER TABLE pe ALTER studyid SET NOT NULL;
ALTER TABLE pe ALTER siteid SET NOT NULL;
ALTER TABLE pe ALTER usubjid SET NOT NULL;
ALTER TABLE pe ALTER peseq SET NOT NULL;
ALTER TABLE pe ALTER objectuniquekey SET NOT NULL;

ALTER TABLE pc ALTER comprehendid SET NOT NULL;
ALTER TABLE pc ALTER studyid SET NOT NULL;
ALTER TABLE pc ALTER siteid SET NOT NULL;
ALTER TABLE pc ALTER usubjid SET NOT NULL;
ALTER TABLE pc ALTER pcseq SET NOT NULL;
ALTER TABLE pc ALTER objectuniquekey SET NOT NULL;

ALTER TABLE pp ALTER comprehendid SET NOT NULL;
ALTER TABLE pp ALTER studyid SET NOT NULL;
ALTER TABLE pp ALTER siteid SET NOT NULL;
ALTER TABLE pp ALTER usubjid SET NOT NULL;
ALTER TABLE pp ALTER ppseq SET NOT NULL;
ALTER TABLE pp ALTER objectuniquekey SET NOT NULL;

ALTER TABLE comprehendcodelist ALTER codekey SET NOT NULL;
ALTER TABLE comprehendcodelist ALTER codename SET NOT NULL;
ALTER TABLE comprehendcodelist ALTER codevalue SET NOT NULL;

ALTER TABLE flexfield ALTER tableName  SET NOT NULL;
ALTER TABLE flexfield ALTER flexName  SET NOT NULL;

ALTER TABLE rslt ALTER comprehendid SET NOT NULL;
ALTER TABLE rslt ALTER studyid SET NOT NULL;
ALTER TABLE rslt ALTER usubjid SET NOT NULL;
ALTER TABLE rslt ALTER rsltseq SET NOT NULL;
ALTER TABLE rslt ALTER objectuniquekey SET NOT NULL;

ALTER TABLE sitemonitoringvisitreport ALTER comprehendid SET NOT NULL;
ALTER TABLE sitemonitoringvisitreport ALTER studyid SET NOT NULL;
ALTER TABLE sitemonitoringvisitreport ALTER siteid SET NOT NULL;
ALTER TABLE sitemonitoringvisitreport ALTER sitename SET NOT NULL;
ALTER TABLE sitemonitoringvisitreport ALTER visitname SET NOT NULL;
ALTER TABLE sitemonitoringvisitreport ALTER objectuniquekey SET NOT NULL;

ALTER TABLE comprehendcustommetric ALTER metricid SET NOT NULL;
ALTER TABLE comprehendcustommetric ALTER studyid SET NOT NULL;

ALTER TABLE tsdv ALTER comprehendid SET NOT NULL;
ALTER TABLE tsdv ALTER studyid SET NOT NULL;
ALTER TABLE tsdv ALTER sdvtier SET NOT NULL;
ALTER TABLE tsdv ALTER formid SET NOT NULL;
ALTER TABLE tsdv ALTER fieldid SET NOT NULL;
ALTER TABLE tsdv ALTER issdv SET NOT NULL;
ALTER TABLE tsdv ALTER objectuniquekey SET NOT NULL;

ALTER TABLE tva ALTER comprehendid SET NOT NULL;
ALTER TABLE tva ALTER studyid SET NOT NULL;
ALTER TABLE tva ALTER armcd SET NOT NULL;
ALTER TABLE tva ALTER objectuniquekey SET NOT NULL;

ALTER TABLE se ALTER comprehendid SET NOT NULL;
ALTER TABLE se ALTER studyid SET NOT NULL;
ALTER TABLE se ALTER siteid SET NOT NULL;
ALTER TABLE se ALTER usubjid SET NOT NULL;
ALTER TABLE se ALTER seseq SET NOT NULL;
ALTER TABLE se ALTER sestdtc SET NOT NULL;
ALTER TABLE se ALTER sestdat SET NOT NULL;
ALTER TABLE se ALTER objectuniquekey SET NOT NULL;

ALTER TABLE ta ALTER comprehendid SET NOT NULL;
ALTER TABLE ta ALTER studyid SET NOT NULL;
ALTER TABLE ta ALTER armcd SET NOT NULL;
ALTER TABLE ta ALTER taetord SET NOT NULL;
ALTER TABLE ta ALTER objectuniquekey SET NOT NULL;

ALTER TABLE te ALTER comprehendid SET NOT NULL;
ALTER TABLE te ALTER studyid SET NOT NULL;
ALTER TABLE te ALTER etcd SET NOT NULL;
ALTER TABLE te ALTER objectuniquekey SET NOT NULL;

ALTER TABLE dq_audit ALTER comprehendid SET NOT NULL;
ALTER TABLE dq_audit ALTER customer_name SET NOT NULL;
ALTER TABLE dq_audit ALTER studyid SET NOT NULL;
ALTER TABLE dq_audit ALTER source_system SET NOT NULL;
ALTER TABLE dq_audit ALTER src_schema SET NOT NULL;
ALTER TABLE dq_audit ALTER src_table SET NOT NULL;
ALTER TABLE dq_audit ALTER record_count SET NOT NULL;
ALTER TABLE dq_audit ALTER record_type SET NOT NULL;
ALTER TABLE dq_audit ALTER dq_run_id SET NOT NULL;
ALTER TABLE dq_audit ALTER objectuniquekey SET NOT NULL;

ALTER TABLE dq_bad_data ALTER comprehendid SET NOT NULL;
ALTER TABLE dq_bad_data ALTER severity SET NOT NULL;
ALTER TABLE dq_bad_data ALTER rule_type SET NOT NULL;
ALTER TABLE dq_bad_data ALTER customer_name SET NOT NULL;
ALTER TABLE dq_bad_data ALTER studyid SET NOT NULL;
ALTER TABLE dq_bad_data ALTER source_system SET NOT NULL;
ALTER TABLE dq_bad_data ALTER src_schema SET NOT NULL;
ALTER TABLE dq_bad_data ALTER src_table SET NOT NULL;
ALTER TABLE dq_bad_data ALTER dq_run_id SET NOT NULL;
ALTER TABLE dq_bad_data ALTER objectuniquekey SET NOT NULL;

ALTER TABLE siteexpendituredata ALTER comprehendid SET NOT NULL;
ALTER TABLE siteexpendituredata ALTER studyid SET NOT NULL;
ALTER TABLE siteexpendituredata ALTER siteid SET NOT NULL;
ALTER TABLE siteexpendituredata ALTER join_helper SET NOT NULL;
ALTER TABLE siteexpendituredata ALTER objectuniquekey SET NOT NULL;

ALTER TABLE siteexpendituresummary ALTER comprehendid SET NOT NULL;
ALTER TABLE siteexpendituresummary ALTER studyid SET NOT NULL;
ALTER TABLE siteexpendituresummary ALTER siteid SET NOT NULL;
ALTER TABLE siteexpendituresummary ALTER join_helper SET NOT NULL;
ALTER TABLE siteexpendituresummary ALTER objectuniquekey SET NOT NULL;


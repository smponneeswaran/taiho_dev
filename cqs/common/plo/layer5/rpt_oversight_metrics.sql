/*
rpt_oversight_metrics PLO
*/
CREATE TABLE rpt_oversight_metrics AS
WITH
cro AS (
  -- CRO name string aggregation per study
  SELECT studyid, STRING_AGG(DISTINCT NULLIF(croname, ''), ', ' ORDER BY NULLIF(croname, '')) AS cronames
  FROM studycro GROUP BY studyid
),
sreg AS (
  -- Site region string aggregation at the study level
  SELECT sr.studyid, STRING_AGG(DISTINCT NULLIF(sr.siteregion, '') , ', ' ORDER BY NULLIF(sr.siteregion, '')) AS studyregions
  FROM site sr GROUP BY sr.studyid
)
SELECT
  -- comprehendcustommetric
  cm.metricid::TEXT AS metricid,
  cm.studyid::TEXT AS studyid,
  cm.siteid::TEXT AS siteid,
  cm.numerator::NUMERIC AS numerator,
  cm.denominator::NUMERIC AS denominator,
  cm.invalidvalue::BOOLEAN AS invalidvalue,
  -- Study fields
  st.studyname::TEXT AS studyname,
  st.program::TEXT AS studyprogram,
  st.medicalindication::TEXT AS studymedicalindication,
  st.studyphase::TEXT AS studyphase,
  st.therapeuticarea::TEXT AS studytherapeuticarea,
  cro.cronames::TEXT AS studycronames,
  st.studystartdate::DATE AS studystartdate,
  st.studystatus::TEXT AS studystatus,
  st.studydescription::TEXT AS studydescription,
  sreg.studyregions::TEXT AS studyregions,
  st.studysponsor::TEXT AS studysponsor,
  dst.study_planned_completion_date::DATE AS studyplannedenddate,
  -- Site fields
  si.sitename::TEXT AS sitename,
  si.sitecountry::TEXT AS sitecountry,
  si.sitecity::TEXT AS sitecity,
  si.sitestate::TEXT AS sitestate,
  si.sitepostal::TEXT AS sitepostal,
  si.siteinvestigatorname::TEXT AS siteinvestigatorname,
  si.sitecraname::TEXT AS sitecraname,
  si.siteactivationdate::DATE AS siteactivationdate,
  si.sitedeactivationdate::DATE AS sitedeactivationdate,
  si.siteregion::TEXT AS siteregion,
  sperf.enrolled_count::INTEGER AS siteenrolledcount,
  si.sitecro::TEXT AS sitecro,
  -- rpt_pivotal_study_analytics
  psa.current_milestone::TEXT AS studycurrentmilestone,
  psa.current_milestone_planned_date::DATE AS studycurrentmilestoneplanneddate,
  psa.current_milestone_projected_date::DATE AS studycurrentmilestoneprojecteddate,
  psa.current_site_activation_count::INTEGER AS studycurrentsiteactivationcount,
  psa.target_site_activation_count::INTEGER AS studytargetsiteactivationcount,
  -- rpt_enrollment_analytics
  ea.actual_enrollment_count::INTEGER AS studyactualenrollmentcount,
  ea.planned_enrollment_count::INTEGER AS studyplannedenrollmentcount,
  -- Other
  NOW()::TIMESTAMP WITHOUT TIME ZONE AS comprehend_update_time
FROM comprehendcustommetric AS cm
JOIN study st ON (cm.studyid = st.studyid)
LEFT JOIN site si ON (cm.studyid = si.studyid AND cm.siteid = si.siteid)
LEFT JOIN cro ON (cm.studyid = cro.studyid)
LEFT JOIN sreg ON (cm.studyid = sreg.studyid)
LEFT JOIN rpt_pivotal_study_analytics psa ON (psa.studyid = cm.studyid)
LEFT JOIN rpt_enrollment_analytics ea ON (ea.studyid = cm.studyid)
LEFT JOIN rpt_site_performance sperf ON (sperf.studyid = cm.studyid AND sperf.siteid = cm.siteid)
LEFT JOIN dimstudy dst ON (cm.studyid = dst.studyid)
;


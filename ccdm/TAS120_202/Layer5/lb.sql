/*
CCDM LB mapping
Notes: Standard mapping to CCDM LB table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     lb_data AS (
                SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                        "FolderName"::text AS visit,
                        null::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        null::integer AS lbseq,
                        null::text AS lbtestcd,
                        "AnalyteName"::text AS lbtest,
                        "DataPageName"::text AS lbcat,
                        null::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        "AnalyteValue"::text AS lborres,
                        null::text AS lbstat,
                        null::text AS lbreasnd,
                        "StdLow"::numeric AS lbstnrlo,
                        "StdHigh"::numeric AS lbstnrhi,
                        "LabUnits"::text AS lborresu,
                        "StdValue"::numeric AS  lbstresn,
                        "StdUnits"::text AS  lbstresu,
                        null::text AS  lbblfl,
                        null::text AS  lbnrind,
                        "LabLow"::text AS  lbornrhi,
                        "LabHigh"::text AS  lbornrlo,
                        null::text AS  lbstresc,
                        null::text AS  lbenint,
                        null::text AS  lbevlint,
                        null::text AS  lblat,
                        null::numeric AS  lblloq,
                        null::text AS  lbloc,
                        null::text AS  lbpos,
                        null::text AS  lbstint,
                        null::numeric AS  lbuloq,
                        null::text AS  lbclsig,
                        null::time without time zone AS lbtm 
						
						
						
						
						)

SELECT 
        /*KEY (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid)::text AS comprehendid, KEY*/
        lb.studyid::text AS studyid,
        lb.siteid::text AS siteid,
        lb.usubjid::text AS usubjid,
        lb.visit::text AS visit,
        lb.lbdtc::timestamp without time zone AS lbdtc,
        lb.lbdy::integer AS lbdy,
        lb.lbseq::integer AS lbseq,
        lb.lbtestcd::text AS lbtestcd,
        lb.lbtest::text AS lbtest,
        lb.lbcat::text AS lbcat,
        lb.lbscat::text AS lbscat,
        lb.lbspec::text AS lbspec,
        lb.lbmethod::text AS lbmethod,
        lb.lborres::text AS lborres,
        lb.lbstat::text AS lbstat,
        lb.lbreasnd::text AS lbreasnd,
        lb.lbstnrlo::numeric AS lbstnrlo,
        lb.lbstnrhi::numeric AS lbstnrhi,
        lb.lborresu::text AS lborresu,
        lb.lbstresn::numeric AS  lbstresn,
        lb.lbstresu::text AS  lbstresu,
        lb.lbtm::time without time zone AS lbtm,
        lb.lbblfl::text AS  lbblfl,
        lb.lbnrind::text AS  lbnrind,
        lb.lbornrhi::text AS  lbornrhi,
        lb.lbornrlo::text AS  lbornrlo,
        lb.lbstresc::text AS  lbstresc,
        lb.lbenint::text AS  lbenint,
        lb.lbevlint::text AS  lbevlint,
        lb.lblat::text AS  lblat,
        lb.lblloq::numeric AS  lblloq,
        lb.lbloc::text AS  lbloc,
        lb.lbpos::text AS  lbpos,
        lb.lbstint::text AS  lbstint,
        lb.lbuloq::numeric AS  lbuloq,
        lb.lbclsig::text AS  lbclsig
        /*KEY , (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid || '~' || lb.lbseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM lb_data lb
JOIN included_subjects s ON (lb.studyid = s.studyid AND lb.siteid = s.siteid AND lb.usubjid = s.usubjid)

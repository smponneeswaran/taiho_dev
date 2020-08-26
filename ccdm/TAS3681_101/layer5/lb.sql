/*
CCDM LB mapping
Notes: Standard mapping to CCDM LB table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     lb_data AS (
                SELECT  lb1."project"::text AS studyid,
                        lb1."SiteNumber"::text AS siteid, 
				 	    lb1."Subject"::text    AS usubjid,
                        lb1."FolderName"::text AS visit,
						CASE
							WHEN lb1."DataPageName" like '%Chemistry%' THEN chem."LBDAT"
							WHEN lb1."DataPageName" like '%Hematology% - CBC' THEN hem."LBDAT"
							WHEN lb1."DataPageName" like '%Coagulation%' THEN coag."LBDAT"
							WHEN lb1."DataPageName" like '%Urinalysis%' THEN urine."LBDAT"
						END::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        null::integer AS lbseq,
                        lb1."AnalyteName"::text AS lbtestcd,
                        lb1."AnalyteName"::text AS lbtest,
                        lb1."DataPageName"::text AS lbcat,
                        null::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                       lb1."AnalyteValue"::text AS lborres,
                        null::text AS lbstat,
                        null::text AS lbreasnd,
                        lb1."StdLow"::numeric AS lbstnrlo,
                        lb1."StdHigh"::numeric AS lbstnrhi,
                        lb1."LabUnits"::text AS lborresu,
                        lb1."StdValue"::numeric AS  lbstresn,
                        lb1."StdUnits"::text AS  lbstresu,
						null::time without time zone AS lbtm, 
                        null::text AS  lbblfl,
                        null::text AS  lbnrind,
                        lb1."LabLow"::text AS  lbornrhi,
                        lb1."LabHigh"::text AS  lbornrlo,
                        null::text AS  lbstresc,
                        null::text AS  lbenint,
                        null::text AS  lbevlint,
                        null::text AS  lblat,
                        null::numeric AS  lblloq,
                        null::text AS  lbloc,
                        null::text AS  lbpos,
                        null::text AS  lbstint,
                        null::numeric AS  lbuloq,
                        null::text AS  lbclsig
			From        tas3681_101."NormLab" lb1
			LEFT JOIN tas3681_101."CHEM" chem on (lb1."project" = chem."project" AND right(lb1."SiteNumber",3) = right(chem."SiteNumber",3) AND lb1."Subject" = chem."Subject" AND lb1."FolderName" = chem."FolderName")
			LEFT JOIN tas3681_101."COAG" coag on (lb1."project" = coag."project" AND right(lb1."SiteNumber",3) = right(coag."SiteNumber",3) AND lb1."Subject" = coag."Subject" AND lb1."FolderName" = coag."FolderName")
			LEFT JOIN tas3681_101."HEMA" hem on (lb1."project" = hem."project" AND right(lb1."SiteNumber",3) = right(hem."SiteNumber",3) AND lb1."Subject" = hem."Subject" AND lb1."FolderName" = hem."FolderName")
			LEFT JOIN tas3681_101."URIN" urine  on (lb1."project" = urine."project" AND right(lb1."SiteNumber",3) = right(urine."SiteNumber",3) AND lb1."Subject" = urine."Subject" AND lb1."FolderName" = urine."FolderName")
			
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
JOIN included_subjects s ON (lb.studyid = s.studyid AND lb.siteid = s.siteid AND lb.usubjid = s.usubjid);
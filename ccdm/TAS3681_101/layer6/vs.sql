/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     vs_data AS (
                -- TAS3681-101  VS
                SELECT  vs."project"::text AS studyid,
                    vs."SiteNumber"::text AS siteid, 
				 	vs."Subject"::text    AS usubjid,
						Null::int AS vsseq,
                    Null::text AS vstestcd,
                    vstest::text AS vstest,
                    vscat::text AS vscat,
                    vsscat::text AS vsscat,
                    vspos::text AS vspos,
                    vsorres::text AS vsorres,
                    vsorresu::text AS vsorresu,
                    vsstresn::numeric AS vsstresn,
                    vsstresu::text AS vsstresu,
                    null::text AS vsstat,
                    null::text AS vsloc,
                    null::text AS vsblfl,
                    vs."FolderName"::text AS visit,
                    vs."VSDAT"::timestamp without time zone AS vsdtc,
                    null::time without time zone AS vstm
                FROM tas3681_101."VS" vs
                cross join lateral(
				values
					('Diastolic Blood Pressure'	, "VSDBP" , 'Vital Signs','Vital Signs', "VSDBP"	,"VSDBP_Units"	  ,"VSDBP"	,"VSDBP_Units"),
					('Systolic Blood Pressure'	, "VSSBP" , 'Vital Signs','Vital Signs', "VSSBP"	,"VSSBP_Units"	  ,"VSSBP"	,"VSSBP_Units"),
					('Pressure Rate'			, "VSHR"  , 'Vital Signs','Vital Signs', "VSHR"	    ,"VSHR_Units"	  ,"VSHR"	,"VSHR_Units"),
					('Respiratory Rate'			, "VSRESP", 'Vital Signs','Vital Signs', "VSRESP"	,"VSRESP_Units"	  ,"VSRESP"	,"VSRESP_Units"),
					('Temperature'				, "VSTEMP", 'Vital Signs','Vital Signs', "VSTEMP"	,"VSTEMP_Units"	  ,"VSTEMP"	,"VSTEMP_Units"),
					('Weight'					, "VSWT"  , 'Vital Signs','Vital Signs', "VSWT"		,"VSWT_Units"	  ,"VSWT"	,"VSWT_Units")
					
				)as t
					(vscat, vstest, vsscat, vspos, vsorres, vsorresu, vsstresn, vsstresu)
								
				union all
				
				-- TAS3681-101  VSB
                SELECT  vsb."project"::text AS studyid,
                    vsb."SiteNumber"::text AS siteid, 
				 	vsb."Subject"::text    AS usubjid,
						Null::int AS vsseq,
                    Null::text AS vstestcd,
                    vstest::text AS vstest,
                    vscat::text AS vscat,
                    vsscat::text AS vsscat,
                    vspos::text AS vspos,
                    vsorres::text AS vsorres,
                    vsorresu::text AS vsorresu,
                    vsstresn::numeric AS vsstresn,
                    vsstresu::text AS vsstresu,
                    null::text AS vsstat,
                    null::text AS vsloc,
                    null::text AS vsblfl,
                    vsb."FolderName"::text AS visit,
                    vsb."VSDAT"::timestamp without time zone AS vsdtc,
                    null::time without time zone AS vstm
                FROM tas3681_101."VSB" vsb
                cross join lateral(
				values
					('Diastolic Blood Pressure'	, "VSDBP" , 'Vital Signs','Vital Signs', "VSDBP"	,"VSDBP_Units"	  ,"VSDBP"	,"VSDBP_Units"),
					('Systolic Blood Pressure'	, "VSSBP" , 'Vital Signs','Vital Signs', "VSSBP"	,"VSSBP_Units"	  ,"VSSBP"	,"VSSBP_Units"),
					('Pressure Rate'			, "VSHR"  , 'Vital Signs','Vital Signs', "VSHR"	    ,"VSHR_Units"	  ,"VSHR"	,"VSHR_Units"),
					('Respiratory Rate'			, "VSRESP", 'Vital Signs','Vital Signs', "VSRESP"	,"VSRESP_Units"	  ,"VSRESP"	,"VSRESP_Units"),
					('Temperature'				, "VSTEMP", 'Vital Signs','Vital Signs', "VSTEMP"	,"VSTEMP_Units"	  ,"VSTEMP"	,"VSTEMP_Units"),
					('Weight'					, "VSWT"  , 'Vital Signs','Vital Signs', "VSWT"		,"VSWT_Units"	  ,"VSWT"	,"VSWT_Units"),
					('Height'					, "VSHT"  , 'Vital Signs','Vital Signs', "VSHT"		,"VSHT_Units"	  ,"VSHT"	,"VSHT_Units")
					
				)as t
					(vscat, vstest, vsscat, vspos, vsorres, vsorresu, vsstresn, vsstresu)
				
				
			
     ),
     all_data as (
                SELECT
                    vs.studyid::text AS studyid,
                    vs.siteid::text AS siteid,
                    vs.usubjid::text AS usubjid,
                    (row_number() over (partition by vs.studyid, vs.siteid, vs.usubjid order by vs.vsdtc, vs.vstm))::int as vsseq,
                    vs.vstestcd::text AS vstestcd,
                    vs.vstest::text AS vstest,
                    vs.vscat::text AS vscat,
                    vs.vsscat::text AS vsscat,
                    vs.vspos::text AS vspos,
                    vs.vsorres::text AS vsorres,
                    vs.vsorresu::text AS vsorresu,
					vs.vsstresn::numeric AS vsstresn,
					vs.vsstresu ::text AS vsstresu,
					vs.vsstat::text AS vsstat,
                    vs.vsloc::text AS vsloc,
                    vs.vsblfl::text AS vsblfl,
                    vs.visit::text AS visit,
                    vs.vsdtc::text::timestamp without time zone AS vsdtc,
                    vs.vstm::text::time without time zone AS vstm
                FROM vs_data vs
    )

SELECT
        /*KEY (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid)::text AS comprehendid, KEY*/
        vs.studyid::text AS studyid,
        vs.siteid::text AS siteid,
        vs.usubjid::text AS usubjid,
        vs.vsseq::int AS vsseq, 
        vs.vstestcd::text AS vstestcd,
        vs.vstest::text AS vstest,
        vs.vscat::text AS vscat,
        vs.vsscat::text AS vsscat,
        vs.vspos::text AS vspos,
        vs.vsorres::text AS vsorres,
        vs.vsorresu::text AS vsorresu,
        vs.vsstresn::numeric AS vsstresn,
        vs.vsstresu::text AS vsstresu,
        vs.vsstat::text AS vsstat,
        vs.vsloc::text AS vsloc,
        vs.vsblfl::text AS vsblfl,
        vs.visit::text AS visit,
        vs.vsdtc::timestamp without time zone AS vsdtc,
        vs.vstm::time without time zone AS vstm
        /*KEY , (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid || '~' || vs.vsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM all_data vs
JOIN included_subjects s ON (vs.studyid = s.studyid AND vs.siteid = s.siteid AND vs.usubjid = s.usubjid);

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
 eg_data AS 
(        
         SELECT   eg.studyid, 
                  LEFT(eg.siteid,3)  AS siteid,
				  eg.usubjid  AS usubjid,
                  (Row_number() OVER (partition BY eg.studyid, eg.siteid, eg.usubjid ORDER BY eg.egseq, eg.egdtc))::int AS egseq,
                  upper(eg.egtestcd) as egtestcd, 
                  eg.egtest, 
                  eg.egcat, 
                  eg.egscat, 
                  eg.egpos, 
                  eg.egorres, 
                  eg.egorresu, 
                  eg.egstresn, 
                  eg.egstresu, 
                  eg.egstat, 
                  eg.egloc, 
                  eg.egblfl, 
                  eg.visit, 
                  eg.egdtc, 
                  eg.egtm 
         FROM     (          -- TAS3681-101  ECG
                             SELECT     "project"::text   AS studyid, 
                                        right("SiteNumber",3)::	text      AS siteid,
										right("Subject",7)::	text      AS usubjid,
                                        NULL::int            AS egseq, 
                                        'ECG'::text     AS egtestcd, 
                                        'ECG'::text       AS egtest, 
                                        'ECG'::text AS egcat, 
                                        egscat::text           AS egscat, 
                                        NULL::text           AS egpos, 
                                        egorres::text      AS egorres, 
                                        egorresu::text     AS egorresu, 
										egstresn::text AS egstresn,
										egstresu::text AS egstresu, 
                                        NULL::text  AS egstat, 
                                        NULL::text  AS egloc, 
                                        "FolderName"::text AS visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
										"ECGTIM"::time without time zone AS egtm 
                             FROM  tas3681_101."ECG" 
                             cross join lateral(
				values
						('RR Interval'				, "ECGRR"	,"ECGRR_Units"	  ,"ECGRR"	,"ECGRR_Units"),
						('Derived QTcF Interval'	, "ECQTCF"	,"ECQTCF_Units"	  ,"ECQTCF"	,"ECQTCF_Units"),
						('HR'						, "ECGHR"   ,"ECGHR_Units"	  ,"ECGHR"	,"ECGHR_Units"),
						('QT Interval'				, "ECGQT"	,"ECGQT_Units"	  ,"ECGQT"	,"ECGQT_Units"),
						('QTc Interval'				, "ECGQTC"	,"ECGQTC_Units"	  ,"ECGQTC"	,"ECGQTC_Units")
					)as t
					(egscat, egorres, egorresu, egstresn, egstresu)
					
			UNION 
			-- TAS3681-101  ECG2
                             SELECT     "project"::text   AS studyid, 
                                        right("SiteNumber",3)::	text      AS siteid,
										right("Subject",7)::	text      AS usubjid,
                                        NULL::int            AS egseq, 
                                        'ECG'::text     AS egtestcd, 
                                        'ECG'::text       AS egtest, 
                                        'ECG'::text AS egcat, 
                                        egscat::text           AS egscat, 
                                        NULL::text           AS egpos, 
                                        egorres::text      AS egorres, 
                                        egorresu::text     AS egorresu, 
										egstresn::text AS egstresn,
										egstresu::text AS egstresu, 
                                        NULL::text  AS egstat, 
                                        NULL::text  AS egloc, 
                                        "FolderName"::text AS visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
										"ECGTIM"::time without time zone AS egtm 
                             FROM  tas3681_101."ECG" 
                             cross join lateral(
				values
						('RR Interval'				, "ECGRR"	,"ECGRR_Units"	  ,"ECGRR"	,"ECGRR_Units"),
						('Derived QTcF Interval'	, "ECQTCF"	,"ECQTCF_Units"	  ,"ECQTCF"	,"ECQTCF_Units"),
						('HR'						, "ECGHR"   ,"ECGHR_Units"	  ,"ECGHR"	,"ECGHR_Units"),
						('QT Interval'				, "ECGQT"	,"ECGQT_Units"	  ,"ECGQT"	,"ECGQT_Units"),
						('QTc Interval'				, "ECGQTC"	,"ECGQTC_Units"	  ,"ECGQTC"	,"ECGQTC_Units")
					)as t
					(egscat, egorres, egorresu, egstresn, egstresu)
					
				UNION 
			-- TAS3681-101  ECG3
                             SELECT     "project"::text   AS studyid, 
                                        right("SiteNumber",3)::	text      AS siteid,
										right("Subject",7)::	text      AS usubjid,
                                        NULL::int            AS egseq, 
                                        'ECG'::text     AS egtestcd, 
                                        'ECG'::text       AS egtest, 
                                        'ECG'::text AS egcat, 
                                        egscat::text           AS egscat, 
                                        NULL::text           AS egpos, 
                                        egorres::text      AS egorres, 
                                        egorresu::text     AS egorresu, 
										egstresn::text AS egstresn,
										egstresu::text AS egstresu, 
                                        NULL::text  AS egstat, 
                                        NULL::text  AS egloc, 
                                        "FolderName"::text AS visit,
                                        "ECGDAT" ::timestamp without time zone AS egdtc,
										"ECGTIM"::time without time zone AS egtm 
                             FROM  tas3681_101."ECG" 
                             cross join lateral(
				values
						('RR Interval'				, "ECGRR"	,"ECGRR_Units"	  ,"ECGRR"	,"ECGRR_Units"),
						('Derived QTcF Interval'	, "ECQTCF"	,"ECQTCF_Units"	  ,"ECQTCF"	,"ECQTCF_Units"),
						('HR'						, "ECGHR"   ,"ECGHR_Units"	  ,"ECGHR"	,"ECGHR_Units"),
						('QT Interval'				, "ECGQT"	,"ECGQT_Units"	  ,"ECGQT"	,"ECGQT_Units"),
						('QTc Interval'				, "ECGQTC"	,"ECGQTC_Units"	  ,"ECGQTC"	,"ECGQTC_Units")
					)as t
					(egscat, egorres, egorresu, egstresn, egstresu)
                              ) eg ) 
SELECT 
       /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
       eg.studyid::text                                   AS studyid, 
       eg.siteid::text                                    AS siteid, 
       eg.usubjid::text                                   AS usubjid, 
       eg.egseq::int                                      AS egseq, 
       eg.egtestcd::text                                  AS egtestcd, 
       eg.egtest::text                                    AS egtest, 
       eg.egcat::text                                     AS egcat, 
       eg.egscat::text                                    AS egscat, 
       eg.egpos::text                                     AS egpos, 
       eg.egorres::text                                   AS egorres, 
       eg.egorresu::text                                  AS egorresu, 
       eg.egstresn::numeric                               AS egstresn, 
       eg.egstresu::text                                  AS egstresu, 
       eg.egstat::text                                    AS egstat, 
       eg.egloc::text                                     AS egloc, 
       eg.egblfl::text                                    AS egblfl, 
       eg.visit::text                                     AS visit, 
       eg.egdtc::timestamp without time zone              AS egdtc, 
       eg.egtm::                   time without time zone AS egtm 
       /*KEY , (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text AS objectuniquekey KEY*/
       /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/ 
FROM   eg_data eg 
JOIN   included_subjects s 
ON     (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid);

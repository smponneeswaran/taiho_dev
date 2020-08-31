/*
CDM get_subj_data function
Client: Regeneron
Description: gets list of all subjects with previous screening numbers
*/
DROP FUNCTION if exists get_svdata(text);

CREATE OR REPLACE FUNCTION get_svdata(pStudyID text)  
RETURNS BOOLEAN
VOLATILE
AS $dbvis$
DECLARE

    lSQL text := '';
    b_add_union_next BOOLEAN := FALSE;
    b_edc_exists BOOLEAN := TRUE;
    t_ddl text;
    t_schema text;
    t_studyid text;
    t_sv_tbl text;
    t_svstdtc text;
    t_prev_scrnum text;

    
BEGIN
    
        t_studyid :=pStudyID;
        t_schema :=lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));
        
         -- verify if edc data source exists and fetch study specific mapping data for edc
          IF EXISTS(SELECT distinct table_schema
                                     FROM information_schema.tables
                                 WHERE table_name not in ('__sites',
                                                                                '__subjects',
                                                                                'audit_FormData',
                                                                                'audit_ItemData',
                                                                                'audit_ItemGroupData',
                                                                                'audit_StudyEventData',
                                                                                'audit_SubjectData',
                                                                                'metadata_fields',
                                                                                'metadata_folders',
                                                                                'metadata_forms',
                                                                                'metadata_log_lines',
                                                                                'metadata_measurement_units',
                                                                                'stream_page_status',
                                                                                'stream_query_detail'
                                                                                )
                                   )
          THEN
                        SELECT 
                                          formid INTO t_sv_tbl
                          FROM  odr_staging.ODR_MAPPING_REQ  
                       WHERE datapoint_id in ('SV_06') AND active_fl=1
                             AND  studyid = t_studyid;  
                                                              
                        SELECT 
                                          fieldid INTO t_svstdtc
                          FROM  odr_staging.ODR_MAPPING_REQ  
                       WHERE datapoint_id='SV_06'  AND active_fl=1
                             AND  studyid = t_studyid;  

                        raise notice 't_sv_tbl=%', t_sv_tbl;
                        raise notice 't_svstdtc=%', t_svstdtc;
        
          ELSE
                        b_edc_exists := FALSE;
                        -- insert alert message "EDC Data not available"
          END IF;    
        
       -- create temp table to fetch data from edc
          IF b_edc_exists and t_sv_tbl is not null and t_svstdtc is not null THEN
                        t_ddl := 'create table crf_svdata_'||t_schema||' as
                                        (
                                                SELECT
                                                         studyid::text AS studyid
                                                         , siteid::text AS siteid
                                                         , usubjid::text AS usubjid
                                                         , log_num::int AS visitnum
                                                         , visit::text AS visit
                                                         , visitseq::int AS visitseq
                                                         , datavalue::date AS svstdtc
                                                         , datavalue::date AS svendtc
                                                FROM fielddata
                                            where studyid='''||t_studyid||''' 
                                                  and formid='''||t_sv_tbl||''' 
                                                  and fieldid = '''||t_svstdtc||''' 
                                                  and datavalue is not null and datavalue <>'''')';
                            
                        EXECUTE t_ddl;
                         
          ELSE
                t_ddl := 'CREATE TABLE crf_svdata_'||t_schema||' AS 
                                  SELECT
                                    NULL::TEXT AS studyid,
                                    NULL::TEXT AS siteid,
                                    NULL::TEXT AS usubjid,
                                    NULL::int AS visitnum,
                                    NULL::TEXT AS visit,
                                    NULL::int AS visitseq, /* defaulted to 1 - deprecated */
                                    NULL::date AS svstdtc,
                                    NULL::date AS svendtc';
                EXECUTE t_ddl;

          END IF;
          
            
             lSQL:=  'select
                                studyid
                                , siteid
                                , usubjid
                                , visitnum
                                , visit
                                , visitseq
                                , svstdtc
                                , svendtc
                              from crf_svdata_'||t_schema ;

        
    IF LENGTH(lSQL) > 0 and b_edc_exists and t_sv_tbl is not null and t_svstdtc is not null  THEN

        t_ddl := 'DROP TABLE IF EXISTS svdatatmp';
        EXECUTE t_ddl;

        t_ddl := 'CREATE TABLE svdatatmp AS ' || lSQL;
        EXECUTE t_ddl;
                
        RETURN TRUE;
	ELSE

        t_ddl := 'DROP TABLE IF EXISTS svdatatmp';
        EXECUTE t_ddl;	

        t_ddl := 'CREATE TABLE svdatatmp ( 	
                            studyid TEXT NOT NULL,	
                            siteid TEXT NOT NULL,	
                            usubjid TEXT NOT NULL,	
                            visitnum int,	
                            visit TEXT,	
                            visitseq int,	
                            svstdtc date ,	
                            svendtc date	
                            )';	
        EXECUTE t_ddl;
        RETURN FALSE;
        
    END IF;

END
$dbvis$ LANGUAGE plpgsql;



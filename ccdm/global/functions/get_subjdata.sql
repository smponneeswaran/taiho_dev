/*
CDM get_subj_data function
Client: Regeneron
Description: gets list of all subjects with previous screening numbers
*/

DROP FUNCTION if exists get_subjdata(text);

CREATE OR REPLACE FUNCTION get_subjdata(pStudyID text)  
RETURNS BOOLEAN
VOLATILE
AS $dbvis$
DECLARE

    lSQL text := '';
    b_add_union_next BOOLEAN := FALSE;
    b_edc_exists BOOLEAN := TRUE;
    b_irt_exists BOOLEAN := TRUE;
    t_ddl text;
    t_schema text;
    t_studyid text;
    t_subj_tbl text;
    t_prev_scrnum text;

    
BEGIN
    
        t_studyid :=pStudyID;
        t_schema :=lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));
        raise notice 't_schema=%',t_schema;

        -- drop table irt_subjdata child table if exists already            
        t_ddl := 'DROP TABLE IF EXISTS irt_subjdata_'||t_schema;
        raise notice 't_ddl= %',t_ddl;
        EXECUTE t_ddl;
                          
       -- create temp table to fetch data from irt
        t_ddl := 'create  table irt_subjdata_'||t_schema||' as
                                (
                                        SELECT 
                                                irt.studyid ::text AS studyid,
                                                right(irt.siteid,6)::text as siteid,
                                                irt.usubjid::text as usubjid,
                                                nullif(prev_scrn_num, '''')::text    AS prev_scrnum
                                          FROM public.irt_data irt
                                          WHERE UPPER(irt.studyid)=UPPER('''||t_studyid||''')
                                )';
        EXECUTE t_ddl;
       raise notice 'irt_subjdata data table successfuly created with data';
       
        
        IF (SELECT COUNT(usubjid) FROM public.irt_data 
                where upper(studyid)= upper(pStudyID))::int >0 then
                b_irt_exists:= TRUE;
        End If;        
        RAISE NOTICE 'b_irt_exists=%',b_irt_exists;

        
         -- verify if edc data source exists and fetch study specific mapping data for edc
          IF EXISTS(SELECT distinct table_schema
                                     FROM information_schema.tables
                                 WHERE table_schema=t_schema
                                        AND table_name not in ('__sites',
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
                                          formid INTO t_subj_tbl
                          FROM  odr_staging.ODR_MAPPING_REQ  
                       WHERE datapoint_id='SB_03'  AND active_fl=1
                             AND  upper(studyid) = upper(t_studyid);  
                                                              
                        SELECT 
                                          fieldid INTO t_prev_scrnum
                          FROM  odr_staging.ODR_MAPPING_REQ  
                       WHERE datapoint_id='PREVSCRN'  AND active_fl=1
                             AND  upper(studyid) = upper(t_studyid);  

                        raise notice 't_subj_tbl=%', t_subj_tbl;
                        raise notice 't_prev_scrnum=%', t_prev_scrnum;
        
          ELSE
                        b_edc_exists := FALSE;
         RAISE NOTICE 'Line #97';
                        -- insert alert message "EDC Data not available"
          END IF;    
         RAISE NOTICE 'Line #99';
       -- create temp table to fetch data from edc
          IF b_edc_exists and t_subj_tbl is not null THEN
                        -- drop table crf_subjdata child table if exists already            
                        t_ddl := 'DROP TABLE IF EXISTS crf_subjdata_'||t_schema;
                        EXECUTE t_ddl;         
                        
                        RAISE NOTICE 'Line #106';
                         
                        t_ddl := 'create  table crf_subjdata_'||t_schema||' as
                                        (
                                                SELECT
                                                         "project"::text AS studyid,
                                                         right("SiteNumber",6)::text AS siteid,
                                                         "Subject"::text AS usubjid,';
                                                         
                        IF t_prev_scrnum is not null then
                                t_ddl:=t_ddl||FORMAT('case when (%I is not null and %I::text <> '''') then CONCAT(substring("Subject",1,6),RIGHT(%I::text,3)) else null end as prev_scrnum'
                                                               , t_prev_scrnum, t_prev_scrnum, t_prev_scrnum );
                        ELSE  
                                t_ddl:=t_ddl||'NULL::TEXT AS prev_scrnum';
                        END IF;
                            
                        t_ddl:=t_ddl||format(' FROM %I.%I)', t_schema, t_subj_tbl);
                        EXECUTE t_ddl;
                        raise notice 'crf_subjdata table successfuly created with data:%', t_ddl;
                        
                        -- drop table edc_subjdata_ child table if exists already            
                        t_ddl := 'DROP TABLE IF EXISTS edc_subjdata_'||t_schema;
                        EXECUTE t_ddl;      
                                                 
                         t_ddl := format(
                                        'create  table edc_subjdata_'||t_schema||' as
                                                (
                                                        SELECT
                                                                distinct
                                                                s."project"::text AS studyid,
                                                                right(e."site_key",6)::text AS siteid,
                                                                e.subject_key::text as usubjid
                                                        FROM %I."__subjects" e
                                                          JOIN %I.%I s on (right(s."SiteNumber",6)=right(e.site_key,6))
                                                )'
                                                , t_schema, t_schema, t_subj_tbl);
                        raise notice 'edc_subjdata table successfuly created with data: %',t_ddl;
                          EXECUTE t_ddl;  
          ELSE
                -- drop table crf_subjdata_ child table if exists already            
                t_ddl := 'DROP TABLE IF EXISTS crf_subjdata_'||t_schema;
                EXECUTE t_ddl;   
                          
                t_ddl := 'CREATE  TABLE crf_subjdata_'||t_schema||' AS 
                                  SELECT
                                    NULL::TEXT AS studyid,
                                    NULL::TEXT AS siteid,
                                    NULL::TEXT AS usubjid,
                                    NULL::TEXT AS prev_scrnum';
                EXECUTE t_ddl;
       raise notice 'crf_subjdata  table successfuly created with no data';

                -- drop table edc_subjdata_ child table if exists already            
                t_ddl := 'DROP TABLE IF EXISTS edc_subjdata_'||t_schema;
                EXECUTE t_ddl;   
                
                t_ddl := 'CREATE  TABLE edc_subjdata_'||t_schema||' AS 
                                  SELECT
                                    NULL::TEXT AS studyid,
                                    NULL::TEXT AS siteid,
                                    NULL::TEXT AS usubjid,
                                    NULL::TEXT AS prev_scrnum';
                EXECUTE t_ddl;

          END IF;

        -- drop table rescrn_subjdata_ child table if exists already            
        t_ddl := 'DROP TABLE IF EXISTS rescrn_subjdata_'||t_schema;
        EXECUTE t_ddl;         

         RAISE NOTICE 'Line #153';
            
          -- create temp table to store all the previously rescreened subject IDs
          t_ddl:='create  table rescrn_subjdata_'||t_schema||' as
                                (
                                        SELECT 
                                                      studyid,
                                                      siteid,
                                                      prev_scrnum
                                        FROM crf_subjdata_'||t_schema||'
                                    WHERE prev_scrnum is not null 
                                        UNION
                                        SELECT
                                                    studyid
                                                    , siteid
                                                    , prev_scrnum
                                            FROM irt_subjdata_'||t_schema||'
                                )';
       raise notice 'rescrn_subjdata data  table successfuly created';
                                
            EXECUTE t_ddl;       
            
             lSQL:=  '(
                                        (
                                                SELECT  lower(studyid)::text as studyid
                                                                 , siteid
                                                                 , usubjid 
                                                    FROM irt_subjdata_'||t_schema||'
                                                 UNION
                                                SELECT  lower(studyid)::text as studyid
                                                                 , siteid
                                                                 , usubjid 
                                                    FROM crf_subjdata_'||t_schema||'
                                                 UNION
                                                SELECT  lower(studyid)::text as studyid
                                                                 , siteid
                                                                 , usubjid 
                                                    FROM edc_subjdata_'||t_schema||'
                                        )
                                        except
                                        (
                                                SELECT  lower(studyid)::text as studyid
                                                                 , siteid
                                                                 , prev_scrnum 
                                                    FROM rescrn_subjdata_'||t_schema||'
                                        )
                                )' ;
               raise notice 'Union SQL %', lSQL;

                raise notice 'b_edc_exists=%', b_edc_exists;
                raise notice 't_subj_tbl=%', t_subj_tbl;
                raise notice 'b_irt_exists=%', b_irt_exists;

    IF  ((b_edc_exists and t_subj_tbl is not null) OR (b_irt_exists)) THEN

        -- drop table odr_stg_subjdata child table if exists already            
        t_ddl := 'DROP TABLE IF EXISTS subjdata_tmp';
        EXECUTE t_ddl;

        t_ddl := 'CREATE TABLE subjdata_tmp AS ' || lSQL;
        EXECUTE t_ddl;
        
        RETURN TRUE;
    ELSE 
        t_ddl := 'DROP TABLE IF EXISTS subjdata_tmp';
        EXECUTE t_ddl;
    
        t_ddl := 'CREATE TABLE subjdata_tmp ( 
                            studyid TEXT NOT NULL,
                            siteid TEXT NOT NULL,
                            usubjid  TEXT NOT NULL
                            )';
        EXECUTE t_ddl;

        RETURN FALSE;
    END IF;

END
$dbvis$ LANGUAGE plpgsql;

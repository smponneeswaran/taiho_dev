CREATE OR REPLACE FUNCTION get_dmdata(pstudyid text)  
RETURNS boolean
VOLATILE
AS $body$
DECLARE

    lsql text := '';
    b_edc_exists BOOLEAN := FALSE;
    t_ddl text := '';
    t_schema text;
    t_study text;
    l_context text;
    
BEGIN
    
        t_study :=pStudyID;
        t_schema :=lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));
                  
         -- verify if edc data source exists 
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
                      b_edc_exists := TRUE;
                      raise notice 'b_edc_exists=%',b_edc_exists;
                      
          END IF;    
        
       -- create  table to fetch data from edc
          IF b_edc_exists THEN
          
          SELECT CONCAT(
                'SELECT DISTINCT'
                , STUDYID
                , SITEID
                , USUBJID  
                , case when jointbl1 is not null then concat('"', jointbl1, '"."FolderSeq"::int AS visitnum, ') 
                           else  '"DM"."FolderSeq"::int AS visitnum, '
                           end
                , '"DM"."FolderName"::text AS visit, '
                , case when DMDTC is null then 'NULL' else DMDTC end || '::date AS DMDTC, '
                , case when BRTHDTC is null then 'NULL' else BRTHDTC end || '::date AS BRTHDTC, '
                , case when AGE is null then 'NULL' else AGE end || '::int AS AGE, '
                , case when SEX is null then 'NULL' else SEX end || '::text AS SEX, '
                , case when RACE is null then 'NULL' else RACE end || '::text AS RACE, '  
                , case when ETHNICITY is null then 'NULL' else ETHNICITY end || '::text AS ETHNICITY, ' 
                , case when ARM is null then 'NULL' else ARM end || '::text AS ARMCD, '
                , case when ARM is null then 'NULL' else ARM end || '::text AS ARM ' 
                , 'FROM "',t_schema, '"."DM"'  
                , case when jointbl1 is not null 
                            then concat('LEFT JOIN "', t_schema, '"."', jointbl1, '" on ("DM"."project"="', jointbl1,'"."project" and "DM"."SiteNumber"="',jointbl1, '"."SiteNumber" and "DM"."Subject"="',jointbl1, '"."Subject")' )
                            else NULL
                  end
                )::text INTO lsql
                FROM (
                SELECT studyid as studyid_src, 
                        '"DM"."project"::text AS studyid, ' AS STUDYID,
                        '"DM"."SiteNumber"::text AS siteid, ' AS SITEID,
                        '"DM"."Subject"::text AS usubjid, ' AS USUBJID,   
                        max(formid) AS FORMID,   
                        max(case when datapoint_id='DM_01' then 'COALESCE("DM"."RecordDate"::DATE,"DM"."MinCreated"::DATE)' else NULL end) AS DMDTC  
                        , max(case when datapoint_id='DM_02' then '"'||fieldid||'"' else NULL end) AS BRTHDTC  
                        , max(case when datapoint_id='DM_03' then technical_expression else NULL end) AS AGE
                        , max(case when datapoint_id='DM_04' then '"'||fieldid||'"' else NULL end) AS SEX
                        , max(case when datapoint_id='DM_05' then technical_expression else NULL end) AS RACE
                        , max(case when datapoint_id='DM_06' then '"'||fieldid||'"' else NULL end) AS ETHNICITY 
                        , NULL AS ARMCD
                        , max(case when datapoint_id='DM_08' then '"'||formid||'"."'||fieldid||'"' else NULL end) AS ARM
                        , max(case when datapoint_id='DM_08' and fieldid is not null and formid is not null then formid else NULL end) AS jointbl1
                FROM ODR_STAGING.ODR_MAPPING_REQ 
                WHERE STUDYID = t_study 
                   AND active_fl=1 AND datapoint_id LIKE 'DM%'
                GROUP BY studyid ) A   ;
                
                RAISE NOTICE 'Sasi';
                RAISE NOTICE 'LSQL=%', length(lsql);
                
                If length(lsql)<>0 then
                        t_ddl := 'CREATE TABLE stg_dmdata AS ' || lsql ;
                        RAISE NOTICE '%', t_ddl;
                        EXECUTE t_ddl;
                ELSE        
                        t_ddl := 'DROP TABLE IF EXISTS stg_dmdata';
                        EXECUTE t_ddl;
                    
                        t_ddl := 'CREATE TABLE stg_dmdata ( 
                                            studyid TEXT NOT NULL,
                                            siteid TEXT NOT NULL,
                                            usubjid TEXT NOT NULL,
                                            visitnum int,
                                            visit TEXT,
                                            dmdtc date ,
                                            brthdtc date,
                                            age integer,
                                            sex text,
                                            race text,
                                            ethnicity text,
                                            armcd text,
                                            arm text
                                            )';
                        EXECUTE t_ddl;
                END IF;
             

        RETURN TRUE;
     ELSE 
                t_ddl := 'DROP TABLE IF EXISTS stg_dmdata';
                EXECUTE t_ddl;
            
                t_ddl := 'CREATE TABLE stg_dmdata ( 
                                    studyid TEXT NOT NULL,
                                    siteid TEXT NOT NULL,
                                    usubjid TEXT NOT NULL,
                                    visitnum int,
                                    visit TEXT,
                                    dmdtc date ,
                                    brthdtc date,
                                    age integer,
                                    sex text,
                                    race text,
                                    ethnicity text,
                                    armcd text,
                                    arm text
                                    )';
                EXECUTE t_ddl;
        
                RETURN FALSE;
    END IF;
/*
EXCEPTION
        WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'ERROR:%', l_context;
        RAISE NOTICE 'MEssage:%', SQLERRM;
        RETURN FALSE;
*/

END
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_aedata(pstudyid text)  
RETURNS boolean
VOLATILE
AS $body$
DECLARE

     lsql text := '';
    b_edc_exists BOOLEAN := FALSE;
    t_ddl text := '';
    t_schema text;
    t_study text;
    rel_yes text := '';
    rel_no text := '';
    sev_str text := '';
    aerel_cur CURSOR (p_studyid TEXT)  FOR  SELECT nullif(fieldid,'') fieldid FROM  odr_staging.odr_mapping_req  WHERE studyid = p_studyid AND datapoint_id = 'AE_10';
    aesev_cur CURSOR (p_studyid TEXT)  FOR  SELECT nullif(fieldid,'') fieldid FROM  odr_staging.odr_mapping_req  WHERE studyid = p_studyid AND datapoint_id = 'AE_08';
    rel_cnt int := 1;
    sev_cnt int := 1;
    l_context text := '';

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

        IF b_edc_exists THEN
          -- create CASE statement for aerelnst
          FOR i IN aerel_cur(pstudyid) LOOP
                IF rel_cnt = 1 THEN 
                        rel_yes := 'WHEN UPPER("'||i.fieldid||'"::text) IN (''1'',''YES'',''RELATED'')  ';
                        rel_no := 'WHEN UPPER("'||i.fieldid||'"::text) IN (''0'',''NO'',''NOT RELATED'') ';
                        rel_cnt = rel_cnt + 1;
                ELSE
                        rel_yes := rel_yes|| ' OR UPPER("'||i.fieldid||'"::text) IN (''1'',''YES'',''RELATED'')  ';
                        rel_no := rel_no|| ' AND UPPER("'||i.fieldid||'"::text) IN (''0'',''NO'',''NOT RELATED'') ';
                END IF;
          END LOOP;
          -- create COALESCE statement for aerelnst         
          FOR i IN aesev_cur(pstudyid) LOOP
                IF sev_cnt = 1 THEN 
                        sev_str := 'COALESCE("'||i.fieldid;
                        sev_cnt = sev_cnt + 1;
                ELSE
                        sev_str := sev_str||'","'||i.fieldid;
                END IF;
          END LOOP;       
        --CRETAE SELECT statement for pull AE data from source
    SELECT CONCAT(
                'SELECT DISTINCT '
                , STUDYID
                , SITEID
                , USUBJID  
                , AESEQ
                , case when AETERM is null then 'NULL' else AETERM end || '::text AS AETERM, '
                , case when AEVERBATIM is null then 'NULL' else AEVERBATIM end || '::text AS AEVERBATIM, '
                , case when AEBODSYS is null then 'NULL' else AEBODSYS end || '::text AS AEBODSYS, '
                , case when AESTDTC is null then 'NULL' else AESTDTC end || '::date AS AESTDTC, '
                , case when AESTTM is null then 'NULL' else AESTTM end || '::time AS AESTTM, '
                , case when AEENDTC is null then 'NULL' else AEENDTC end || '::date AS AEENDTC, '
                , case when AEENTM is null then 'NULL' else AEENTM end || '::time AS AEENTM, '
                , case when nullif(sev_str,'') IS NOT NULL then sev_str||'")' else NULL end || '::text AS AESEV, '
                , case when AESER is null then 'NULL' else 'CASE WHEN UPPER('||AESER||') = ''YES'' THEN ''Serious'' WHEN UPPER('||AESER||') = ''NO'' THEN ''Non-Serious'' ELSE NULL END' end || '::text AS AESER, '
                , case when nullif(rel_yes,'') IS NOT NULL AND nullif(rel_no,'') IS NOT NULL THEN 'CASE '||rel_yes||'THEN ''Yes'' '||rel_no||'THEN ''No'' ELSE NULL END' ELSE NULL end || '::text AS AERELNST '
                , 'FROM "',t_schema, '"."AE"',' WHERE "RecordPosition" != 0 '
                )::text INTO lsql
                FROM (
                SELECT studyid as studyid_src, 
                        '"AE"."project"::text AS studyid, ' AS STUDYID,
                        '"AE"."SiteNumber"::text AS siteid, ' AS SITEID,
                        '"AE"."Subject"::text AS usubjid, ' AS USUBJID,
                        '"AE"."RecordPosition"::int AS aeseq, ' AS AESEQ,
                        max(case when datapoint_id='AE_01' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AETERM,
                        max(case when datapoint_id='AE_02' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AEVERBATIM,
                        max(case when datapoint_id='AE_03' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AEBODSYS,
                        max(case when datapoint_id='AE_04' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AESTDTC,
                        max(case when datapoint_id='AE_05' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AESTTM,
                        max(case when datapoint_id='AE_06' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AEENDTC,
                        max(case when datapoint_id='AE_07' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AEENTM,
                        max(case when datapoint_id='AE_09' then '"'||nullif(fieldid,'')||'"' else NULL end) AS AESER
                FROM odr_staging.odr_mapping_req 
                WHERE STUDYID = t_study 
                   AND active_fl=1 AND datapoint_id LIKE 'AE%'
                GROUP BY studyid 
                ) A  ;

             RAISE NOTICE '%', lsql ; 

       END IF;

         IF LENGTH(lSQL) > 0 and b_edc_exists 
         THEN

         -- drop table odr_stg_dmdata child table if exists already            
        t_ddl := 'DROP TABLE IF EXISTS stg_aedata';
        EXECUTE t_ddl;

         t_ddl := 'CREATE TABLE stg_aedata AS ' || lSQL;
        EXECUTE t_ddl;

         RETURN TRUE;

    ELSE 
        t_ddl := 'DROP TABLE IF EXISTS stg_aedata';
        EXECUTE t_ddl;

         t_ddl := 'CREATE TABLE stg_aedata ( 
                            studyid TEXT NOT NULL,
                            siteid TEXT NOT NULL,
                            usubjid TEXT NOT NULL,
							aeseq INT,
                            aeterm text,
                            aeverbatim TEXT,
                            aebodsys text ,
                            aestdtc date,
                            aeendtc date,
                            aesev text,
                            aeser text,
                            aerelnst text,
                            aesttm text,
                            aeentm text
                            )';
        EXECUTE t_ddl;

         RETURN FALSE;
    END IF;

   EXCEPTION
        WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_context = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'ERROR:%', l_context;
        RAISE NOTICE 'MEssage:%', SQLERRM;
        RETURN FALSE;  
END 
$body$
LANGUAGE plpgsql;

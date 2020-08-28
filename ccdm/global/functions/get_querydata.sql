/*
CDM get_querydata function
Client: Taiho
*/

CREATE OR REPLACE FUNCTION get_querydata(pStudyID text)  
RETURNS BOOLEAN
VOLATILE
AS $dbvis$
DECLARE

	t_ddl text := '';
	t_schema text;
	t_study text;
	qsql text := '';
	err_context text := '';

BEGIN
	t_study := pStudyID;
	t_schema := lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));
	
	raise notice 't_study %',t_study;
	raise notice 't_schema %',t_schema;

	IF NOT EXISTS(SELECT table_name
		FROM information_schema.tables
		WHERE table_name = 'stg_querydata')
	THEN
		t_ddl := 'CREATE TABLE odr_staging.stg_querydata (
			studyid text NOT NULL,
			siteid text NOT NULL,
			usubjid text NOT NULL,
			queryid text, 
			visit text,
			formid text,
			fieldid text,
			querytext text,
			querytype text,
			querystatus text,
			queryopeneddate date,
			queryresponsedate date,
			querycloseddate date,
			formseq int,
			log_num int
		) 
		PARTITION BY LIST (studyid)';

		EXECUTE t_ddl;
	END IF;

	IF EXISTS(SELECT table_name
		FROM information_schema.tables
		WHERE table_name = 'stream_query_detail' 
		AND table_schema = t_schema)
	THEN
         --SQL to fetch query data for given study
	qsql := 'SELECT
			left("study"::text, strpos("study", '' - '') - 1)::text AS studyid, 
			substr("subjectname",1, 6 )::text AS siteid,
			"subjectname"::text AS usubjid, 
			"id_"::text AS queryid,
			"folder"::text    as visit,
			"form"::text AS formid, 
			"field"::text AS fieldid, 
			"querytext"::text AS querytext, 
			"markinggroupname"::text AS querytype, 
			"name"::text AS querystatus, 
			convert_to_date("qryopendate")::date AS queryopeneddate, 
			convert_to_date("qryresponsedate")::date AS queryresponsedate,
			convert_to_date("qrycloseddate")::date AS querycloseddate,
			1::int as formseq,
			"log"::int as log_num
		FROM "'||t_schema||'"."stream_query_detail"
		WHERE lower(left("markinggroupname", 9)) = ''site from''';
	raise notice 'IF %',t_study;
	ELSE 
		qsql := 'SELECT 
			NULL::text AS studyid, 
			NULL::text AS siteid,
			NULL::text AS usubjid, 
			NULL::text AS queryid,
			NULL::text AS visit,
			NULL::text AS formid, 
			NULL::text AS fieldid, 
			NULL::text AS querytext, 
			NULL::text AS querytype, 
			NULL::text AS querystatus, 
			NULL::date AS queryopeneddate, 
			NULL::date AS queryresponsedate,
			NULL::date AS querycloseddate,
			NULL::int as formseq,
			NULL::int as log_num
		WHERE 1=2';
	raise notice 'ELSE %',t_study;
	END IF;

	--Drop fielddef stage table
	t_ddl := 'DROP TABLE IF EXISTS odr_staging.stg_querydata_'||t_schema;
		EXECUTE t_ddl;

	--Create stage table for fielddef and populate it
	t_ddl := 'CREATE TABLE odr_staging.stg_querydata_'||t_schema ||' AS ('|| qsql||' )';
		EXECUTE t_ddl;

	t_ddl := 'ALTER TABLE odr_staging.stg_querydata_'||t_schema ||' ALTER COLUMN studyid SET NOT NULL';
		EXECUTE t_ddl;

	t_ddl := 'ALTER TABLE odr_staging.stg_querydata_'||t_schema ||' ALTER COLUMN siteid SET NOT NULL';
		EXECUTE t_ddl;
        
	t_ddl := 'ALTER TABLE odr_staging.stg_querydata_'||t_schema ||' ALTER COLUMN usubjid SET NOT NULL';
		EXECUTE t_ddl;


	t_ddl := 'CREATE INDEX ON odr_staging.stg_querydata_'||t_schema||'(studyid, siteid, usubjid)';
		EXECUTE t_ddl;

	t_ddl := 'ALTER TABLE odr_staging.stg_querydata ATTACH PARTITION odr_staging.stg_querydata_'||t_schema||' FOR VALUES IN ('''||t_study||''')';
		EXECUTE t_ddl;	

	RETURN TRUE;

EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
		RAISE notice 'Error Name:%',SQLERRM;
		RAISE notice 'Error State:%', SQLSTATE;
		RAISE notice 'Error Context:%', err_context;
	RETURN FALSE;

END
$dbvis$
LANGUAGE plpgsql;
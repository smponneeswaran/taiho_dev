/*
CDM get_studymetadata function
Client: Taiho  
*/

CREATE OR REPLACE FUNCTION get_studymetadata(pStudyID text)  
RETURNS BOOLEAN
VOLATILE
AS $dbvis$
DECLARE

	t_ddl text := '';
	t_schema text;
	t_study text;
	fieldsql text := '';
	formsql text := '';
	formdef_data text := '';
	err_context text := '';
	b_edc_exists BOOLEAN := TRUE;

BEGIN
	t_study  := pStudyID;
	t_schema := lower(replace(replace(pStudyID, '-', '_'), ' ', '_'));

	IF NOT EXISTS(SELECT table_name
		FROM information_schema.tables
		WHERE table_name = 'stg_fielddef')
	THEN
		t_ddl := 'CREATE TABLE odr_staging.stg_fielddef(
			studyid TEXT NOT NULL,
			formid TEXT NOT NULL,
			fieldid TEXT NOT NULL,
			fieldname TEXT, 
			isprimaryendpoint BOOLEAN,
			issecondaryendpoint BOOLEAN,
			issdv BOOLEAN,
			isrequired BOOLEAN
		) 
		PARTITION BY LIST (studyid)';

		EXECUTE t_ddl;
	END IF;

---------------------Populate formdef staging table-------------------------------------------------------

	IF NOT EXISTS(SELECT table_name
		FROM information_schema.tables
		WHERE table_name = 'stg_formdef') 
	THEN
		t_ddl := 'CREATE TABLE odr_staging.stg_formdef(
			studyid TEXT NOT NULL,
			formid TEXT NOT NULL,
			formname TEXT,
			isprimaryendpoint BOOLEAN,
			issecondaryendpoint BOOLEAN,
			issdv BOOLEAN,
			isrequired BOOLEAN
		) 
		PARTITION BY LIST (studyid)';

		EXECUTE t_ddl;
	END IF;

    IF NOT EXISTS(SELECT distinct table_schema
		FROM information_schema.tables
		WHERE table_schema=t_schema
		AND table_name in (
			'metadata_fields',
			'metadata_folders',
			'metadata_forms')
		)
	THEN
		b_edc_exists := FALSE;
		-- insert alert message "EDC Data not available"
		RAISE notice '3';                        
	END IF;            

	IF b_edc_exists THEN

		--SQL to fetch Fielddef data for given study
		fieldsql := 'SELECT mf.* FROM (
			SELECT 
				DISTINCT '''||t_study||'''::text AS studyid,
				"FormDefOID"::text AS formid,
				substr("OID", strpos("OID", ''.'')+1)::text AS fieldid,
				coalesce("SASLabel", "Name")::text AS fieldname,
				false::boolean AS isprimaryendpoint,
				false::boolean AS issecondaryendpoint,
				CASE WHEN "Mandatory"::boolean = true and coalesce("SourceDocument", false)::boolean = true THEN true
				ELSE false END ::boolean AS issdv,
				"Mandatory"::boolean AS isrequired
			FROM "'||t_schema||'"."metadata_fields"
			) mf';

		RAISE notice 'fieldsql: %', fieldsql;

		--Drop fielddef stage table
		t_ddl := 'DROP TABLE IF EXISTS odr_staging.stg_fielddef_'||t_schema;
		EXECUTE t_ddl;
		
		--Create stage table for fielddef and populate it
		t_ddl := 'CREATE TABLE odr_staging.stg_fielddef_'||t_schema ||' AS (
			'|| fieldsql||' JOIN (SELECT studyid FROM study) st ON (lower(trim(mf.studyid)) = lower(trim(st.studyid))))';
		EXECUTE t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_fielddef_'||t_schema ||' ALTER COLUMN studyid SET NOT NULL';
		EXECUTE t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_fielddef_'||t_schema ||' ALTER COLUMN formid SET NOT NULL';
		EXECUTE t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_fielddef_'||t_schema ||' ALTER COLUMN fieldid SET NOT NULL';
		EXECUTE t_ddl;
		
		t_ddl := 'CREATE INDEX ON odr_staging.stg_fielddef_'||t_schema||'(studyid, formid, fieldid)';
		EXECUTE t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_fielddef ATTACH PARTITION odr_staging.stg_fielddef_'||t_schema||' FOR VALUES IN ('''||t_study||''')';
		EXECUTE t_ddl;	

		--Create temp table to store formdef_data
		t_ddl := 'CREATE TEMP TABLE formdef_data_'||t_schema||' AS 
			'||'SELECT '''||t_study||'''::text AS studyid,
					"OID"::text AS formid,
					"Name"::text AS formname,
					false::boolean AS isprimaryendpoint,
					false::boolean AS issecondaryendpoint,
					false::boolean AS issdv,
					false::boolean AS isrequired
				FROM "'||t_schema||'"."metadata_forms"';
		EXECUTE t_ddl;


		formsql := 'SELECT 
				studyid::text AS studyid,
				formid::text AS formid,
				formname::text AS formname,
				isprimaryendpoint::boolean AS isprimaryendpoint,
				issecondaryendpoint::boolean AS issecondaryendpoint,
				issdv::boolean AS issdv,
				isrequired::boolean AS isrequired
			FROM formdef_data_'||t_schema  ;
--          UNION 
--          SELECT DISTINCT q.studyid::text AS studyid,
--              q.formid::text AS formid,
--              q.formid::text AS formname,
--              false::boolean AS isprimaryendpoint,
--              false::boolean AS issecondaryendpoint,
--              false::boolean AS issdv,
--              false::boolean AS isrequired
--          FROM query q
--          LEFT JOIN formdef_data_'||t_schema||' fd ON (q.studyid = fd.studyid AND q.formid = fd.formname)
--          WHERE fd.studyid IS NULL';

		--Drop fielddef stage table
		t_ddl := 'DROP TABLE IF EXISTS odr_staging.stg_formdef_'||t_schema;
		EXECUTE t_ddl;
		
		--Create stage table for fielddef and populate it
		t_ddl := 'CREATE TABLE odr_staging.stg_formdef_'||t_schema ||' AS ' || formsql;
		EXECUTE t_ddl;
		RAISE notice '%', t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_formdef_'||t_schema ||' ALTER COLUMN studyid SET NOT NULL';
		EXECUTE t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_formdef_'||t_schema ||' ALTER COLUMN formid SET NOT NULL';
		EXECUTE t_ddl;
		
		t_ddl := 'CREATE INDEX ON odr_staging.stg_formdef_'||t_schema||'(studyid, formid)';
		EXECUTE t_ddl;
		
		t_ddl := 'ALTER TABLE odr_staging.stg_formdef ATTACH PARTITION odr_staging.stg_formdef_'||t_schema||' FOR VALUES IN ('''||t_study||''')';
		EXECUTE t_ddl;	
		
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;


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
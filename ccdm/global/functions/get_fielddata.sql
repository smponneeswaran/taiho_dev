/*
CDM get_fielddata function
Client: Taiho
*/

DROP FUNCTION IF EXISTS get_fielddata(text);

CREATE OR REPLACE FUNCTION get_fielddata (pstudyid TEXT)
RETURNS BOOLEAN
VOLATILE
AS $$
DECLARE
	rec RECORD;
	lSQL TEXT := '';
	b_add_union_next BOOLEAN := FALSE;
	t_ddl TEXT;

BEGIN


	FOR rec IN (
		SELECT 
			DISTINCT i.formid::TEXT AS table_name,
			i.fieldid::TEXT AS column_name,
			LOWER(REPLACE(REPLACE(SUBSTRING(i.studyid::TEXT, 1),'-','_'), ' ', '_')) 
			AS table_schema, 
			i.studyid::TEXT AS studyid
		FROM fielddef i 
		JOIN information_schema.columns tb ON (
			tb.table_schema = LOWER(REPLACE(REPLACE(SUBSTRING(i.studyid::TEXT, 1),'-','_'), ' ', '_'))
			AND tb.table_name = i.formid AND tb.column_name = i.fieldid)
		WHERE i.studyid = pStudyID
	)
	LOOP
		IF b_add_union_next THEN
			lSQL := lSQL || ' UNION ALL '; 
		END IF; 

		lSQL := lSQL || format('( 
			SELECT 
				%L::TEXT                        AS studyid,
				"SiteNumber"::TEXT              AS siteid,                        
				"Subject"::TEXT                 AS usubjid,
				RIGHT("SiteNumber",6)::TEXT     AS siteidjoin,
				%L::TEXT                        AS formid,
				%L::TEXT                        AS fieldid,
				TRIM(COALESCE(NULLIF("FolderName", ''''), "DataPageName"))::TEXT  AS visit,
				count(*)  over (partition by "project", "SiteNumber", "Subject", "FolderName")::int AS visit_cnt,
				"instanceId"::INTEGER           AS instanceid,
				"InstanceRepeatNumber"::INTEGER           AS InstanceRepeatNumber,
				"DataPageId"::INTEGER           AS datapageid,
				"RecordId"::INTEGER             AS recordid,
				1::INTEGER                      AS visitseq,
				DENSE_RANK() OVER (PARTITION BY "Subject",COALESCE(NULLIF("FolderName", ''''), "DataPageName") ORDER BY "RecordId")::INTEGER AS formseq,
				ROW_NUMBER() OVER (PARTITION BY "Subject",COALESCE(NULLIF("FolderName", ''''), "DataPageName") ORDER BY "RecordId")::INTEGER AS fieldseq,
				"FolderSeq"::INTEGER AS log_num,
				%I::TEXT                        AS dataValue,
				NULL::DATE                      AS dataentrydate, --populated by fielddata.sql
				COALESCE("RecordDate"::DATE,"MinCreated"::DATE) AS datacollecteddate,
				NULL::DATE                      AS sdvdate      -- populated by fielddata.sql
			FROM %I.%I tbl /*LIMIT LIMIT 10 LIMIT*/)'
		, rec.studyid, rec.table_name, rec.column_name, rec.column_name, rec.table_schema, rec.table_name);

		b_add_union_next := TRUE;        
		raise notice '%',lSQL;

	END LOOP;

	t_ddl := 'DROP TABLE IF EXISTS stg_fielddata CASCADE';
	EXECUTE t_ddl;
	
        
    IF LENGTH(lSQL) > 0 THEN

		t_ddl := 'CREATE TABLE stg_fielddata AS ' || lSQL;
		EXECUTE t_ddl;

		t_ddl := 'CREATE INDEX ON stg_fielddata(usubjid,instanceid,datapageid,recordid)';
		EXECUTE t_ddl;

		t_ddl := 'ANALYZE stg_fielddata';
		EXECUTE t_ddl;

		RETURN TRUE;
	ELSE 
        t_ddl := 'CREATE TABLE stg_fielddata AS 
			SELECT
				NULL::TEXT AS studyid,
				NULL::TEXT AS siteid,
				NULL::TEXT AS usubjid,
				NULL::TEXT AS siteidjoin,
				NULL::TEXT AS formid,
				NULL::TEXT AS fieldid,
				NULL::TEXT AS visit,
				NULL::INTEGER AS visit_cnt,
				NULL::INTEGER AS instanceid,
				NULL::INTEGER AS InstanceRepeatNumber,
				NULL::INTEGER AS datapageid,
				NULL::INTEGER AS recordid,
				NULL::INTEGER AS visitseq,
				NULL::INTEGER AS formseq,
				NULL::INTEGER AS fieldseq,
				NULL::INTEGER AS log_num,
				NULL::TEXT AS datavalue,
				NULL::DATE AS dataentrydate,
				NULL::DATE AS datacollecteddate,
				NULL::DATE AS sdvdate WHERE 1=2';
		EXECUTE t_ddl;

		RETURN FALSE;
	END IF;

END
$$ LANGUAGE plpgsql;
/*
CDM get_formdata function
Client: Taiho
*/

CREATE OR REPLACE FUNCTION get_formdata(pStudyID text)  --by only looking at audittrail we are not including data ourside of RAVE... 
RETURNS TABLE(
    studyid text, 
    siteid text, 
    usubjid text, 
    formid text, 
    visit text, 
    recordid integer,
    visitseq integer, 
    formseq integer, 
    dataentrydate date, 
    datacollecteddate date,
    sdvdate date
)
	VOLATILE
AS $dbvis$

DECLARE
	rec record;
	lSQL text := '';
BEGIN
	FOR rec IN (
		SELECT
			i.formid::text AS table_name, 
			lower(replace(substring(i.studyid::text, 1),'-','_')) AS table_schema, 
			i.studyid::text AS studyid
		FROM formdef i 
		JOIN information_schema.tables tb ON (
		tb.table_schema = lower(replace(substring(i.studyid::text, 1),'-','_')) AND tb.table_name = i.formid)
		WHERE i.studyid = pStudyID)
    LOOP
        lSQL := lSQL || format('
			SELECT DISTINCT
				%L::text AS studyid,
				right("SiteNumber",3)::text AS siteid,
				right("Subject",3)::text AS usubjid,
				%L::text AS formid,
				COALESCE(NULLIF("FolderName",''''), NULLIF("DataPageName",'''')) AS visit,
				"RecordId"::integer AS recordid,
				1::integer AS visitseq,
				"RecordPosition"::integer AS formseq,
				null::date AS dataentrydate,
				COALESCE("RecordDate"::date,"MinCreated"::date) AS datacollecteddate,
				null::date AS sdvdate
			FROM %I.%I tbl
			UNION ALL ', rec.studyid, rec.table_name, rec.table_schema, rec.table_name
		);
    END LOOP;

    --remove last "UNION ALL" 
    IF length(lSQL) >= 11 THEN
        lSQL = substring(lSQL, 0, length(lSQL) - 11);
    END IF;

    IF length(lSQL) > 0 THEN
        RETURN QUERY EXECUTE lSQL;
    ELSE 
        RETURN;
    END IF;

END
$dbvis$ LANGUAGE plpgsql;
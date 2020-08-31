/*
CDM get_dataentrydate function
Client: Regeneron
*/

DROP FUNCTION IF EXISTS get_sites();

CREATE OR REPLACE FUNCTION get_sites()  --by only looking at audittrail we are not including data ourside of RAVE... 
RETURNS BOOLEAN
VOLATILE
AS $$
DECLARE
    rec RECORD;
    lSQL TEXT := '';
    tbl_schema TEXT;
    pStudyID TEXT;
    t_ddl TEXT;
    
BEGIN

        select trim(trim(current_schema(),'cqs_'),'_new') into tbl_schema;
        pStudyID := replace(upper(tbl_schema),'_','-');
                
        IF EXISTS (SELECT distinct table_schema
                                     FROM information_schema.tables
                                 WHERE table_schema=tbl_schema
                                          and table_name not in ('__sites',
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
                                                                                )) THEN
                lSQL := 'SELECT '''||pStudyID||'''::text AS studyid ';
                lSQL := lSQL || format(
                                                            '
                                                            , oid::text AS siteid
                                                            , name::text AS sitename
                                                        FROM %I."__sites"'
                                                , tbl_schema
                                                );
        ELSE
                lSQL := NULL;
        END IF;
        
    t_ddl := 'DROP TABLE IF EXISTS temp_edc_sites';
    EXECUTE t_ddl;
    
    IF LENGTH(lSQL) > 0 THEN

        t_ddl := 'CREATE TABLE temp_edc_sites AS ' || lSQL;
        EXECUTE t_ddl;

        t_ddl := 'CREATE INDEX ON temp_edc_sites(studyid, siteid)';
        EXECUTE t_ddl;

        t_ddl := 'ANALYZE temp_edc_sites';
        EXECUTE t_ddl;

        RETURN TRUE;
    ELSE 
        t_ddl := 'CREATE TABLE temp_edc_sites AS 
                  SELECT
                    NULL::TEXT AS studyid,
                    NULL::TEXT AS siteid,
                    NULL::TEXT AS sitename';
        EXECUTE t_ddl;
        
        RETURN FALSE;
    END IF;

END
$$ LANGUAGE plpgsql;


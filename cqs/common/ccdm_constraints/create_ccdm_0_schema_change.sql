
--  --------------------------------------------------------------------------
--  The following line is an annotation of sorts to disable parallel testing
--  by the test framework. Do not remove:
--  parallel_test=0
-- 
--  This package of functions implements the schema handling feature
--  for CCDM. The functions are all defined and then executed
--  in the proper order at the end of the file. The overall strategy
--  is to:
--
--    1. Detect the 'current' schema
--    2. Derive a previously-created 'gold' schema
--       that defines the canonical definition of
--       the USDM schema to which the current schema
--       should conform.
--    3. Modify the current schema by:
--       - creating missing tables
--       - adding missing columns
--       - adjusting conflicting datatypes
--       - adding column defaults
--  --------------------------------------------------------------------------

--  --------------------------------------------------------------------------
--  Select the current schema and also derive the golden
--  schema's name from it. If _tmp is present in the
--  current schema it is removed and _gold is added
--  to the end. This is to work around the fact that the
--  CQS adapter seeder operates in <schema>_tmp while
--  the 'gold' schema name will be <schema>_gold.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_information(
  OUT t_current_schema TEXT,
  OUT t_gold_schema TEXT) AS
$$
BEGIN
  SELECT INTO t_current_schema
    current_schema();

  SELECT
    FORMAT('%s_gold', REPLACE(t_current_schema, '_tmp', ''))
  INTO
    t_gold_schema;
END
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Assert that the given schema exists by raising an exception if
--  it does not.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_assert_exists(
  t_schema_name TEXT = NULL) RETURNS VOID AS
$$
BEGIN
  IF t_schema_name IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;
  
  IF NOT EXISTS(
    SELECT
      schema_name
    FROM
      information_schema.schemata
    WHERE
      schema_name = t_schema_name)
  THEN
    RAISE NOTICE '[%] WARNING: Schema %s does not exist.', TIMEOFDAY()::TIMESTAMP, t_schema_name;
  END IF;
END
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Select column type and 'default' information for the
--  specified schema/table/column
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_column_information(
  IN t_schema_name TEXT = NULL,
  IN t_table_name TEXT = NULL,
  IN t_column_name TEXT = NULL,
  OUT t_column_type TEXT,
  OUT t_column_default TEXT) AS
$$
BEGIN
  IF t_schema_name IS NULL OR t_table_name IS NULL OR t_column_name IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  SELECT
    data_type, column_default::TEXT
  INTO
    t_column_type, t_column_default
  FROM
    information_schema.columns
  WHERE
    table_schema = t_schema_name AND
    table_name = t_table_name AND
    column_name = t_column_name;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Select those tables that are present in gold schema
--  but missing in current. Lower case names are assumed here
--  to conform to USDM and to avoid extra tables being created
--  if the mappings create a mixed case table name.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_missing_tables(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL)
  RETURNS TABLE(t TEXT) AS
$$
BEGIN
  IF t_current_schema IS NULL OR t_gold_schema IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  RETURN QUERY
    SELECT
      LOWER(gold.table_name::TEXT)
    FROM
      information_schema.tables gold 
    WHERE
      gold.table_schema = t_gold_schema
    EXCEPT
    SELECT
      LOWER(curr.table_name::TEXT)
    FROM
      information_schema.tables curr 
    WHERE
      curr.table_schema = t_current_schema;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Select those tables present in both the gold schema and the current.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_common_tables(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL)
  RETURNS TABLE(t TEXT) AS
$$
BEGIN  
  IF t_current_schema IS NULL OR t_gold_schema IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  RETURN QUERY
    SELECT
      gold.table_name::TEXT
    FROM
      information_schema.tables gold
    WHERE
      gold.table_schema = t_gold_schema
    INTERSECT
    SELECT
      curr.table_name::TEXT
    FROM
      information_schema.tables curr
    WHERE curr.table_schema = t_current_schema;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Select those column missing in the current schema that are
--  present in the gold. This is done in a case insensitive way
--  to assume USDM compliance and results are returned in lower case.
--  This will avoid the creation of redundant columns.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_missing_columns(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL,
  t_table_name TEXT = NULL)
  RETURNS TABLE(column_name TEXT) AS
$$
BEGIN
  IF t_current_schema IS NULL OR t_gold_schema IS NULL OR t_table_name IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  RETURN QUERY
    SELECT
      LOWER(gold.column_name::TEXT)
    FROM
      information_schema.columns gold
    WHERE
      gold.table_schema = t_gold_schema AND
      gold.table_name = t_table_name
    EXCEPT
    SELECT
      LOWER(curr.column_name::TEXT)
    FROM
      information_schema.columns curr
    WHERE
      curr.table_schema = t_current_schema AND
      curr.table_name = t_table_name;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Select those columns that the gold and current schemas have in common
--  for the given table name.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_common_columns(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL,
  t_table_name TEXT = NULL)
  RETURNS TABLE(column_name TEXT) AS
$$
BEGIN
  IF t_current_schema IS NULL OR t_gold_schema IS NULL OR t_table_name IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  RETURN QUERY
    SELECT
      gold.column_name::TEXT 
    FROM
      information_schema.columns gold
    WHERE
      gold.table_schema = t_gold_schema AND
      gold.table_name = t_table_name
    INTERSECT
    SELECT
      curr.column_name::TEXT
    FROM
      information_schema.columns curr
    WHERE
      curr.table_schema = t_current_schema AND
      curr.table_name = t_table_name;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Create the tables present in gold schema that are missing from
--  current. Here we use the CREATE TABLE 'LIKE' function
--  to replicate the table definition. We only include the
--  DEFAULTS because comments, index, etc are added later explicitly
--  via the other create_ccdm_... sql files.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_create_missing_tables(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL)
  RETURNS VOID AS
$$
DECLARE
  t_table_name TEXT;
  t_column_name TEXT;
  t_ddl TEXT;
  t_missing_tables TEXT[];

BEGIN
  RAISE NOTICE '[%] Creating missing tables...', TIMEOFDAY()::TIMESTAMP;

  IF t_current_schema IS NULL OR t_gold_schema IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  t_missing_tables := ARRAY(
    SELECT
      schema_missing_tables
    FROM
      schema_missing_tables(t_current_schema, t_gold_schema));
      
  FOREACH t_table_name IN ARRAY t_missing_tables LOOP

    -- Create the new tables exactly like the gold table
    -- including the defaults (but no indexes, FKs, etc)
    
    t_ddl := FORMAT('CREATE TABLE %s.%s (LIKE %s.%s INCLUDING DEFAULTS);',
        QUOTE_IDENT(t_current_schema),
        t_table_name, t_gold_schema, t_table_name);

    RAISE NOTICE '[%] %', TIMEOFDAY()::TIMESTAMP, t_ddl;
    EXECUTE t_ddl;
  END LOOP;

  RAISE NOTICE '[%] Completed missing table creation.', TIMEOFDAY()::TIMESTAMP;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Add columns that present in gold schema but are missing from
--  current. We do not replicate the source's default value
--  because we will do that explicitly in a later step.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_add_missing_columns(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL)
  RETURNS VOID AS
$$
DECLARE
  t_table_name TEXT;
  t_column_name TEXT;
  t_column_type TEXT;
  t_column_default TEXT;
  t_ddl TEXT;
  t_missing_columns TEXT[];
  t_common_tables TEXT[];
  
BEGIN
  RAISE NOTICE '[%] Adding missing columns...', TIMEOFDAY()::TIMESTAMP;

  IF t_current_schema IS NULL OR t_gold_schema IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  t_common_tables := ARRAY(
    SELECT
      schema_common_tables 
    FROM
      schema_common_tables(t_current_schema, t_gold_schema));
    
  FOREACH t_table_name IN ARRAY t_common_tables LOOP
    t_missing_columns := ARRAY(
      SELECT schema_missing_columns(t_current_schema, t_gold_schema,
        t_table_name));
    
    FOREACH t_column_name IN ARRAY t_missing_columns LOOP
    
      -- We need the column information (type and default)
      -- for the missing column as specified by the
      -- column in the gold schema.
      
      SELECT
        ctd.t_column_type, ctd.t_column_default 
      INTO
        t_column_type, t_column_default 
      FROM
        schema_column_information(
          t_gold_schema, t_table_name, t_column_name) ctd;
      
      -- Add the missing column with the given type
      t_ddl := FORMAT('ALTER TABLE %s.%s ADD COLUMN %s %s;',
          QUOTE_IDENT(t_current_schema),
          t_table_name, t_column_name, t_column_type);

      RAISE NOTICE '[%] %', TIMEOFDAY()::TIMESTAMP, t_ddl;
      EXECUTE t_ddl;
      
      -- Now add the default and populate it if specified.
      
      IF t_column_default IS NOT NULL THEN
      
        t_ddl := FORMAT('ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s;', 
          QUOTE_IDENT(t_current_schema),
          t_table_name, t_column_name, t_column_default);

        RAISE NOTICE '[%] %', TIMEOFDAY()::TIMESTAMP, t_ddl;
        EXECUTE t_ddl;
        
        -- Supply the default values to the present table
        t_ddl := FORMAT('UPDATE %s.%s SET %s = %s WHERE %s IS NULL;', 
          QUOTE_IDENT(t_current_schema),
          t_table_name, t_column_name, t_column_default, t_column_name);

        RAISE NOTICE '[%] %', TIMEOFDAY()::TIMESTAMP, t_ddl;
        EXECUTE t_ddl;

      END IF;
    END LOOP;
  END LOOP;

  RAISE NOTICE '[%] Completed missing column additions.', TIMEOFDAY()::TIMESTAMP;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  If a column's datatype in the current schema is different
--  from the corresponding datatype in the gold schema,
--  modify the column type for this table in the current schema.
--
--  POTENTIAL FAILURES:
--  * gold default value might not work for new type
--  * values inside the column might not convert
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_alter_changed_datatypes(
  t_current_schema TEXT = NULL,
  t_gold_schema TEXT = NULL)
  RETURNS VOID AS
$$
DECLARE
  t_table_name TEXT;
  t_column_name TEXT;
  t_column_type_gold TEXT;
  t_column_default_gold TEXT;
  t_column_type_curr TEXT;
  t_column_default_curr TEXT;
  t_ddl TEXT;
  t_common_columns TEXT[];
  t_common_tables TEXT[];
  
BEGIN
  RAISE NOTICE '[%] Altering datatypes that have changed...', TIMEOFDAY()::TIMESTAMP;

  IF t_current_schema IS NULL OR t_gold_schema IS NULL THEN
    RAISE EXCEPTION '[%] At least one parameter is NULL!', TIMEOFDAY()::TIMESTAMP;
  END IF;

  t_common_tables := ARRAY(
    SELECT schema_common_tables 
    FROM schema_common_tables(t_current_schema, t_gold_schema));
    
  FOREACH t_table_name IN ARRAY t_common_tables LOOP
    t_common_columns := ARRAY(
      SELECT schema_common_columns(
        t_current_schema, t_gold_schema, t_table_name));

    FOREACH t_column_name IN ARRAY t_common_columns LOOP

      -- Column type in GOLD
      SELECT
        ctd.t_column_type, ctd.t_column_default 
      INTO
        t_column_type_gold, t_column_default_gold
      FROM
        schema_column_information(
          t_gold_schema, t_table_name, t_column_name) ctd;

      -- Column type in CURRENT
      SELECT
        ctd.t_column_type, ctd.t_column_default 
      INTO
        t_column_type_curr, t_column_default_curr
      FROM
        schema_column_information(
          t_current_schema, t_table_name, t_column_name) ctd;
      
      -- Generate and execute the DDL to set the new datatype for
      -- the altered column
      IF t_column_type_gold != t_column_type_curr THEN

        t_ddl := FORMAT('ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s using %s::%s',
          t_table_name, t_column_name, t_column_type_gold,
          t_column_name, t_column_type_gold);

        RAISE NOTICE '[%] %', TIMEOFDAY()::TIMESTAMP, t_ddl;
        EXECUTE t_ddl;
      END IF;
    END LOOP;
  END LOOP;

  RAISE NOTICE '[%] Completed datatype alterations.', TIMEOFDAY()::TIMESTAMP;
END;
$$
LANGUAGE plpgsql;

--  --------------------------------------------------------------------------
--  Invoke all the schema modifications steps in the
--  proper order. Some steps depend on others, for example
--  if we are altering the column defaults, we need to make
--  sure that the datatype has been fixed first.
--  --------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION schema_handler()
  RETURNS VOID AS
$$
DECLARE
  t_current_schema TEXT;
  t_gold_schema TEXT;

BEGIN
  SELECT
    si.t_current_schema, si.t_gold_schema
  INTO
    t_current_schema, t_gold_schema 
  FROM
    schema_information() si;
    
  PERFORM schema_assert_exists(t_gold_schema);
  
  RAISE NOTICE '[%] Modifying current schema % to match the gold schema %',
    TIMEOFDAY()::TIMESTAMP, t_current_schema, t_gold_schema;

  PERFORM schema_create_missing_tables(t_current_schema, t_gold_schema);
  PERFORM schema_add_missing_columns(t_current_schema, t_gold_schema);
  PERFORM schema_alter_changed_datatypes(t_current_schema, t_gold_schema);
  
  RAISE NOTICE '[%] Schema modifications complete.', TIMEOFDAY()::TIMESTAMP;
END;
$$
LANGUAGE plpgsql;

-- And GO!
DO $$
BEGIN
  PERFORM schema_handler();
END $$;

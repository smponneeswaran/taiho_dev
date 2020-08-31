CREATE OR REPLACE FUNCTION gather_stastics(object_name TEXT)
  RETURNS BOOLEAN AS
$$
DECLARE
  t_ddl TEXT;

BEGIN

  t_ddl := FORMAT('ANALYZE %s;', object_name);
  RAISE NOTICE '[%] %', TIMEOFDAY()::TIMESTAMP, t_ddl;
  EXECUTE t_ddl;

  RETURN TRUE;

END;
$$
LANGUAGE plpgsql;


/*
CDM is_integer function
Client: Regeneron
Notes: 
    Cast text to a integer and return boolean otherwise
*/

CREATE OR REPLACE FUNCTION is_integer(pInt text) 
RETURNS BOOLEAN AS
$$
DECLARE
    lInt integer:= null;
BEGIN
	lInt = pInt::integer;
    RETURN true;
EXCEPTION
    WHEN OTHERS THEN
        RETURN false;
END
$$ LANGUAGE plpgsql;

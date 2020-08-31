/*
CDM convert_to_timestamp function
Client: Regeneron
Notes:
    Cast text to a timestamp and return null otherwise
*/

CREATE OR REPLACE FUNCTION convert_to_timestamp(pTimestamp text)
RETURNS timestamp AS
$$
DECLARE
    lTimestamp timestamp := null;
BEGIN
    lTimestamp := pTimestamp::timestamp without time zone;
    RETURN lTimestamp;
EXCEPTION
    WHEN OTHERS THEN
        RETURN null;
END
$$ LANGUAGE plpgsql;

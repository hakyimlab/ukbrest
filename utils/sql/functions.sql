-- Return NULL if integer value is negative
CREATE OR REPLACE FUNCTION nullifneg(integer) RETURNS integer
    AS 'select case when $1 >= 0 then $1 else null end;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

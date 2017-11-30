-- Returns NULL if integer value is negative
CREATE OR REPLACE FUNCTION nullifneg(integer) RETURNS integer
    AS 'select case when $1 >= 0 then $1 else null end;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- Returns children codings recursively, given data field and the disease/value parent node_id
CREATE OR REPLACE FUNCTION get_children_codings(text, integer) RETURNS setof text
    AS '
    with recursive children_coding(coding, node_id, parent_id) as (
      select coding, node_id, parent_id
      from codings
      where data_coding = (select distinct coding from fields where field_id = $1) and parent_id = $2
    union
      select c.coding, c.node_id, c.parent_id
      from children_coding cc, codings c
      where c.data_coding = (select distinct coding from fields where field_id = $1) and c.parent_id = cc.node_id)
    select coding
    from children_coding
    '
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;


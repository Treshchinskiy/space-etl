CREATE MATERIALIZED VIEW IF NOT EXISTS space_db.people_in_space_mv
TO space_db.people
AS
SELECT
    tupleElement(line, 1) AS craft,
    tupleElement(line, 2) AS name,
    _inserted_at
FROM (
    SELECT arrayJoin(
        JSONExtract(json_data, 'people', 'Array(Tuple(craft String, name String))')
    ) AS line, _inserted_at
    FROM space_db.people_in_space_raw
);
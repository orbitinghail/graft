WITH RECURSIVE descendants(id, parent_id, kind, name_or_index, value) AS (
    SELECT f.id, f.parent_id, f.kind, f.name_or_index, f.value
    FROM field f
    WHERE f.id = :id

    UNION ALL

    SELECT f.id, f.parent_id, f.kind, f.name_or_index, f.value
    FROM field f
    JOIN descendants d ON f.parent_id = d.id
    WHERE f.id != f.parent_id
)
SELECT * FROM descendants

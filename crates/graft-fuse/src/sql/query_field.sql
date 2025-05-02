WITH RECURSIVE descendants(id, parent_id, kind, name_or_index, value, path) AS (
    SELECT f.id, f.parent_id, f.kind, f.name_or_index, f.value, CAST(f.id AS TEXT)
    FROM field f
    WHERE f.id = :id

    UNION ALL

    SELECT f.id, f.parent_id, f.kind, f.name_or_index, f.value, d.path || '.' || f.id
    FROM field f
    JOIN descendants d ON f.parent_id = d.id
    WHERE f.id != f.parent_id
)
SELECT id, parent_id, kind, name_or_index, value
FROM descendants
ORDER BY path;

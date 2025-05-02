-- seed objects and arrays
INSERT INTO
    field (id, parent_id, kind, name_or_index)
VALUES
    (1, 1, 2, 'root'),
    (1000, 1, 2, 'sample-1'),
    (1001, 1000, 2, 'sample-2'),
    (1002, 1000, 3, 'endpoints'),
    (1100, 1, 2, 'sample-2'),
    (1101, 1100, 3, 'replica_set'),
    (1102, 1100, 2, 'credentials'),
    (1200, 1, 2, 'sample-3'),
    (1201, 1200, 3, 'experiments');

-- seed values
INSERT INTO
    field (parent_id, name_or_index, value)
VALUES
    -- sample-1 fields
    (1000, 'sample-2', 'production'),
    (1000, 'version', 'v3.2.1'),
    -- logging
    (1001, 'level', 'debug'),
    (1001, 'to_file', 1),
    -- endpoints array (stored backwards to test query field ordering)
    (1002, 1, '/api/v1/orders'),
    (1002, 0, '/api/v1/users'),
    -- sample-2 fields
    (1100, 'engine', 'postgresql'),
    (1100, 'pool_size', 40),
    -- replica_set array
    (1101, 0, 'db-1.internal'),
    (1101, 1, 'db-2.internal'),
    -- credentials object
    (1102, 'user', 'service'),
    (1102, 'password', 'REDACTED'),
    -- sample-3 fields
    (1200, 'beta_dashboard', 1),
    (1200, 'use_edge_cache', 0),
    -- experiments array
    (1201, 0, 'new_nav'),
    (1201, 1, 'async_hooks');

-- seed inodes
INSERT INTO
    inode (id, parent_id, kind, name, field_id)
VALUES
    (1, 1, 1, 'root', NULL),
    (2, 1, 2, 'sample-1.json', 1000),
    (3, 1, 2, 'sample-2.toml', 1100),
    (4, 1, 2, 'sample-3.yaml', 1200);

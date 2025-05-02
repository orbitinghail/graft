-- seed objects and arrays
INSERT INTO
    field (id, parent, kind, name_or_index)
VALUES
    (1, 1, 4, 'root'),
    (1000, 1, 4, 'config_app'),
    (1001, 1000, 4, 'logging'),
    (1002, 1000, 5, 'endpoints'),
    (1100, 1, 4, 'config_db'),
    (1101, 1100, 5, 'replica_set'),
    (1102, 1100, 4, 'credentials'),
    (1200, 1, 4, 'feature_flags'),
    (1201, 1200, 5, 'experiments');

-- seed fields
INSERT INTO
    field (parent, kind, name_or_index, value)
VALUES
    -- config_app fields
    (1000, 1, 'env', 'production'),
    (1000, 1, 'version', 'v3.2.1'),
    -- logging
    (1001, 1, 'level', 'debug'),
    (1001, 3, 'to_file', 1),
    -- endpoints array
    (1002, 1, 0, '/api/v1/users'),
    (1002, 1, 1, '/api/v1/orders'),
    -- config_db fields
    (1100, 1, 'engine', 'postgresql'),
    (1100, 2, 'pool_size', 40),
    -- replica_set array
    (1101, 1, 0, 'db-1.internal'),
    (1101, 1, 1, 'db-2.internal'),
    -- credentials object
    (1102, 1, 'user', 'service'),
    (1102, 1, 'password', 'REDACTED'),
    -- feature_flags fields
    (1200, 3, 'beta_dashboard', 1),
    (1200, 3, 'use_edge_cache', 0),
    -- experiments array
    (1201, 1, 0, 'new_nav'),
    (1201, 1, 1, 'async_hooks');

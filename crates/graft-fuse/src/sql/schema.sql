CREATE TABLE IF NOT EXISTS inode (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    -- the directory that contains this inode
    parent_id INTEGER NOT NULL,
    -- dir(1) or file(2)
    kind INTEGER NOT NULL,
    name TEXT NOT NULL,
    -- the root field id for file inodes
    field_id INTEGER,
    FOREIGN KEY (parent_id) REFERENCES inode (id) ON DELETE CASCADE,
    FOREIGN KEY (field_id) REFERENCES field (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS inode_parent ON inode (parent_id);

CREATE TABLE IF NOT EXISTS field (
    id INTEGER PRIMARY KEY NOT NULL,
    parent_id INTEGER NOT NULL,
    -- string(1), numeric(2), boolean(3), object(4), list(5)
    kind INTEGER NOT NULL,
    -- name or index (for an array) of the field
    name_or_index NOT NULL,
    -- dynamically typed contents of the field
    -- must be null for objects and arrays
    value,
    FOREIGN KEY (parent_id) REFERENCES field (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS field_parent ON field (parent_id);

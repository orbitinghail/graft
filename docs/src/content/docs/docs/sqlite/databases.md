---
title: Databases
description: Understanding how to connect to different databases using Graft
---

When using Graft, each SQLite database maps to a **Graft** - a pairing of a local volume (your working copy) with a remote volume (the source of truth in object storage).

You can connect to databases in several ways:
- By Volume ID (direct)
- By Tag (recommended for named databases)
- Create new databases
- Clone from remote
- Fork from snapshot

## Connect by Volume ID

You can specify a Volume ID directly in the connection string:

```sql
.open 'file:GonugMKom6Q92W5YddpVTd?vfs=graft'
```

This opens or creates a graft with the specified local Volume ID.

## Connect by Tag

For easier management, use tags - human-readable names for grafts:

```sql
-- List all available tags
pragma graft_tags;

-- Switch to a tagged graft
pragma graft_switch = "main";
```

Tags make it easy to work with multiple databases without memorizing Volume IDs.

## Create a New Database

### With a Random Volume ID

Use the literal string `random` to generate a new Volume ID:

```sql
.open 'file:random?vfs=graft'
```

### Using Pragma

Create a fresh graft:

```sql
pragma graft_new;
```

Both methods create a new, empty database with no remote tracking.

## Clone from Remote

To create a local copy of a remote volume:

```sql
pragma graft_clone = "GonugMKom6Q92W5YddpVTd";
```

This is like `git clone` - creates a new local graft that tracks the specified remote volume.

## Fork from Snapshot

To create an independent copy of your current database:

```sql
-- First ensure all pages are downloaded
pragma graft_hydrate;

-- Then fork
pragma graft_fork;
```

This creates a divergent copy that's independent from the original. The volume must be fully hydrated before forking.

## Retrieving Volume IDs

After connecting with `random`, you'll need the generated Volume ID to open additional connections.

### SQLite CLI

```sql
.databases
```

The Volume ID appears in the second column for each database using Graft.

### Programmatically (Python)

```python
import sqlite3
import sqlite_graft

# Load graft extension
db = sqlite3.connect(":memory:")
db.enable_load_extension(True)
sqlite_graft.load(db)

# Open with random Volume ID
conn = sqlite3.connect('file:random?vfs=graft', autocommit=True, uri=True)

# Get the Volume ID
cursor = conn.execute('PRAGMA database_list')
for row in cursor.fetchall():
    db_alias = row[1]    # 'main', 'temp', etc.
    volume_id = row[2]   # The Volume ID
    print(f"{db_alias}: {volume_id}")
```

These retrieved Volume IDs can be used to open the same databases across multiple connections and from multiple devices.

---
title: Databases
description: Understanding how to connect to different databases using Graft
---

The Graft SQLite extension maps each SQLite database to a single [Graft Volume](/docs/concepts/volumes).

You can either connect to a specific database by using a [Graft Volume ID](/docs/concepts/volumes) as the database name or by using the literal string `random` to dynamically create a new volume.

## Connect to a specific database by Volume Id

When connecting to a Graft SQLite database, you can specify a particular Volume ID directly:

```sql
.open 'file:GonugMKom6Q92W5YddpVTd?vfs=graft'
```

## Create a new database with a random Volume Id

Alternatively, you can use `random` to automatically generate a new Volume:

```sql
.open 'file:random?vfs=graft'
```

To open additional connections to a randomly generated Volume, you'll first need the generated Volume ID. You can retrieve it using either of the following methods:

- **Using the SQLite CLI:**

  ```sql
  .databases
  ```

  The Volume ID will appear in the second column for each attached database which uses Graft.

- **Programmatically via SQLite interfaces such as Python:**

  ```python
  import sqlite3
  import sqlite_graft

  # load graft using a temporary (empty) in-memory SQLite database
  db = sqlite3.connect(":memory:")
  db.enable_load_extension(True)
  sqlite_graft.load(db)

  conn = sqlite3.connect('file:random?vfs=graft', autocommit=True, uri=True)
  cursor = conn.execute('PRAGMA database_list')
  db_list = cursor.fetchall()

  for db in db_list:
      db_alias = db[1]    # Database alias (e.g., 'main', 'attached_db')
      volume_id = db[2]   # Filename, i.e., the Volume ID
      print(f"{db_alias}: {volume_id}")
  ```

These retrieved Volume IDs can then be used to open the same Volumes across multiple connections and from multiple nodes.

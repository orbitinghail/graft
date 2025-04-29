---
title: Python
description: Use the Graft SQLite extension with Python
sidebar:
  order: 0
---

```bash
pip install sqlite-graft
```

## Usage

```python
import sqlite3
import sqlite_graft

# load graft using a temporary (empty) in-memory SQLite database
db = sqlite3.connect(":memory:")
db.enable_load_extension(True)
sqlite_graft.load(db)

# open a Graft volume as a database
db = sqlite3.connect(f"file:random?vfs=graft", autocommit=True, uri=True)

# use pragma to verify graft is working
result = db.execute("pragma graft_status")
print(result.fetchall()[0][0])
```

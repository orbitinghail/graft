---
title: Ruby
description: Use the Graft SQLite extension with Ruby
sidebar:
  order: 2
---

```bash
gem install sqlite-graft
```

## Usage

```ruby
require 'sqlite3'
require 'sqlite_graft'

db = SQLite3::Database.new(':memory:')
db.enable_load_extension(true)
SqliteGraft.load(db)

db = SQLite3::Database.new('file:random?vfs=graft');
db.execute("PRAGMA graft_status") do |row|
  p row
end
```

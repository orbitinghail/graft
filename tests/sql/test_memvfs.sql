-- Load the memvfs extension and open a new connection using it
-- The memvfs extension is located at crates/sqlite-plugin/examples/memvfs.rs
.load libmemvfs
.open main.db

.databases
.vfsinfo
.vfsname

CREATE TABLE t1(a, b);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;
pragma hello_vfs=1234;

select * from dbstat;

vacuum;
drop table t1;
vacuum;

select * from dbstat;

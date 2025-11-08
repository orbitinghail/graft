.open "file:main?vfs=graft"
pragma graft_switch="5rMJhorqTw-2dcMpAG9SgLPw:5rMJhorrQK-2dv4hJLznFMj8";

.databases
.vfsinfo
pragma graft_status;

CREATE TABLE t1(a, b);
INSERT INTO t1 VALUES(1, 2);
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;

BEGIN;
SELECT * FROM t1;
INSERT INTO t1 VALUES(3, 4);
SELECT * FROM t1;
COMMIT;

pragma graft_status;

vacuum;
drop table t1;
vacuum;

select * from dbstat;

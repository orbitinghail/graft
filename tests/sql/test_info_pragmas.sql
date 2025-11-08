-- initialize two connections to the same database
.connection 0
.open "file:main?vfs=graft"
pragma graft_switch="5rMJhorqTw-2dcMpAG9SgLPw:5rMJhorrQK-2dv4hJLznFMj8";
pragma graft_status;

.connection 1
.open "file:main?vfs=graft"
pragma graft_status;

-- initialize the db on connection 0
.connection 0
.echo off
.read datasets/bank.sql
.echo on

-- check pragmas
pragma graft_status;
pragma graft_snapshot;
pragma graft_pages;
pragma graft_version;

-- check pragmas on connection 1
.connection 1
pragma graft_status;
pragma graft_snapshot;
pragma graft_pages;
pragma graft_version;

-- open a snapshot on connection 1
begin;
select count(*) from ledger;
pragma graft_snapshot;

-- switch to connection 0, write something, check snapshot, switch back
.connection 0
INSERT INTO ledger (account_id, amount) VALUES (1, -10), (2, 10);
pragma graft_snapshot;
.connection 1

-- check that connection 1 pragmas can't see the new snapshot
pragma graft_snapshot;
pragma graft_pages;

-- close the snapshot and check that we can see the latest snapshot
commit;

pragma graft_snapshot;
pragma graft_pages;

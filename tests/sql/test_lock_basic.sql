-- initialize two connections to the same database
.connection 0
.open "file:main?vfs=graft"
pragma graft_switch="74ggc1B6R4-2kkvcy9fi4CHJ:74ggc1B6jg-2udz14pbDayZC";
pragma graft_status;

.connection 1
.open "file:main?vfs=graft"
pragma graft_status;

-- load the sample dataset
.read datasets/simple.sql

-- scenario: lock a table in one connection, then try to lock it in the other
.connection 0
begin immediate;
.connection 1
-- EXPECT ERROR: database is locked
begin immediate;

-- reset
.connection 0
rollback;
.connection 1
-- EXPECT ERROR: no transaction
rollback;

-- scenario: verify that upgrading is refused if a read snapshot is outdated
.connection 0
begin;
-- take a read lock
select count(*) from t;

.connection 1
-- update the table, this autocommits
insert into t values(1);

.connection 0
-- try to upgrade the lock via performing a write; this should fail
-- EXPECT ERROR: database is locked
insert into t values(2);

-- reset
.connection 0
rollback;
.connection 1
-- EXPECT ERROR: no transaction
rollback;

-- scenario: verify that we can commit a write tx while another tx holds a read lock

-- take a write lock
.connection 0
begin immediate;

-- take a read lock
.connection 1
begin;
select count(*) from t;

-- upgrade our write lock to Pending
.connection 0
insert into t values('committed while read lock is held');

-- try to commit; this should work because the read lock is not blocking
commit;

-- back on the read conn, verify that we still don't see the new row
.connection 1
select * from t;

-- commit the read tx
commit;

-- verify that we now see the new row
select * from t;

-- check metadata
pragma graft_status;
.connection 0
pragma graft_status;

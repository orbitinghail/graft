-- sqlite demo table with 4k rows

BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);
INSERT INTO t VALUES(null);
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
insert into t select null from t;
COMMIT;

# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] structured logging
- [x] RecoverPendingCommit job
- [x] RuntimeHandle::read_page
- [x] Runtime::create_volume_from_remote
- [ ] comprehensive tests
- [ ] libgraft SQLite

# Working on SQLite v2

- [ ] tag handle should probably not cache the graft id - it's easy to get out of sync (see test_sync_and_reset)
- [ ] how to recover from a remote volume disappearing? (or switching remotes)
      -> currently you need to hydrate before it goes away, then fork, then push
- [ ] build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- [ ] consider adding a read oracle (do some perf testing)
- [ ] port tests
- [ ] write first draft of antithesis tests

# Features needed for SyncConf demo

- [x] consistency check pragma: blake3 hash of volume
- [x] autosync delay on commit
- [x] fix graft_hydrate
- [x] why is graft_audit slow?
- [x] can we make checksumming faster?
- [x] graft_info pragma

Demo flow:

- attach to a remote volume with no local state
- instant fast forward
- perform a partial read -> notice only X pages downloaded
- hydrate full volume, perform consistency check

Demo script:

```sql
pragma graft_clone = '5rMJiBh2yA-2duiKapixrzr5';
pragma graft_pull;
pragma graft_info;
pragma graft_status;
pragma graft_audit;

-- moving a remote volume to local
pragma graft_hydrate;
pragma graft_fork;
pragma graft_switch = 'FORKVID';
-- exit shell, restart without s3 flag
pragma graft_push;

-- G7: GDP per capita
SELECT
  c.short_name AS Country,
  s.indicator_name AS "Indicator",
  printf('%,.0f', MAX(CASE WHEN w.year = 1990 THEN w.value END)) AS "1990",
  printf('%,.0f', MAX(CASE WHEN w.year = 2000 THEN w.value END)) AS "2000",
  printf('%,.0f', MAX(CASE WHEN w.year = 2010 THEN w.value END)) AS "2010",
  printf('%,.0f', MAX(CASE WHEN w.year = 2020 THEN w.value END)) AS "2020"
FROM wdi_csv w
JOIN wdi_country c ON w.country_code = c.country_code
JOIN wdi_series  s ON w.indicator_code = s.series_code
WHERE w.indicator_code = 'NY.GDP.PCAP.CD'
  AND w.country_code IN ('USA','CAN','GBR','FRA','DEU','ITA','JPN')
  AND w.year IN (1990, 2000, 2010, 2020)
GROUP BY c.short_name, s.indicator_name
ORDER BY MAX(CASE WHEN w.year = 2020 THEN w.value END) DESC;

-- G7: exports vs imports
SELECT
  c.short_name AS Country,
  s.indicator_name AS "Indicator",
  printf('%,.0f', MAX(CASE WHEN w.year = 1990 THEN w.value END)) AS "1990",
  printf('%,.0f', MAX(CASE WHEN w.year = 2000 THEN w.value END)) AS "2000",
  printf('%,.0f', MAX(CASE WHEN w.year = 2010 THEN w.value END)) AS "2010",
  printf('%,.0f', MAX(CASE WHEN w.year = 2020 THEN w.value END)) AS "2020"
FROM wdi_csv w
JOIN wdi_country c ON w.country_code = c.country_code
JOIN wdi_series  s ON w.indicator_code = s.series_code
WHERE w.indicator_code IN ('NE.EXP.GNFS.ZS', 'NE.IMP.GNFS.ZS')
  AND w.country_code IN ('USA','CAN','GBR','FRA','DEU','ITA','JPN')
  AND w.year IN (1990, 2000, 2010, 2020)
GROUP BY c.short_name, s.indicator_name
ORDER BY 1, 2;

-- G7: population
SELECT
  c.short_name AS Country,
  s.indicator_name AS "Indicator",
  printf('%,.0f', MAX(CASE WHEN w.year = 1990 THEN w.value END)) AS "1990",
  printf('%,.0f', MAX(CASE WHEN w.year = 2000 THEN w.value END)) AS "2000",
  printf('%,.0f', MAX(CASE WHEN w.year = 2010 THEN w.value END)) AS "2010",
  printf('%,.0f', MAX(CASE WHEN w.year = 2020 THEN w.value END)) AS "2020"
FROM wdi_csv w
JOIN wdi_country c ON w.country_code = c.country_code
JOIN wdi_series  s ON w.indicator_code = s.series_code
WHERE w.indicator_code IN ('SP.POP.TOTL')
  AND w.country_code IN ('USA','CAN','GBR','FRA','DEU','ITA','JPN')
  AND w.year IN (1990, 2000, 2010, 2020)
GROUP BY c.short_name, s.indicator_name
ORDER BY 1, 2;
```

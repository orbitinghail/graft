```bash
rm -r .graft/clients

useaws tigris
aws s3 ls 's3://graft-primary/volumes/'

just run sqlite shell --release --s3 graft-primary --client f1
just run sqlite shell --release --client f1
```

```sql
-- volume on s3
pragma graft_clone = '5rMJiBh2yA-2duiKapixrzr5';
-- volume on fs
pragma graft_clone = '5rMJiK5qTN-2e3L1vhNrUdbg';

pragma graft_pull;
pragma graft_push;
pragma graft_info;
pragma graft_status;
pragma graft_audit;

-- get the total balance of all accounts
SELECT SUM(balance) FROM accounts;

-- transfer $10 from account 1 to account 2
INSERT INTO ledger (account_id, amount) VALUES (1, -10), (2, 10);

-- get the balance of account 1 and 2
SELECT * FROM accounts WHERE id IN (1, 2);

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

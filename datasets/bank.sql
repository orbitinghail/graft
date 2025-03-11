CREATE TABLE accounts (
    id INTEGER PRIMARY KEY NOT NULL,
    balance INTEGER NOT NULL,
    CHECK (balance >= 0)
) STRICT;

CREATE TABLE ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    account_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
) STRICT;

CREATE INDEX ledger_account_id ON ledger (account_id);

-- define a trigger that updates the balance of the account when a ledger entry is added
CREATE TRIGGER update_balance AFTER INSERT ON ledger
BEGIN
    UPDATE accounts
    SET balance = balance + NEW.amount
    WHERE id = NEW.account_id;
END;

-- generate 1000 initial empty accounts
WITH RECURSIVE
  cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<1000)
INSERT INTO accounts (balance) SELECT 0 FROM cnt;

-- generate 1000 initial ledger entries of $50 each
WITH RECURSIVE
  cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<1000)
INSERT INTO ledger (account_id, amount) SELECT x, 50 FROM cnt;

/*
DEMO QUERIES:

.read datasets/bank.sql

-- get the total balance of all accounts
SELECT SUM(balance) FROM accounts;

-- validate the materialized balances match the computed balances
SELECT account_id, balance, ledger_balance, 'incorrect balance' as error
FROM
    (select account_id, sum(amount) as ledger_balance from ledger group by 1) as ledger
    INNER JOIN accounts ON accounts.id = ledger.account_id
WHERE balance != ledger_balance;

-- get top 10 accounts with the highest balance
SELECT * FROM accounts ORDER BY balance desc, id asc LIMIT 10;

-- transfer $10 from account 1 to account 2
INSERT INTO ledger (account_id, amount) VALUES (1, -10), (2, 10);

-- get the balance of account 1 and 2
SELECT * FROM accounts WHERE id IN (1, 2);
*/

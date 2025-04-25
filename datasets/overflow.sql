DROP TABLE IF EXISTS big_data;

CREATE TABLE big_data (id INTEGER PRIMARY KEY, data BLOB);

-- Insert a large blob
INSERT INTO
    big_data (data)
VALUES
    (zeroblob (17315));

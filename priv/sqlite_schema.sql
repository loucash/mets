CREATE TABLE IF NOT EXISTS data_points (
    rowkey  BLOB NOT NULL,
    offset  BLOB NOT NULL,
    value   BLOB NOT NULL,
    ttl     INTEGER DEFAULT NULL,
    PRIMARY KEY(rowkey,
                offset)
);

CREATE TABLE IF NOT EXISTS row_key_index (
    ns                  BLOB NOT NULL,
    name                BLOB NOT NULL,
    aggregate_fun       BLOB NOT NULL,
    aggregate_param1    BLOB NOT NULL,
    aggregate_param2    BLOB NOT NULL,
    rowtime             BLOB NOT NULL,
    rowkey              BLOB NOT NULL,
    ttl                 INTEGER DEFAULT NULL,
    PRIMARY KEY(ns,
                name,
                aggregate_fun,
                aggregate_param1,
                aggregate_param2,
                rowtime,
                rowkey)
);

CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    date DATE NOT NULL,
    time TIME NOT NULL
);
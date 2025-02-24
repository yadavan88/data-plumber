CREATE TABLE starlog_offsetable (
  --id SERIAL PRIMARY KEY,
  star_date DOUBLE PRECISION,
  log_type VARCHAR(50),
  crew_id INTEGER,
  entry TEXT,
  planetary_date DATE,
  starfleet_time TIMESTAMP
);

-- for magnum sink
CREATE TABLE starlogentry (
  --id SERIAL PRIMARY KEY,
  stardate DOUBLE PRECISION,
  logtype VARCHAR(50),
  crewid INTEGER,
  entry TEXT,
  planetarydate DATE,
  starfleettime TIMESTAMP
);
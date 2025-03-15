CREATE TABLE starlog_offsetable (
  --id SERIAL PRIMARY KEY,
  star_date DOUBLE PRECISION,
  log_type VARCHAR(50),
  crew_id INTEGER,
  entry TEXT,
  planetary_date DATE,
  starfleet_time TIMESTAMP
);

-- for streaming 
CREATE TABLE starlog_streaming (
  id SERIAL PRIMARY KEY,
  star_date DOUBLE PRECISION,
  log_type VARCHAR(50),
  crew_id INTEGER,
  entry TEXT,
  planetary_date DATE,
  starfleet_time TIMESTAMP
);

-- for magnum sink
CREATE TABLE star_log_entry (
  --id SERIAL PRIMARY KEY,
  star_date DOUBLE PRECISION,
  log_type VARCHAR(50),
  crew_id INTEGER,
  entry TEXT,
  planetary_date DATE,
  starfleet_time TIMESTAMP
);
CREATE EXTENSION hstore;
CREATE TABLE districts(
    id SERIAL PRIMARY KEY NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    dhis2id TEXT NOT NULL DEFAULT '',
    is_treatment BOOLEAN DEFAULT 'f',
    eligible BOOLEAN DEFAULT 'f', --whether district participates in survey
    previous_values JSON NOT NULL DEFAULT '{}'::json,
    cdate TIMESTAMP DEFAULT NOW()
);

CREATE TABLE facilities(
    id SERIAL PRIMARY KEY NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    dhis2id TEXT NOT NULL DEFAULT '',
    uuid TEXT NOT NULL DEFAULT '',
    district INTEGER REFERENCES districts(id),
    level TEXT NOT NULL DEFAULT '',
    eligible BOOLEAN NOT NULL DEFAULT 'f',
    is_treatment BOOLEAN DEFAULT 'f',
    previous_values JSON NOT NULL DEFAULT '{}'::json,
    cdate TIMESTAMP DEFAULT NOW()
);

CREATE VIEW facilities_view AS
SELECT a.name, a.dhis2id, a.uuid, b.name as district,
    a.is_treatment as is_treatment_facility, a.eligible as eligible_facility,
    a.level, a.previous_values as facility_values, b.is_treatment as is_treatment_district,
    b.previous_values as district_values
    FROM facilities a, districts b
    WHERE a.district = b.id AND a.eligible = 't' AND b.eligible = 't';

-- used for scheduling messages
CREATE TABLE schedules(
    id SERIAL PRIMARY KEY NOT NULL,
    params JSON NOT NULL DEFAULT '{}'::json,
    run_time TIMESTAMP NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'ready',
    cdate TIMESTAMPTZ DEFAULT NOW()
);

--\copy districts(name, dhis2id) FROM '~/projects/unicef/interapp/districts.csv' WITH DELIMITER ',' CSV HEADER;
--
--UPDATE districts SET eligible = 'f' WHERE name IN
--    ('Bukedea', 'Manafwa', 'Lira', 'Pader', 'Tororo', 'Gulu', 'Lamwo', 'Kitgum', 'Agago',
--        'Apac', 'Kabarole', 'Bundibugyo', 'Kibaale', 'Nakaseke', 'Mubende', 'Katakwi');
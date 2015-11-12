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

CREATE TABLE district_contacts(
    id SERIAL PRIMARY KEY NOT NULL,
    district_id INTEGER REFERENCES districts NOT NULL,
    phonenumber TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '' UNIQUE,
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

-- Add some views

--alter sequence schedules_id_seq restart with 1;
DROP VIEW IF EXISTS schedule_sms_view;
CREATE VIEW schedule_sms_view AS
    SELECT
        id,
        params->>'text' as text,
        array_length(regexp_split_to_array(params->>'sender', ','), 1) as sms_count,
        to_char(run_time, 'yyyy-mm-dd HH:MI') as run_time,
        status,
        params->>'fuuid' as fuuid
    FROM
        schedules
    WHERE
        length(params->>'sender') > 0;

DROP VIEW IF EXISTS facilities_without_reporters;
CREATE VIEW facilities_without_reporters AS
    SELECT
        district, name, dhis2id, level, is_treatment_facility, is_treatment_district
    FROM
        facilities_view
    WHERE
        uuid IN  (SELECT DISTINCT params->>'fuuid' FROM schedules WHERE length(params->>'sender') = 0);

CREATE OR REPLACE FUNCTION get_monthly_reports(month text)
RETURNS TABLE(
    facility text, flevel text, fdistrict text, treatment_facility boolean,
    treatment_district boolean, curr_anc1 text, curr_anc4 text, curr_delivery text,
    curr_pcv text, curr_rr text, comp_anc1 text, comp_anc4 text, comp_delivery text,
    comp_pcv text, comp_rr text, rank text, pos int, total_score text)
AS
$$
BEGIN
    RETURN QUERY
    SELECT
        name,
        level,
        district,
        is_treatment_facility,
        is_treatment_district,
        round((facility_values->month->>'curr_anc1')::numeric)::text,
        round((facility_values->month->>'curr_anc4')::numeric)::text,
        round((facility_values->month->>'curr_delivery')::numeric)::text,
        round((facility_values->month->>'curr_pcv')::numeric)::text,
        round((facility_values->month->>'curr_rr')::numeric)::text,
        round((facility_values->month->>'comp_anc1')::numeric)::text,
        round((facility_values->month->>'comp_anc4')::numeric)::text,
        round((facility_values->month->>'comp_delivery')::numeric)::text,
        round((facility_values->month->>'comp_pcv')::numeric)::text,
        round((facility_values->month->>'comp_rr')::numeric)::text,
        facility_values->month->>'rank'::text,
        regexp_replace(facility_values->month->>'rank'::text, '[a-z]', '', 'g')::int as position,
        round((facility_values->month->>'total_score')::numeric)::text
    FROM
        facilities_view
    ORDER BY district, level, position;
END;
$$ language plpgsql;

-- Add this to mTrac DB
CREATE OR REPLACE FUNCTION public.get_dhts(xdistrict text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
    DECLARE
    r TEXT;
    p TEXT;
    BEGIN
        r := '';
        FOR p IN SELECT split_part(default_connection, ',', 1) FROM reporters
        WHERE facility_id = (SELECT id FROM facilities WHERE name = xdistrict)
        AND length(default_connection) > 1 AND groups LIKE '%DH%' LOOP
            r := r || p || ',';
        END LOOP;
        RETURN rtrim(r,',');
    END;
$function$

--\copy districts(name, dhis2id) FROM '~/projects/unicef/interapp/districts.csv' WITH DELIMITER ',' CSV HEADER;
--
--UPDATE districts SET eligible = 'f' WHERE name IN
--    ('Bukedea', 'Manafwa', 'Lira', 'Pader', 'Tororo', 'Gulu', 'Lamwo', 'Kitgum', 'Agago',
--        'Apac', 'Kabarole', 'Bundibugyo', 'Kibaale', 'Nakaseke', 'Mubende', 'Katakwi');

-- Add some handy commands
-- show facilities without reporter
-- copy (select * from facilities_without_reporters) to '/tmp/facilities_without_reporters.csv' with delimiter ',' csv header;
-- count SMS sent out
-- select sum(sms_count) from schedule_sms_view;

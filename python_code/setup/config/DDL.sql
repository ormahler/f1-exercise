CREATE STREAM pit_stops_for_agg_stream (
    id BIGINT,
    raceId INT,
    driverId INT,
    stop INT,
    lap INT,
    date VARCHAR,
    time VARCHAR,
    duration VARCHAR,
    duration_milliseconds BIGINT
) WITH (
    KAFKA_TOPIC = 'pit_stops',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
);

CREATE TABLE driver_avg_pit_stop_duration AS
SELECT driverId AS driverId_key,
       as_value(driverId) AS driverId,
       AVG(duration_milliseconds) AS avg_pit_stop_duration_milliseconds
FROM pit_stops_for_agg_stream
WHERE duration_milliseconds >= 0
GROUP BY driverId
EMIT CHANGES;

CREATE STREAM drivers_stream (
    driverId INT,
    driverRef VARCHAR,
    number VARCHAR,
    code VARCHAR,
    forename VARCHAR,
    surname VARCHAR,
    dob VARCHAR,
    nationality VARCHAR,
    url VARCHAR
) WITH (
   KAFKA_TOPIC = 'drivers',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
);

CREATE TABLE drivers_table AS
SELECT  driverId AS driverId_key,
        as_value(driverId) AS driverId,
        latest_by_offset(driverRef) AS driverRef,
        latest_by_offset(number) AS number,
        latest_by_offset(code) AS code,
        latest_by_offset(forename) AS forename,
        latest_by_offset(surname) AS surname,
        latest_by_offset(dob) AS dob,
        latest_by_offset(nationality) as nationality,
       latest_by_offset( url) AS url
FROM drivers_stream
GROUP BY driverId;

CREATE TABLE enriched_drivers_table AS
SELECT drivers_table.driverId_key KEY,
       as_value(drivers_table.driverId) AS driverId,
       avg_pit_stop_duration_milliseconds,
       driverRef,
       number,
       code,
       forename,
       surname,
       dob,
       nationality,
       url
FROM driver_avg_pit_stop_duration
LEFT JOIN drivers_table ON driver_avg_pit_stop_duration.driverId_key = drivers_table.driverId_key
EMIT CHANGES;
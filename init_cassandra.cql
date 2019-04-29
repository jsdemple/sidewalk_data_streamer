CREATE KEYSPACE raw_instrument WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};


CREATE TABLE raw_instrument.dmi(
 coordinate_id BIGINT,
 record_no INT,
 left_total INT,
 right_total INT,
 left INT,
 right INT,
PRIMARY KEY (coordinate_id)
);


CREATE TABLE raw_instrument.imu(
 coordinate_id BIGINT,
 unix_epoch FLOAT,
 accelerometer0 FLOAT,
 accelerometer1 FLOAT,
 accelerometer2 FLOAT,
 magnetometer0 FLOAT,
 magnetometer1 FLOAT,
 magnetometer2 FLOAT,
 gyroscope0 FLOAT,
 gyroscope1 FLOAT,
 gyroscope2 FLOAT,
 euler0 FLOAT,
 euler1 FLOAT,
 euler2 FLOAT,
 linear_acceleration0 FLOAT,
 linear_acceleration1 FLOAT,
 linear_acceleration2 FLOAT,
 gravity0 FLOAT,
 gravity1 FLOAT,
 gravity2 FLOAT,
 quaternion0 FLOAT,
 quaternion1 FLOAT,
 quaternion2 FLOAT,
 quaternion3 FLOAT,
PRIMARY KEY (coordinate_id)
);


CREATE TABLE raw_instrument.gps(
 coordinate_id BIGINT,
 gps_timestamp VARCHAR,
 latitude FLOAT,
 lat_dir VARCHAR,
 longitude FLOAT,
 lon_dir VARCHAR,
 num_sats INT,
 horizontal_dil FLOAT,
 altitude FLOAT,
 altitude_units VARCHAR,
 geo_sep_units VARCHAR,
 age_gps_data VARCHAR,
 unused VARCHAR,
PRIMARY KEY (coordinate_id)
);



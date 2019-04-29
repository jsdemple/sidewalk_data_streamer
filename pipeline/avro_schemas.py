#/usr/bin/python

dmi = {
 "namespace": "dmi.avro",
 "type": "record",
 "name": "sidewalk_rig",
 "fields": [
     {"name": "coordinate_id",  "type": "int"},
     {"name": "record_no", "type": "int"},
     {"name": "left_total",  "type": "int"},
     {"name": "right_total", "type": "int"},
     {"name": "left",  "type": "int"},
     {"name": "right", "type": "int"},    
     ]
}

imu = {
 "namespace": "imu.avro",
 "type": "record",
 "name": "sidewalk_rig",
 "fields": [
     {"name": "coordinate_id",  "type": "int"},   
     {"name": "unix_epoch",  "type": "float"},
     {"name": "accelerometer0", "type": "float"},
     {"name": "accelerometer1", "type": "float"},
     {"name": "accelerometer2", "type": "float"},
     {"name": "magnetometer0", "type": "float"},
     {"name": "magnetometer1", "type": "float"},
     {"name": "magnetometer2", "type": "float"}, 
     {"name": "gyroscope0", "type": "float"},
     {"name": "gyroscope1", "type": "float"},
     {"name": "gyroscope2", "type": "float"},
     {"name": "euler0", "type": "float"},
     {"name": "euler1", "type": "float"},
     {"name": "euler2", "type": "float"},  
     {"name": "linear_acceleration0", "type": "float"},
     {"name": "linear_acceleration1", "type": "float"},
     {"name": "linear_acceleration2", "type": "float"}, 
     {"name": "gravity0", "type": "float"},
     {"name": "gravity1", "type": "float"},
     {"name": "gravity2", "type": "float"},
     {"name": "quaternion0", "type": "float"},
     {"name": "quaternion1", "type": "float"},
     {"name": "quaternion2", "type": "float"},
     {"name": "quaternion3", "type": "float"}, 
     ]
}

gps = {
 "namespace": "gps.avro",
 "type": "record",
 "name": "sidewalk_rig",
 "fields": [
     {"name": "coordinate_id",  "type": "int"},
     {"name": "gps_timestamp", "type": "string"},
     {"name": "latitude", "type": ["float", "null"]},
     {"name": "lat_dir",  "type": "string"},
     {"name": "longitude", "type": ["float", "null"]},
     {"name": "lon_dir",  "type": "string"},
     {"name": "num_sats", "type": ["int", "null"]}, 
     {"name": "horizontal_dil",  "type": ["float", "null"]},
     {"name": "altitude", "type": ["float", "null"]},
     {"name": "altitude_units",  "type": "string"},
     {"name": "geo_sep_units", "type": "string"},
     {"name": "age_gps_data", "type": "string"},
     {"name": "unused", "type": "string"},
     ]
}


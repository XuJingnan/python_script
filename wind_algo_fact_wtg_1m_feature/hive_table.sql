create EXTERNAL table if not exists wind_algo_fact_wtg_1m_feature(
wtg_id string,
data_time bigint,
temp_out_door double,
wind_direction double,
wind_vane double,
pitch_angle double,
read_wind_speed double,
rotor_speed double,
gen_speed double,
active_power double,
wtg_type_id string,
wtg_type_name string,
rated_power double,
ideal_active_power double
) partitioned by (hp_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
location '/user/hive/warehouse/bi_dev.db/wind_algo_fact_wtg_1m_feature';
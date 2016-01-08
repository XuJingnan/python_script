create external table if not exists wind_algo_fact_wtg_1m_clean(
wtg_id string,
site_id string,
data_time bigint,
`date` string,
production_active_accum bigint,
production_reactive_accum bigint,
consumption_active_accum bigint,
consumption_reactive_accum bigint,
temp_out_door double,
wind_direction double,
nacelle_position double,
wind_vane double,
pitch_angle double,
wind_speed double,
read_wind_speed double,
rotor_speed double,
gen_speed double,
torque_setpoint double,
torque double,
active_power_calc double,
active_power double,
power_curve_state double
) partitioned by (hp_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'hdfs://titan/prod/bi/logs/wind/wind_algo_fact_wtg_1m_clean';
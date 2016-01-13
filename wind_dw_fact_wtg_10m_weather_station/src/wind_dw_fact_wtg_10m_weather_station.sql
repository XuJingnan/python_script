use ${env.HIVE_DB};

create table if not exists wind_dw_fact_wtg_10m_weather_station(
site_id string,
wtg_id string,
date_time string,
p_date string,
read_wind_speed_avg double,
read_wind_speed_max double,
read_wind_speed_min double,
read_wind_speed_std double,
wind_direction_avg double, 
wind_direction_max double,
wind_direction_min double,
wind_direction_std double,
active_power_avg double, 
active_power_max double,
active_power_min double,
active_power_std double
)partitioned by (hp_date string, hp_ds string);

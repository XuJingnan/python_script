#** 
# Function Name: wtg_order.sql
# Function Desc: extract read_wind_speed, wind_direction, active_power info from table wind_dw_fact_wtg_10m
#                to serve as a service for forecast team 
# Inputs: ${env.YYYY}:
# Inputs: ${env.MM}:
# Inputs: ${env.DD}:
# Inputs: ${env.DS}:
# Author: xujingnan
# Date: 2015-12-21
*#

use ${env.HIVE_DB};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set mapred.job.name=wind_dw_fact_wtg_10m_weather_station_${env.YYYY}${env.MM}${env.DD};

alter table wind_dw_fact_wtg_10m_weather_station drop if exists partition(hp_date = '${env.YYYY}-${env.MM}-${env.DD}');

insert overwrite table wind_dw_fact_wtg_10m_weather_station 
partition (hp_date, hp_ds)
select 
    hp_site,
    wtg_id,
    data_time,
    '${env.YYYY}-${env.MM}-${env.DD}', 
    read_wind_speed_avg,
    read_wind_speed_max,
    read_wind_speed_min,
    read_wind_speed_std,
    wind_direction_avg, 
    wind_direction_max,
    wind_direction_min,
    wind_direction_std,
    active_power_avg, 
    active_power_max,
    active_power_min,
    active_power_std,
    '${env.YYYY}-${env.MM}-${env.DD}',
    hp_ds 
from wind_dw_fact_wtg_10m
where 
hp_yyyymm = '${env.YYYY}${env.MM}' and 
to_date(data_time) = '${env.YYYY}-${env.MM}-${env.DD}';

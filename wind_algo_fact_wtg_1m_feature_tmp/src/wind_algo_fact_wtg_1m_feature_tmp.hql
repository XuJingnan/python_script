use ${env.HIVE_DB};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set mapred.job.name=wind_algo_fact_wtg_1m_feature_tmp_${env.YYYY}${env.MM}${env.DD};

alter table wind_algo_fact_wtg_1m_feature_tmp drop if exists partition(hp_date = '${env.YYYY}${env.MM}${env.DD}');

insert overwrite table wind_algo_fact_wtg_1m_feature_tmp
partition (hp_date='${env.YYYY}${env.MM}${env.DD}')
select
        wtg_clean.wtg_id,
        wtg_clean.data_time,
        wtg_clean.temp_out_door,
        wtg_clean.wind_direction,
        wtg_clean.wind_vane,
        wtg_clean.pitch_angle,
        wtg_clean.read_wind_speed,
        wtg_clean.rotor_speed,
        wtg_clean.gen_speed,
        wtg_clean.active_power,
        wtg_dim_full.wtg_type_id,
        wtg_dim_type.wtg_type_name,
	    wtg_dim_type.rated_power
from
(
    select
        wtg_id,
        data_time,
        temp_out_door,
        wind_direction,
        wind_vane,
        pitch_angle,
        read_wind_speed,
        rotor_speed,
        gen_speed,
        active_power
    from wind_algo_fact_wtg_1m_clean
    where hp_date='${env.YYYY}${env.MM}${env.DD}'
) wtg_clean
join
(
    select
        wtg_id,
        wtg_type_id
    from wind_dw_dim_wtg_full
    where site_type = 2 or site_type = 1
) wtg_dim_full
on wtg_clean.wtg_id = wtg_dim_full.wtg_id
join
(
    select
        wtg_type_id,
        wtg_type_name,
        rated_power
    from wind_dw_dim_wtg_type
) wtg_dim_type
on wtg_dim_full.wtg_type_id = wtg_dim_type.wtg_type_id;
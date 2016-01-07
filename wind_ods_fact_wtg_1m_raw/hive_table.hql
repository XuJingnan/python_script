create external table if not exists wind_ods_fact_wtg_1m_raw(
all_key_value string
) partitioned by (hp_date string)
location 'hdfs://titan/prod/bi/logs/wind/wind_ods_fact_wtg_1m_raw';
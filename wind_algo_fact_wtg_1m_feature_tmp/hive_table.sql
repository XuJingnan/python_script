CREATE TABLE IF NOT EXISTS wind_algo_fact_wtg_1m_feature_tmp (
  wtg_id          string,
  data_time       BIGINT,
  temp_out_door   DOUBLE,
  wind_direction  DOUBLE,
  wind_vane       DOUBLE,
  pitch_angle     DOUBLE,
  read_wind_speed DOUBLE,
  rotor_speed     DOUBLE,
  gen_speed       DOUBLE,
  active_power    DOUBLE,
  wtg_type_id     string,
  wtg_type_name   string,
  rated_power     DOUBLE
) partitioned BY (hp_date string
)
  location '/user/hive/warehouse/bi_dev.db/wind_algo_fact_wtg_1m_feature_tmp';
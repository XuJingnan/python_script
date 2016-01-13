# cal_dt format: YYYYMMDD 
# ds: 1,2,4
v_days=$(( ($(date -d $1 +%s) - $(date -d "0000-01-01" +%s))/(24*60*60)%100 ))
wormhole ${OCEAN_HOME}/wind/wind_dw_fact_wtg_10m_weather_station.hdfs2mysql.xml -E cal_dt=$1 -E part_num=${v_days} -E HIVE_DB=${HIVE_DB}

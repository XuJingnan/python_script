import cx_Oracle

from utils import *


def read_db(sql, save_path):
    con = cx_Oracle.connect('im', 'im', '172.16.33.159:1521/prjdb')
    df = pd.read_sql(sql, con)
    df.to_csv(save_path, index=False)
    con.close()


def make_dir(day):
    mkdir_cmd = 'mkdir -p {input}/{day}'.format(input=INPUT_DIR, day=day)
    os.system(mkdir_cmd)


def read_dim_site_full(day):
    sql = 'select site_id, air_density from {table}'.format(table=DIM_SITE_FULL)
    out_path = os.sep.join([INPUT_DIR, day, DIM_SITE_FULL])
    read_db(sql, out_path)


def read_dim_wtg_full(day):
    sql = 'select wtg_id, site_id, altitude, hub_height, scada_ntf_id, rated_power, rated_torque, on_grid_rotor_speed, on_grid_generator_speed from {table}' \
        .format(table=DIM_WTG_FULL)
    out_path = os.sep.join([INPUT_DIR, day, DIM_WTG_FULL])
    read_db(sql, out_path)


def read_stg_fact_no_conn(day, today):
    sql = 'select * from {table} ' \
          'where nc_starttime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and nc_starttime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(
        table=STG_FACT_NO_CONN, yesterday=day, today=today)
    out_path = os.sep.join([INPUT_DIR, day, STG_FACT_NO_CONN])
    read_db(sql, out_path)


def read_stg_fact_standard_state(day, today):
    sql = 'select * from {table} ' \
          'where ss_starttime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and ss_starttime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(
        table=STG_FACT_STANDARD_STATE, yesterday=day, today=today)
    out_path = os.sep.join([INPUT_DIR, day, STG_FACT_STANDARD_STATE])
    read_db(sql, out_path)


def read_stg_fact_health_state(day, today):
    sql = 'select * from {table} ' \
          'where sc_starttime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and sc_starttime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(
        table=STG_FACT_HEALTH_STATE, yesterday=day, today=today)
    out_path = os.sep.join([INPUT_DIR, day, STG_FACT_HEALTH_STATE])
    read_db(sql, out_path)


def read_dim_ntf_data(day):
    sql = 'select ntf_id, rotor_speed, a, b, c from {table}'.format(table=DIM_NTF_DATA)
    out_path = os.sep.join([INPUT_DIR, day, DIM_NTF_DATA])
    read_db(sql, out_path)


def read_dim_wtg_pc(day):
    sql = 'select wtg_id, pc_type, pc_id from {table}'.format(table=DIM_WTG_PC)
    out_path = os.sep.join([INPUT_DIR, day, DIM_WTG_PC])
    read_db(sql, out_path)


def read_dim_pc_data(day):
    sql = 'select pc_id, wind_speed_rank, power from {table}'.format(table=DIM_PC_DATA)
    out_path = os.sep.join([INPUT_DIR, day, DIM_PC_DATA])
    read_db(sql, out_path)


def read_stg_fact_wtg_10m(day, today):
    sql = 'select WTG_ID,DATATIME,TEMOUTAVE,WINDDIRECTIONAVE,NACELLEPOSITIONAVE,BLADEPITCHAVE,' \
          'WINDSPEEDAVE,WINDSPEEDSTD,READWINDSPEEDAVE,ROTORSPDAVE,GENSPDAVE,TORQUESETPOINTAVE,TORQUEAVE,ACTIVEPWAVE,' \
          'PCURVESTSAVE,APPRODUCTION,RPPRODUCTION,APCONSUMED,RPCONSUMED ' \
          'from {table} where datatime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and datatime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(table=STG_FACT_WTG_10M, yesterday=day,
                                                                       today=today)
    out_path = os.sep.join([INPUT_DIR, day, STG_FACT_WTG_10M])
    read_db(sql, out_path)


def read_dim_alias_envid_mapping(day):
    sql = 'select idx, mdm_id from {table}'.format(table=DIM_ALIAS_ENVID_MAPPING)
    out_path = os.sep.join([INPUT_DIR, day, DIM_ALIAS_ENVID_MAPPING])
    read_db(sql, out_path)


def read_all_data():
    # day = get_date()
    # today = get_today()
    day = execute_day_str
    today = today_str
    make_dir(day)

    read_dim_alias_envid_mapping(day)
    read_stg_fact_wtg_10m(day, today)
    read_stg_fact_no_conn(day, today)
    read_stg_fact_standard_state(day, today)
    read_stg_fact_health_state(day, today)
    read_dim_wtg_full(day)
    read_dim_site_full(day)
    read_dim_ntf_data(day)
    read_dim_wtg_pc(day)
    read_dim_pc_data(day)


if __name__ == '__main__':
    read_all_data()

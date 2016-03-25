import cx_Oracle

from utils import *


def read_from_db(sql, save_path):
    con = cx_Oracle.connect('envision', 'envision', '172.16.33.159:1521/prjdb')
    df = pd.read_sql(sql, con)
    df.to_csv(save_path, index=False)
    con.close()


def make_dir(day):
    mkdir_cmd = 'mkdir -p {input}/{day}'.format(input=INPUT_DIR, day=day)
    os.system(mkdir_cmd)


def read_im_dim_wind_site(day):
    sql = 'select site_id, air_density from {table}'.format(table=IM_DIM_WIND_SITE)
    out_path = os.sep.join([INPUT_DIR, day, IM_DIM_WIND_SITE])
    read_from_db(sql, out_path)


def read_im_dim_wind_wtg(day):
    sql = 'select wtg_id, site_id, altitude, hub_height, scada_ntf_id, contract_pc_id, actual_pc_id, rated_power, rated_torque, on_grid_rotor_speed, on_grid_generator_speed from {table}' \
        .format(table=IM_DIM_WIND_WTG)
    out_path = os.sep.join([INPUT_DIR, day, IM_DIM_WIND_WTG])
    read_from_db(sql, out_path)


def read_im_no_conn(day, today):
    sql = 'select wtg_id, nc_starttime, nc_id from {table} ' \
          'where nc_starttime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and nc_starttime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(
        table=TABLE_IM_NO_CONN, yesterday=day, today=today)
    out_path = os.sep.join([INPUT_DIR, day, TABLE_IM_NO_CONN])
    read_from_db(sql, out_path)


def read_im_ss(day, today):
    sql = 'select wtg_id, ss_starttime, ss_id from {table} ' \
          'where ss_starttime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and ss_starttime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(
        table=TABLE_IM_SS, yesterday=day, today=today)
    out_path = os.sep.join([INPUT_DIR, day, TABLE_IM_SS])
    read_from_db(sql, out_path)


def read_mdm_ntf_data(day):
    sql = 'select parentid, rotorspeed, a, b, c from {table}'.format(table=TABLE_MDM_NTF_DATA)
    out_path = os.sep.join([INPUT_DIR, day, TABLE_MDM_NTF_DATA])
    read_from_db(sql, out_path)


def read_mdm_powercurve_data(day):
    sql = 'select parentid, windspeedrank, power from {table}'.format(table=TABLE_MDM_POWERCURVE_DATA)
    out_path = os.sep.join([INPUT_DIR, day, TABLE_MDM_POWERCURVE_DATA])
    read_from_db(sql, out_path)


def read_tbl_pointvalue_10m(day, today):
    sql = 'select WTG_ID,DATATIME,TEMOUTAVE,WINDDIRECTIONAVE,NACELLEPOSITIONAVE,BLADEPITCHAVE,' \
          'WINDSPEEDAVE,WINDSPEEDSTD,ROTORSPDAVE,GENSPDAVE,TORQUESETPOINTAVE,TORQUEAVE,ACTIVEPWAVE,' \
          'PCURVESTSAVE,APPRODUCTION,RPPRODUCTION,APCONSUMED,RPCONSUMED ' \
          'from {table} where datatime >= to_date(\'{yesterday}\', \'yyyy-mm-dd\') ' \
          'and datatime < to_date(\'{today}\', \'yyyy-mm-dd\')'.format(table=TABLE_TBL_POINTVALUE_10M, yesterday=day,
                                                                       today=today)
    out_path = os.sep.join([INPUT_DIR, day, TABLE_TBL_POINTVALUE_10M])
    read_from_db(sql, out_path)


def read_mdm_alias_envid_mapping(day):
    sql = 'select idx, mdm_id from {table}'.format(table=TABLE_MDM_ALIAS_ENVID_MAPPING)
    out_path = os.sep.join([INPUT_DIR, day, TABLE_MDM_ALIAS_ENVID_MAPPING])
    read_from_db(sql, out_path)


def read_all_data():
    # day = get_date()
    # today = get_today()
    day = '2016-03-10'
    today = '2016-03-11'
    make_dir(day)

    read_im_dim_wind_site(day)
    read_mdm_alias_envid_mapping(day)
    read_im_dim_wind_wtg(day)
    read_im_no_conn(day, today)
    read_im_ss(day, today)
    read_mdm_ntf_data(day)
    read_mdm_powercurve_data(day)
    read_tbl_pointvalue_10m(day, today)


if __name__ == '__main__':
    read_all_data()

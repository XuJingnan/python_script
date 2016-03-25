from sqlalchemy import create_engine

from utils import *


# todo replace
def write_to_db(df, table, if_exists='append', index=False):
    engine = create_engine('oracle+cx_oracle://im:im@172.16.33.159:1521/prjdb')
    df.to_sql(name=table, con=engine, if_exists=if_exists, index=index, chunksize=1000)


def write_im_10m_clean():
    data_dir = os.sep.join([OUTPUT_DIR, execute_day_str, TABLE_IM_10M_CLEAN])
    data = []
    for f in os.listdir(data_dir):
        if f == IM_TURBINES:
            continue
        data.append(pd.read_csv(os.sep.join([data_dir, f]), parse_dates=[STG_FACT_WTG_10M_DATATIME]))
    df = pd.concat(data)
    table = DW_TMP_FACT_WTG_10M_CLEAN
    write_to_db(df, table)


def write_all_data():
    write_im_10m_clean()


if __name__ == '__main__':
    write_all_data()

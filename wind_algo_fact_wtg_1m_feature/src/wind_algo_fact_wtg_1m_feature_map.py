# !/usr/bin/env python
import os
import sys

TABLE1_NAME = "wind_dw_dim_wtg_ideal_power_curve"
# wtg_id,wind_speed_rank,active_power,created_time
TABLE2_NAME = "wind_algo_fact_wtg_1m_feature_tmp"
# wtg_id,data_time,temp_out_door,wind_direction,wind_vane,pitch_angle,read_wind_speed,rotor_speed,gen_speed,active_power,wtg_type_id,wtg_type_name,rated_power

INPUT_SEPARATOR = "\001"
OUTPUT_SEPARATOR = "\t"


def mapper():
    file_path = os.environ["map_input_file"]
    if TABLE1_NAME in file_path:
        for line in sys.stdin:
            if line.strip() == "":
                continue
            values = line[:-1].split(INPUT_SEPARATOR)
            print OUTPUT_SEPARATOR.join([values[0], "1", OUTPUT_SEPARATOR.join(values[1:])])
    elif TABLE2_NAME in file_path:
        for line in sys.stdin:
            if line.strip() == "":
                continue
            values = line[:-1].split(INPUT_SEPARATOR)
            print OUTPUT_SEPARATOR.join([values[0], "2", OUTPUT_SEPARATOR.join(values[1:])])


if __name__ == "__main__":
    mapper()

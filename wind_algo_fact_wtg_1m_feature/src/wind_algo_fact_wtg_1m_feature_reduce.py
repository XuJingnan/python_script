import sys

SEPARATOR = "\t"


def calc_ideal_active_power(low_speed, low_power, high_speed, high_power, speed):
    return ((high_power - low_power) / (high_speed - low_speed)) * (speed - low_speed) + low_power


def process(values, d):
    del values[1]
    tmp = values[6]
    if tmp == '\N':
        ideal_active_power = -1.0
    else:
        speed = float(tmp)
        if speed % 0.5 == 0:
            if speed in d:
                ideal_active_power = d[speed]
            else:
                ideal_active_power = -1.0
        else:
            if speed - int(speed) > 0.5:
                high = float(int(speed)) + 1
                low = high - 0.5
            else:
                low = float(int(speed))
                high = low + 0.5
            if low in d and high in d:
                ideal_active_power = calc_ideal_active_power(low, d[low], high, d[high], speed)
            else:
                ideal_active_power = -1.0
    values.append(str(ideal_active_power))
    print SEPARATOR.join(values)


# table 1 input: #  wtg_id,1,wind_speed_rank,active_power,created_time
# table 2 input: #  wtg_id,2,data_time,temp_out_door,wind_direction,
#                   wind_vane,pitch_angle,read_wind_speed,rotor_speed,gen_speed,
#                   active_power,wtg_type_id,wtg_type_name,rated_power
def reducer():
    last_wtg_id = ""
    d = {}
    for line in sys.stdin:
        if line.strip() == "":
            continue
        values = line[:-1].split(SEPARATOR)
        cur_wtg_id = values[0]
        if cur_wtg_id != last_wtg_id:
            if values[1] == '1':
                last_wtg_id = cur_wtg_id
                d.clear()
                wind_speed_rank = float(values[2])
                ideal_active_power = float(values[3])
                d[wind_speed_rank] = ideal_active_power
            else:
                # only has table 2 data, no table 1 data
                pass
        else:
            if values[1] == '1':
                wind_speed_rank = float(values[2])
                ideal_active_power = float(values[3])
                d[wind_speed_rank] = ideal_active_power
            else:
                process(values, d)


if __name__ == "__main__":
    reducer()

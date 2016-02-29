import sys

desc_alias_mapping = {}
wtg_master_mapping = {}
standard_table = {"COMMON.Reserved": 0,
                  "COMMON.ProtocolID": 1,
                  "COMMON.WTGID": 2,
                  "COMMON.DateTime": 3,
                  "COMMON.Version": 4
                  }


def init():
    with open("desc-alias-mapping.csv") as f:
        f.readline()
        while True:
            line = f.readline()
            if line == "":
                break
            values = line.strip().split(",")
            protocolId = values[0]
            fieldDesc = values[-2]
            fieldAlias = values[-1]
            desc_alias_mapping.setdefault(protocolId, [])
            schema = desc_alias_mapping.get(protocolId)
            schema.append((fieldAlias, fieldDesc))
    with open("standard-table.csv") as f:
        f.readline()
        index = len(standard_table)
        while True:
            line = f.readline()
            if line == "":
                break
            standard_table[line.strip().split(",")[-4]] = index
            index += 1
    with open("wtg-master-mapping.csv") as f:
        f.readline()
        while True:
            line = f.readline()
            if line == "":
                break
            tmp = line.strip().split(",")
            wtg_master_mapping[tmp[2]] = tmp[-1]


if __name__ == "__main__":
    init()
    for line in sys.stdin:
        line = line.strip().split('\t')
        if len(line) == 1:
            continue
        values = line[1].split(",")
        try:
            i = int(values[0])
        except:
            continue
        outData = ["" for i in range(len(standard_table))]
        # copy ProtocolID, WTGID, DateTime, Version
        outData[1:5] = values[0:4]
        sch = desc_alias_mapping.get(values[0])
        values = values[4:]
        if len(sch) != len(values):
            exit(1)
        # inData format [(alias0, desc0, value0), (alias1, desc1, value1), ...]
        inData = [(sch[i][0], sch[i][1], values[i]) for i in range(len(values))]
        tmp = ""
        for i in range(len(inData)):
            # inData alias in standard table, put to the position in standard table
            if inData[i][0] in standard_table:
                outData[standard_table.get(inData[i][0])] = inData[i][2]
                continue
            # otherwise, put to Common.Reserved
            tmp += inData[i][1] + "=" + inData[i][2] + ";"
        outData[0] = tmp[:-1]
        # map wtg id to wtg master id, if no mapping, preserve original wtg id
        outData[2] = wtg_master_mapping.get(outData[2], outData[2])
        print ",".join(outData)

import sys

schemas = {}
standard_schema = {"COMMON.Reserved": 0, "COMMON.ProtocolID": 1, "COMMON.Version": 4, "COMMON.WTGID": 2,
                   "COMMON.DateTime": 3}


def init():
    with open("mapping.csv") as f:
        while True:
            line = f.readline()
            if line == "":
                break
            values = line.strip().split(",")
            protocolId = values[0]
            fieldDesc = values[-2]
            fieldAlias = values[-1]
            schemas.setdefault(protocolId, [])
            schema = schemas.get(protocolId)
            schema.append((fieldAlias, fieldDesc))
    with open("1m_standard_table.csv") as f:
        index = len(standard_schema)
        while True:
            line = f.readline()
            if line == "":
                break
            standard_schema[line.strip().split(",")[-1]] = index
            index += 1


if __name__ == "__main__":
    init()
    for line in sys.stdin:
        if line == "":
            break
        values = line.strip().split(",")
        outData = ["" for i in range(len(standard_schema))]
        outData[1:5] = values[0:4]
        sch = schemas.get(values[0])
        values = values[4:]
        if len(sch) != len(values):
            exit(1)
        inData = [(sch[i][0], sch[i][1], values[i]) for i in range(len(values))]
        tmp = ""
        for i in range(len(inData)):
            if inData[i][0] != "":
                outData[standard_schema.get(inData[i][0])] = inData[i][2]
            else:
                tmp += inData[i][1] + "=" + inData[i][2] + ";"
        outData[0] = tmp[:-1]
        print ",".join(outData)

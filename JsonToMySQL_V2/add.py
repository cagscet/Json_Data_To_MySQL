def insert_single(data, conn, cursor):
    for cihaz in data:
        info = cihaz["INFORMATION"]
        comm = info["communication_interfaces"][0]
        cell = comm["cell_identity"]

        query = """
            INSERT INTO information (
                imei, version, LastIp, mac, prov_version, conf_version,
                cpu_serial, hw_version, gsm_modul_name, gsm_connect_type,
                imsi, sim_id, gsm_no, operator_name,
                cell_MNC, cell_MCC, LAC, serialno
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                version=VALUES(version),
                LastIp=VALUES(LastIp),
                mac=VALUES(mac),
                prov_version=VALUES(prov_version),
                conf_version=VALUES(conf_version),
                cpu_serial=VALUES(cpu_serial),
                hw_version=VALUES(hw_version),
                gsm_modul_name=VALUES(gsm_modul_name),
                gsm_connect_type=VALUES(gsm_connect_type),
                imsi=VALUES(imsi),
                sim_id=VALUES(sim_id),
                gsm_no=VALUES(gsm_no),
                operator_name=VALUES(operator_name),
                cell_MNC=VALUES(cell_MNC),
                cell_MCC=VALUES(cell_MCC),
                LAC=VALUES(LAC),
                serialno=VALUES(serialno)
        """

        values = (
            int(info["imei"]),
            info["version"],
            info["LastIp"],
            info["mac"],
            info["prov_version"],
            info["conf_version"],
            info["cpu_serial"],
            info["hw_version"],
            comm["gsm_modul_name"],
            comm["gsm_connect_type"],
            int(comm["imsi"]),
            comm["sim_id"],
            comm["gsm_no"],
            comm["operator_name"],
            int(cell["MNC"]),
            int(cell["MCC"]),
            int(cell["LAC"]),
            int(info["serialno"])
        )

        cursor.execute(query, values)
        print(f"{info['imei']} eklendi veya güncellendi.")

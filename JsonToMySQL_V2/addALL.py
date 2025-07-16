import json
import mysql.connector
from datetime import datetime


def insert_all(data, conn, cursor):
    today = datetime.today().date()


    for cihaz in data:
        info = cihaz["INFORMATION"]
        comm = info["communication_interfaces"][0]
        cell = comm["cell_identity"]

        # SQL sorgularını buraya yaz
        query = """
            INSERT INTO information (imei, version, LastIp, mac, prov_version, conf_version,
                                     cpu_serial, hw_version, gsm_modul_name, gsm_connect_type,
                                     imsi, sim_id, gsm_no, operator_name,
                                     cell_MNC, cell_MCC, LAC, serialno)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                version = VALUES(version),
                LastIp = VALUES(LastIp),
                mac = VALUES(mac),
                prov_version = VALUES(prov_version),
                conf_version = VALUES(conf_version),
                cpu_serial = VALUES(cpu_serial),
                hw_version = VALUES(hw_version),
                gsm_modul_name = VALUES(gsm_modul_name),
                gsm_connect_type = VALUES(gsm_connect_type),
                imsi = VALUES(imsi),
                sim_id = VALUES(sim_id),
                gsm_no = VALUES(gsm_no),
                operator_name = VALUES(operator_name),
                cell_MNC = VALUES(cell_MNC),
                cell_MCC = VALUES(cell_MCC),
                LAC = VALUES(LAC),
                serialno = VALUES(serialno)
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

    conn.commit()
    print("addALL işlemi tamamlandı.")
    print(f"{info['imei']} eklendi veya güncellendi.")

    # ********************Meterss*********************
    query_meters = """
                   INSERT INTO meters (imei, meter_serial) \
                   VALUES (%s, %s) ON DUPLICATE KEY \
                   UPDATE meter_serial= \
                   VALUES (meter_serial) \
                   """
    for cihaz in data:
        info = cihaz["INFORMATION"]
        meters_list = info["provisionedMeters"]

        for meter_serial_str in meters_list:
            meter_serial = int(meter_serial_str)
            values_meters = (int(info["imei"]), meter_serial)
            cursor.execute(query_meters, values_meters)

    conn.commit()
    print("meters tablosuna veri eklendi veya güncellendi.")

    # ***********************Taosos************************
    query_taosos = """
                   INSERT INTO taosos (imei, meter_serial, datetime, NumberOfSetValue, numofRecords, \
                                       numofReports, ReportsTimes, RecordsTimes) \
                   VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                   UPDATE \
                       NumberOfSetValue= \
                   VALUES (NumberOfSetValue), numofRecords= \
                   VALUES (numofRecords), numofReports= \
                   VALUES (numofReports), ReportsTimes= \
                   VALUES (ReportsTimes), RecordsTimes= \
                   VALUES (RecordsTimes) \
                   """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        taosos_list = cihaz["PERFORMANCE"]["TAOSOS"]["provisionedMeters"]

        for meter in taosos_list:
            values_taosos = (
                int(info["imei"]),
                int(meter["serialNumber"]),
                int(meter["NumberOfSetValue"]),
                int(meter["numofRecords"]),
                int(meter["numofReports"]),
                ",".join(meter["ReportsTimes"]),
                ",".join(meter["RecordsTimes"])
            )
            cursor.execute(query_taosos, values_taosos)

    conn.commit()
    print("taosos tablosuna veri eklendi veya güncellendi.")

    # *****************************Load Prfile***********************
    query_loadprofile = """
                        INSERT INTO loadprofile (imei, meter_serial, datetime, NumberOfSetValue, numofRecords, \
                                                 numofReports, \
                                                 RecordsTimes, ReportsTimes) \
                        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                        UPDATE \
                            NumberOfSetValue= \
                        VALUES (NumberOfSetValue), numofRecords= \
                        VALUES (numofRecords), numofReports= \
                        VALUES (numofReports), RecordsTimes= \
                        VALUES (RecordsTimes), ReportsTimes= \
                        VALUES (ReportsTimes) \
                        """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        loadprofile_list = cihaz["PERFORMANCE"]["LoadProfile"]["provisionedMeters"]

        for meter in loadprofile_list:
            values_loadprofile = (
                int(info["imei"]),
                int(meter["serialNumber"]),
                int(meter["NumberOfSetValue"]),
                int(meter["NumOfRecord"]),
                int(meter["NumOfReport"]),
                ",".join(meter["LogOfRecord"]),
                ",".join(meter["LogOfReport"])
            )
            cursor.execute(query_loadprofile, values_loadprofile)

    conn.commit()
    print("loadprofile tablosuna veri eklendi veya güncellendi.")

    # *************************Readout***********************************
    query_readout = """
                    INSERT INTO readout (imei, meter_serial, datetime, NumberOfSetValue, numofRecords, numofReports, \
                                         RecordsTimes, ReportsTimes) \
                    VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                    UPDATE \
                        NumberOfSetValue= \
                    VALUES (NumberOfSetValue), numofRecords= \
                    VALUES (numofRecords), numofReports= \
                    VALUES (numofReports), RecordsTimes= \
                    VALUES (RecordsTimes), ReportsTimes= \
                    VALUES (ReportsTimes) \
                    """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        readout_list = cihaz["PERFORMANCE"]["Readout"]["provisionedMeters"]

        for meter in readout_list:
            values_readout = (
                int(info["imei"]),
                int(meter["serialNumber"]),
                int(meter["NumberOfSetValue"]),
                int(meter["NumOfRecord"]),
                int(meter["NumOfReport"]),
                ",".join(meter["LogOfRecord"]),
                ",".join(meter["LogOfReport"])
            )
            cursor.execute(query_readout, values_readout)

    conn.commit()
    print("readout tablosuna veri eklendi veya güncellendi.")

    # *******************Obisread******************************

    query_obisread = """
                     INSERT INTO obisread (imei, meter_serial, datetime, NumberOfSetValue, numofRecords, numofReports, \
                                           RecordsTimes, ReportsTimes) \
                     VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                     UPDATE \
                         NumberOfSetValue= \
                     VALUES (NumberOfSetValue), numofRecords= \
                     VALUES (numofRecords), numofReports= \
                     VALUES (numofReports), RecordsTimes= \
                     VALUES (RecordsTimes), ReportsTimes= \
                     VALUES (ReportsTimes) \
                     """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        obisread_list = cihaz["PERFORMANCE"]["OBISRead"]["provisionedMeters"]

        for meter in obisread_list:
            values_obisread = (
                int(info["imei"]),
                int(meter["serialNumber"]),
                int(meter["NumberOfSetValue"]),
                int(meter["NumOfRecord"]),
                int(meter["NumOfReport"]),
                ",".join(meter["LogOfRecord"]),
                ",".join(meter["LogOfReport"])
            )
            cursor.execute(query_obisread, values_obisread)

    conn.commit()
    print("obisread tablosuna veri eklendi veya güncellendi.")

    # ********************************Relays********************************

    query_relays = """
                   INSERT INTO relays (imei, date, set_relay1_on, set_Relay1_off, set_relay2_on, set_relay2_off, \
                                       log_relay1_on, log_relay1_off, log_relay2_on, log_relay2_off) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                   UPDATE \
                       set_relay1_on= \
                   VALUES (set_relay1_on), set_Relay1_off= \
                   VALUES (set_Relay1_off), set_relay2_on= \
                   VALUES (set_relay2_on), set_relay2_off= \
                   VALUES (set_relay2_off), log_relay1_on= \
                   VALUES (log_relay1_on), log_relay1_off= \
                   VALUES (log_relay1_off), log_relay2_on= \
                   VALUES (log_relay2_on), log_relay2_off= \
                   VALUES (log_relay2_off) \
                   """

    #  tarihi bu gün olarak aldım
    today_date = datetime.today().date()

    for cihaz in data:
        info = cihaz["INFORMATION"]
        relays = cihaz["PERFORMANCE"]["RelaysInfo"]

        set_val = relays["SetValue"]
        log_val = relays["LogValue"]

        values_relays = (
            int(info["imei"]),
            today_date,
            set_val["Relay1ON"],
            set_val["Relay1OFF"],
            set_val["Relay2ON"],
            set_val["Relay2OFF"],
            ",".join(log_val.get("Relay1ON", [])),
            ",".join(log_val.get("Relay1OFF", [])),
            ",".join(log_val.get("Relay2ON", [])),
            ",".join(log_val.get("Relay2OFF", []))
        )
        cursor.execute(query_relays, values_relays)

    conn.commit()
    print("relays tablosuna veri eklendi veya güncellendi.")

    # **********************AC FAIL INFO****************
    query_ac_fail = """
                    INSERT INTO ac_fail_info (imei, date, ac_fail_time, ac_recovery_time) \
                    VALUES (%s, %s, %s, %s) ON DUPLICATE KEY \
                    UPDATE \
                        ac_fail_time= \
                    VALUES (ac_fail_time), ac_recovery_time= \
                    VALUES (ac_recovery_time) \
                    """

    today_date = datetime.today().date()

    for cihaz in data:
        info = cihaz["INFORMATION"]
        ac_fail = cihaz["PERFORMANCE"]["ACFailreInfo"]

        log_fail = ac_fail.get("LogOfACFailure", [])

        # İlk iki zaman varsa al, yoksa "00:00:00"
        ac_fail_time = log_fail[0] if len(log_fail) > 0 else '00:00:00'
        ac_recovery_time = log_fail[1] if len(log_fail) > 1 else '00:00:00'

        values_ac_fail = (
            int(info["imei"]),
            today_date,
            ac_fail_time,
            ac_recovery_time
        )

        cursor.execute(query_ac_fail, values_ac_fail)

    conn.commit()
    print("ac_fail_info tablosuna veri eklendi veya güncellendi.")

    # ***************************REBOOT INFO********************************
    query_reboot = """
                   INSERT INTO reboot_info (imei, date, reboot_time) \
                   VALUES (%s, %s, %s) ON DUPLICATE KEY \
                   UPDATE \
                       reboot_time= \
                   VALUES (reboot_time) \
                   """

    today_date = datetime.today().date()

    for cihaz in data:
        info = cihaz["INFORMATION"]
        reboot = cihaz["PERFORMANCE"]["RebootInfo"]

        log_reboot = reboot.get("LogOfReboot", [])

        # İlk reboot zamanını alalım, yoksa default '00:00:00'
        reboot_time = log_reboot[0] if len(log_reboot) > 0 else '00:00:00'

        values_reboot = (
            int(info["imei"]),
            today_date,
            reboot_time
        )

        cursor.execute(query_reboot, values_reboot)
    conn.commit()
    print("reboot_info tablosuna veri eklendi veya güncellendi.")

    # ******************************GSM CONNECTION INFO*******************
    query_gsm_conn = """
    INSERT INTO gsm_connection_info (
        imei, datetime, conn_or_disconn, ip, gsm_connect_type,
        SignalLevel, mmc, mcc, lac, cid
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        datetime = VALUES(datetime),
        conn_or_disconn = VALUES(conn_or_disconn),
        ip = VALUES(ip),
        gsm_connect_type = VALUES(gsm_connect_type),
        SignalLevel = VALUES(SignalLevel),
        mmc = VALUES(mmc),
        mcc = VALUES(mcc),
        lac = VALUES(lac),
        cid = VALUES(cid)
    """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        gsm_conn = cihaz["PERFORMANCE"]["GSMConnectionInfo"]

        # Bağlantı kayıtları (conn_or_disconn = True)
        for record in gsm_conn.get("LogOfConnectin", []):
            dt = datetime.strptime(record["date time"], "%d/%m/%Y %H:%M:%S")
            values = (
                int(info["imei"]),
                dt,
                True,
                record.get("IP", ""),
                record["gsm_connect_type"],
                int(record["SignalLevel"]),
                int(record["MNC"]),
                int(record["MCC"]),
                int(record["LAC"]),
                int(record["CID"])
            )
            cursor.execute(query_gsm_conn, values)

        # Kesinti kayıtları (conn_or_disconn = False)
        for record in gsm_conn.get("LogOfDisConnectin", []):
            dt = datetime.strptime(record["date time"], "%d/%m/%Y %H:%M:%S")
            values = (
                int(info["imei"]),
                dt,
                False,
                "",  # Disconnection kayıtlarında IP yok
                record["gsm_connect_type"],
                int(record["SignalLevel"]),
                int(record["MNC"]),
                int(record["MCC"]),
                int(record["LAC"]),
                int(record["CID"])
            )
            cursor.execute(query_gsm_conn, values)
    conn.commit()
    print("gsm_connection_info  eklendi.")

    # *********SERVER CONNECTION INFO**********************************
    query_server_conn = """
    INSERT INTO server_connection_info (
        imei, datetime, conn_or_disconn
    ) VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        datetime = VALUES(datetime),
        conn_or_disconn = VALUES(conn_or_disconn)
    """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        server_conn = cihaz["PERFORMANCE"]["ServerConnectionInfo"]

        # Bağlantı kayıtları (conn_or_disconn = True)
        for record in server_conn.get("LogOfConnectin", []):
            dt = datetime.strptime(record["date time"], "%d/%m/%Y %H:%M:%S")
            values = (
                int(info["imei"]),
                dt,
                True
            )
            cursor.execute(query_server_conn, values)

        # Kesinti kayıtları (conn_or_disconn = False)
        for record in server_conn.get("LogOfDisConnectin", []):
            dt = datetime.strptime(record["date time"], "%d/%m/%Y %H:%M:%S")
            values = (
                int(info["imei"]),
                dt,
                False
            )
            cursor.execute(query_server_conn, values)

    conn.commit()
    print("server_connection_info tablosuna veri eklendi.")

    # ************************Monitor****************************
    query_monitor = """
    INSERT INTO monitor (
        imei, datetime, usage_flash, usage_ram, battery_temp,
        battery_volt, cpu_temp, gsmSignalLevel
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        datetime = VALUES(datetime),
        usage_flash = VALUES(usage_flash),
        usage_ram = VALUES(usage_ram),
        battery_temp = VALUES(battery_temp),
        battery_volt = VALUES(battery_volt),
        cpu_temp = VALUES(cpu_temp),
        gsmSignalLevel = VALUES(gsmSignalLevel)
    """

    for cihaz in data:
        info = cihaz["INFORMATION"]
        monitor = cihaz["MONITOR"]["HourlyBased"]

        # örnek oalrak ilk değereleri kullanma
        usage_flash = int(monitor["usage_flash"][0].replace('%', ''))
        usage_ram = int(monitor["usage_ram"][0].replace('%', ''))
        battery_temp = int(monitor["battery_temp"][0].replace('C', ''))
        battery_volt = float(monitor["battery_volt"][0])
        cpu_temp = int(float(monitor["cpu_temp"][0].replace('C', '')))
        gsm_signal_level = int(monitor["gsmSignalLevel"][0])

        values_monitor = (
            int(info["imei"]),
            datetime.now(),
            usage_flash,
            usage_ram,
            battery_temp,
            battery_volt,
            cpu_temp,
            gsm_signal_level
        )

        cursor.execute(query_monitor, values_monitor)

    conn.commit()
    print("monitor tablosuna veri eklendi.")

    #  kapatma
    cursor.close()
    conn.close()

#print("gsm_no değeri:", comm["gsm_no"])

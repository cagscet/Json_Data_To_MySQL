import json
import mysql.connector

def delete_data(data, conn, cursor):
    tablo_listesi = [
        "ac_fail_info",
        "gsm_connection_info",
        "information",
        "loadprofile",
        "meters",
        "monitor",
        "obisread",
        "readout",
        "reboot_info",
        "relays",
        "taosos"
    ]

    try:
        # JSON doğrudan dict veya liste olabilir
        devices = [data] if isinstance(data, dict) else data

        for device in devices:
            print("Gelen data:", data)
            imei = device.get("imei")
            if not imei:
                print("Uyarı: IMEI bilgisi eksik, atlanıyor")
                continue

            try:
                cursor.execute("START TRANSACTION;")

                for table in tablo_listesi:
                    query = f"DELETE FROM `{table}` WHERE `imei` = %s"
                    cursor.execute(query, (imei,))
                    print(f"{table}: {cursor.rowcount} satır silindi")

                conn.commit()
                print(f"IMEI {imei} başarıyla silindi\n")

            except Exception as e:
                conn.rollback()
                print(f"IMEI {imei} silinirken hata: {str(e)}")

    except Exception as e:
        print("Genel hata:", str(e))



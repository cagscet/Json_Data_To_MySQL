import json
import mysql.connector

def update_data(data, conn, cursor):
    try:
        record_id = data.get("imei")
        updates = data.get("updates", {})

        if not record_id or not updates:
            print("Eksik veri: 'imei' ya da 'updates' eksik.")
            return

        for table_name, fields in updates.items():
            if not fields:
                continue

            # Kolonları ve değerleri hazırla
            columns = ', '.join([f"{col} = %s" for col in fields.keys()])
            values = list(fields.values())

            sql = f"UPDATE {table_name} SET {columns} WHERE imei = %s"
            values.append(record_id)  # WHERE id = ? kısmı için id’yi sona ekle

            try:
                cursor.execute(sql, values)
                print(f"{table_name} tablosu güncellendi.")
            except Exception as e:
                print(f"{table_name} tablosu güncellenirken hata oluştu:", e)

    except Exception as e:
        print("update_data fonksiyonunda genel bir hata oluştu:", e)









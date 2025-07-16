import json
import mysql.connector
from addALL import insert_all
from add import insert_single
from update import update_data
from remove import delete_data
from sqlCommand import run_sql_command

def handle_command(command, data, conn, cursor): #Burada fonksiyona göre içe aktarma yapılıyor
    if command == "addALL":
        insert_all(data, conn, cursor)
    elif command == "add":
        insert_single(data, conn, cursor)
    elif command == "update":
        update_data(data, conn, cursor)
    elif command == "remove":
        delete_data(data, conn, cursor)
    elif command == "sqlCommand":
        run_sql_command(data, conn, cursor)
    else:
        print("Geçersiz komut:", command)

# def handle_command(command, data, conn, cursor): #deneme hepsini ekleme
#     command_from_gateway = "addALL"
#     handle_command(command_from_gateway, data, conn, cursor)

if __name__ == "__main__":
    with open(" json konumu ", "r", encoding="utf-8") as file:
        data = json.load(file)
    #print("Veri tipi:", type(data)) kontrol amaçlı

    # Bağlantıyı burada açıyoruz
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="yourpassword",
        database="jsonv2"
    )
    cursor = conn.cursor()

    #print("Veri tipi:", type(data))

    command_from_gateway = data.get("command")
    # Eğer "data" varsa onu al, yoksa tüm json'ı data olarak atar sqlCommand için
    data = data.get("data") if "data" in data else data # hem data hem query aldırır

    handle_command(command_from_gateway, data, conn, cursor)
    #delete_data_by_imei(869604065500199, conn, cursor)

    conn.commit()   # Commit önce
    cursor.close()  # Sonra cursor kapat
    conn.close()    # Ve bağlantıyı kapat

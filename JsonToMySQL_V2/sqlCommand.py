def run_sql_command(data, conn, cursor):
    query = data.get("query")
    if not query:
        print("SQL sorgusu eksik veya yok")
        return

    try:
        cursor.execute(query)

        if query.strip().lower().startswith("select"): #başında selecet varsa sorguyu küçük harf yapıp alır
            results = cursor.fetchall() #sorgudan dönen tüm satırları alır
            columns = [desc[0] for desc in cursor.description]

            print("Sorgu Sonucu : ")
            for row in results:
                row_dict = dict(zip(columns, row)) #satır değerler ile kolon isimlerini eşleştrip bir dict yapar
                print(row_dict)

        else:
            conn.commit()
            print(f"Sorgu başarıyla çalıştırıldı. Etkilenen satır : {cursor.rowcount}")

    except Exception as e:
        print("SQL komutu çalıştırılırken bir hata oluştu ",e)

import cx_Oracle as cxO
import pandas as pd
import requests
import json
from datetime import datetime

con_sd = cxO.connect('tiago_teste/tiago_teste#desenv@dbaesabi.teste.com.br:1521/tiago_teste')
cursor_sd = con_sd.cursor()
base_api = "teste_teste"

# Verifica se a tabela existe
cursor_sd.execute("SELECT COUNT(*) FROM user_tables WHERE table_name = :1", (base_api.upper(),))
regra_base_existente = cursor_sd.fetchone()[0]

# Cria a tabela se não existir
if not regra_base_existente:
    criação_tabela = f"""
    CREATE TABLE {base_api} (
        id VARCHAR2(20),
        userId VARCHAR2(100),
        "date" DATE,
        products VARCHAR2(4000)
    )
    """
    cursor_sd.execute(criação_tabela)


# definicao da data de inicio e data fim
start_date = "2019-12-10"
end_date = "2020-10-10"

url = f"https://fakestoreapi.com/carts?startdate={start_date}&enddate={end_date}"

# Passo da verificação onde acontece o consumo da api e reconhecimento de json
response = requests.get(url)
if response.status_code == 200:
    try:
        json_data = response.json()
        df = pd.DataFrame(json_data)
        
        for index, row in df.iterrows():
            date_str = row['date']
            date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ').date()
            row['date'] = date_obj
            
            # Converte a lista de produtos para uma string JSON
            products_str = json.dumps(row['products'])
            
            cursor_sd.execute(f"SELECT COUNT(*) FROM {base_api} WHERE userId = :1", (row['userId'],))
            resultado = cursor_sd.fetchone()[0]

            if resultado > 0:
                atualização_dos_dados = f"""
                UPDATE {base_api}
                SET id = :1, "date" = TO_DATE(:3, 'YYYY-MM-DD'), products = :4
                WHERE userId = :2
                """
                cursor_sd.execute(atualização_dos_dados, (row['id'], row['userId'], row['date'].strftime('%Y-%m-%d'), products_str))
            else:
                insert_query = f"INSERT INTO {base_api} (id, userId, \"date\", products) VALUES (:1, :2, TO_DATE(:3, 'YYYY-MM-DD'), :4)"
                cursor_sd.execute(insert_query, (row['id'], row['userId'], row['date'].strftime('%Y-%m-%d'), products_str))
        
        con_sd.commit()
    except ValueError as e:
        print(f"Erro ao processar JSON: {e}")
else:
    print(f"Erro ao acessar a API: Status code {response.status_code}")

cursor_sd.close()
con_sd.close()
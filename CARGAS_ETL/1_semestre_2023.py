
import requests
import pandas as pd
import sqlalchemy as sa
import time

print("Carga iniciada")


SERVER = 'WIN-4QCVT3LAP21' 
DATABASE = 'DB_PNCP'
CONN_STR = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"


engine = sa.create_engine(CONN_STR, fast_executemany=True)


datas = pd.date_range(start="2023-01-01", end="2023-06-30")

total_inserido = 0

for data_atual in datas:
    data_str = data_atual.strftime('%Y%m%d')
    data_formatada = data_atual.strftime('%d/%m/%Y')
    
    pagina = 1
    total_paginas = 1 
    
    print(f"\n extração do dia: {data_formatada}")
    
    while pagina <= total_paginas:
        
        URL_API = f"https://pncp.gov.br/api/consulta/v1/contratos?dataInicial={data_str}&dataFinal={data_str}&pagina={pagina}"
        
        try:
            
            response = requests.get(URL_API, timeout=30)
            response.raise_for_status()
            json_resposta = response.json()
            
            
            total_paginas = json_resposta.get('totalPaginas', 1)
            contratos = json_resposta.get('data', [])
            
            if not contratos:
                print(f"  Pg {pagina}/{total_paginas}: Vazia Sem dados.")
                break 
                
            
            df = pd.json_normalize(contratos)
            
            
            df.columns = [c.replace('.', '_') for c in df.columns]
            

            df.to_sql('contratos_raw', schema='bronze', con=engine, if_exists='append', index=False)
            
            linhas_bloco = len(contratos)
            total_inserido += linhas_bloco
            
            print(f"   Pag {pagina}/{total_paginas}: +{linhas_bloco} registos inseridos (Total acumulado: {total_inserido})")
            
            pagina += 1
            
            
            time.sleep(1) 
            
        except requests.exceptions.HTTPError as err_http:
            print(f" ERRO DE REDE/paI no dia {data_formatada}, pagina {pagina}: {err_http}")
            break 
            
        except Exception as e:
            print(f" ERRO DE EXECUÇÃO no dia {data_formatada}, página {pagina}: {e}")
            break

print(f"\n  Total de {total_inserido} contratos inseridos")

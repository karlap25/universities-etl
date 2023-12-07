import requests
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

countries = ["Guatemala", "Costa Rica", "Nicaragua", "El Salvador", "Honduras", "Panama"]

def extract(api_url) -> dict:
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error en la solicitud. Código de estado: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error al hacer la solicitud a la API: {e}")
        return None

def guardar_en_txt(data, archivo_txt):
    try:
        with open(archivo_txt, 'w', encoding='utf-8') as txtfile:
            for item in data:
                txtfile.write(f'{item}\n')
        print(f"Datos guardados exitosamente en {archivo_txt}")
    except Exception as e:
        print(f"Error al guardar en archivo de texto: {e}")

def transform(data: dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Numero total de universidades: {len(data)}")
    df = df[df['country'].isin(countries)]
    print(f"Numero de universidades en Centro america: {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "name", "web_pages"]]

def load(df:pd.DataFrame):
    connectionString = f'mssql+pyodbc://kep:Bobi*5598@kepserver.database.windows.net:1433/kep?driver=ODBC+Driver+17+for+SQL+Server'
    engine = sqlalchemy.create_engine(connectionString)
    r = df.to_sql(name="universities", schema="etl", con=engine, if_exists='replace', index=False)
    print(f"{r} datos  cargados a la BD.")

def etl():
    # Lista de países a consultar en la API
    todos_los_datos = []

    # Iterar sobre los países y obtener datos de la API
    for country in countries:
        url_api = f"http://universities.hipolabs.com/search?country={country}"
        data = extract(url_api)

        if data:
            print(f"Datos obtenidos para {country}")
            todos_los_datos.extend(data)
        else:
            print(f"No se obtuvieron datos de la API para {country}. Verifica la URL de la API y la disponibilidad de datos.")

    archivo_txt = "todos_los_datos.txt"
    print(f"Total de datos obtenidos: {len(todos_los_datos)}")
    guardar_en_txt(todos_los_datos, archivo_txt)

    popo = transform(todos_los_datos)
    load(popo)

if __name__ == '__main__':
    etl()
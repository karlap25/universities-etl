# ETL proyect
## ETL
ETL is a data integration method that combine data from diferents sources. ETL cleanses and organizes data in a way which addresses.

This process is divided into three phases:

### Extract
Data is extracted from API source
```python
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
```
### Tranform
The extracted data is transformed into a format for analysis and reporting. This involves cleaning , filtering, aggregating and converting the data.
```python
def transform(data: dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Numero total de universidades: {len(data)}")
    df = df[df['country'].isin(countries)]
    print(f"Numero de universidades en Centro america: {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "name", "web_pages"]]

```

### Load
The transformed data is loaded into a database, in this case I used the new free database from Microsoft.
```python
def load(df:pd.DataFrame):
    connectionString = f'mssql+pyodbc://kep:Bobi*5598@kepserver.database.windows.net:1433/kep?driver=ODBC+Driver+17+for+SQL+Server'
    engine = sqlalchemy.create_engine(connectionString)
    r = df.to_sql(name="universities", schema="etl", con=engine, if_exists='replace', index=False)
    print(f"{r} datos  cargados a la BD.")

```

#Requirements
SQLAlchemy           2.0.23
python-dateutil      2.8.2
pyodbc               5.0.1
pip                  23.3.1
pandas               1.4.3
nbclient             0.6.4
nbformat             5.4.0
ipython              8.4.0
ipython-genutils     0.2.0
fastjsonschema       2.15.3
jsonschema           4.6.0
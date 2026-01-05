import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

commodities = ['CL=F', 'GC=F', 'SI=F']

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine= create_engine(DATABASE_URL)

def buscar_dados_commodities(simbolo, start='2024-06-01', end='2024-06-30'):
    ticker = yf.Ticker(simbolo)

    dados = ticker.history(
        start=start,
        end=end,
        interval='1d'
    )[['Close']]

    dados = dados.reset_index()  # Date vira coluna
    dados.rename(columns={'Date': 'Date', 'Close': 'Close'}, inplace=True)

    dados['simbolo'] = simbolo

    return dados

def buscar_todos_dados_commodities(commodities):
    todos_dados = []
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_no_db(df, schema='public'):
    df.to_sql(
        'commodities',
        engine,
        if_exists='replace',
        index=False, 
        schema=schema
    )
    
if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    salvar_no_db(dados_concatenados, schema='public')
    
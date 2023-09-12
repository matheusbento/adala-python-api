import psycopg2 #
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
import json
import pandas as pd
from connection.database import generate_session
from json import JSONEncoder
import numpy
import os
import psycopg2
class Report:

    def __init__(self, schema_name):
        self.schema_name = schema_name

    def get_dataset(self, conn, data, filters):
        sql_select = "SELECT * FROM main"
        if filters['time_observation_start'] is not None or filters['date_observation_start'] is not None:
            sql_select += " WHERE true"

        if filters['time_observation_start'] is not None:
            sql_select += f" AND time_obs BETWEEN '{filters['time_observation_start']}' AND '{filters['time_observation_end']}' ORDER BY time_obs"
        elif filters['date_observation_start'] is not None:
            sql_select += f" AND date_obs BETWEEN '{filters['date_observation_start']}' AND '{filters['date_observation_end']}' ORDER BY date_obs"

        print(sql_select)
        result = conn.execute(sql_select)
        nomes_colunas = result.keys()
        # Recupera os resultados da consulta
        df = pd.DataFrame(result, columns=nomes_colunas)

        dados = df.iloc[:, len(df.columns)-2].to_numpy()
        dados2 = map(np.array, dados)
        d = np.array(list(dados2))

        dd = np.transpose(d)

        return dd

    def convert_to_json_serializable(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.astype(float).tolist()  # Convert Decimal to float and then to list
        elif isinstance(obj, tuple):
            return tuple(self.convert_to_json_serializable(item) for item in obj)
        elif isinstance(obj, list):
            return [self.convert_to_json_serializable(item) for item in obj]
        else:
            return obj

    def get(self, chart_type, data_processing_method, data_column, filter):
        session = generate_session(self.schema_name)
        data = {
            'chart_type': chart_type,
            'data_processing_method': data_processing_method,
            'data_column': data_column
        }
        filters = {'date_observation_start':None, 'date_observation_end':None, 'time_observation_start':'110726',
                    'time_observation_end':'111416.174361'}
        result = self.get_dataset(session(), data, filters)
        return self.convert_to_json_serializable(result)
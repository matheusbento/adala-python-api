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

    def operator_to_sql(self, item):
        switcher = {
            "between": f" AND {item.get('column')} BETWEEN '{item.get('min_value')}' AND '{item.get('max_value')}'"
        }

        # get() method of dictionary data type returns
        # value of passed argument if it is present
        # in dictionary otherwise second argument will
        # be assigned as default value of passed argument
        return switcher.get(item.get('operation'), "nothing")

    def get_dataset(self, conn, data, filters):
        sql_select = "SELECT * FROM main"
        sql_select += " WHERE true"

        if filters is not None:
            for item in filters:
                sql_select += self.operator_to_sql(item)

        sql_select += " ORDER BY delta_t"
        # if filters['time_observation_start'] is not None:
        #     sql_select += f" AND time_obs BETWEEN '{filters['time_observation_start']}' AND '{filters['time_observation_end']}' ORDER BY time_obs"
        # elif filters['date_observation_start'] is not None:
        #     sql_select += f" AND date_obs BETWEEN '{filters['date_observation_start']}' AND '{filters['date_observation_end']}' ORDER BY date_obs"

        print(sql_select)
        result = conn.execute(sql_select)
        nomes_colunas = result.keys()
        # Recupera os resultados da consulta
        df = pd.DataFrame(result, columns=nomes_colunas)

        df_filtered = df.iloc[:, len(df.columns)-2]
        print(df_filtered.head())
        dados = df_filtered.to_numpy()
        dados2 = map(np.array, dados)
        d = np.array(list(dados2))

        # tirei um transpose daqui para testar os fits
        data = np.transpose(d)

        return data, df

    def convert_to_json_serializable(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.astype(float).tolist()  # Convert Decimal to float and then to list
        elif isinstance(obj, tuple):
            return tuple(self.convert_to_json_serializable(item) for item in obj)
        elif isinstance(obj, list):
            return [self.convert_to_json_serializable(item) for item in obj]
        else:
            return obj

    def get(self, chart_type, data_processing_method, data_column, filters):
        session = generate_session(self.schema_name)
        data = {
            'chart_type': chart_type,
            'data_processing_method': data_processing_method,
            'data_column': data_column
        }
        # filters = {'date_observation_start':None, 'date_observation_end':None, 'time_observation_start':'110726',
        #             'time_observation_end':'111416.174361'}
        result, df = self.get_dataset(session(), data, filters)

        return self.convert_to_json_serializable(result)

    def converter_para_str(self, valor):
        try:
            return str(valor)
        except Exception:
            return ''

    def is_string(self, element):
        return isinstance(element, str)

    def calcular_media_ou_concatenar(self, column):
        if column.dtype.kind in 'iufc':  # Verifica se a coluna é numérica (int, float ou complex)
            return str(column.mean())
        else:
            return str(column.unique()[0])

    def generate(self, chart_type, data_processing_method, data_column, filters):
        session = generate_session(self.schema_name)
        data = {
            'chart_type': chart_type,
            'data_processing_method': data_processing_method,
            'data_column': data_column
        }
        # filters = {'date_observation_start':None, 'date_observation_end':None, 'time_observation_start':'110726',
        #             'time_observation_end':'111416.174361'}
        result, df = self.get_dataset(session(), data, filters)
        sort_by = [item["column"] for item in filters]
        new_df = df.drop(columns=['id', data_column])
        # print(new_df.dtypes)
        print(new_df)
        filtered_df = new_df.apply(self.calcular_media_ou_concatenar)
        print(filtered_df, filtered_df.dtypes)
        hdu = fits.PrimaryHDU(result)

        for column_name, column_data in filtered_df.iteritems():
            header_key = column_name.upper()  # Usar o nome da coluna em letras maiúsculas como chave do cabeçalho
            header_value = str(column_data)  # Usar o primeiro valor da coluna como valor do cabeçalho (assumindo que os valores são todos iguais)
            hdu.header[header_key] = header_value

        return hdu
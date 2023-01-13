import string
import deprecation
import pandas as pd
import numpy as np
import os
import sys
import datetime

from connection.database import generate_session
from repository.schema_repository import SchemaRepository

sys.path.append("..")

pd.options.mode.chained_assignment = None


class Builder:

    def __init__(self, schema_name: string, file_path: string):
        session = generate_session()
        self.db = session()
        self.schema_name = schema_name
        self.file_path = file_path
        self.entities = {}
        self.complex_keys = {}
        self.last_elements = {}
        self.import_data = dict()

    @staticmethod
    def get_dataframe(file_path: string) -> pd:
        # Split the extension from the path and normalise it to lowercase.
        ext = os.path.splitext(file_path)[-1].lower()

        # Now we can simply use == to check for equality, no need for wildcards.
        if ext == ".pkl":
            return pd.read_pickle(file_path)
        elif ext == ".csv":
            return pd.read_csv(file_path, sep=';')
        else:
            print(ext, "is an unknown file format.")

    @staticmethod
    def map_type(element_type):
        types = dict({
            "str": 'varchar',
            "DataFrame": "DataFrame",
            "ndarray": "jsonb",
            "list": "jsonb",
            "Timestamp": "timestamp",
            "datetime": "timestamp",
            "float64": "numeric",
            "int": "int",
            "int64": "int"
        })

        return types[element_type] if types[element_type] else element_type

    @staticmethod
    def map_size(element_type):
        sizeMapper = dict({
            "str": '255',
            "int": None,
            "Timestamp": None,
            "float64": None,
            "ndarray": None,
            "list": None,
            "int64": None,
            "datetime": None,
        })

        return sizeMapper[element_type] if sizeMapper[element_type] else None

    def parse_type(self, element, column):
        val = dict()
        val['name'] = column.lower().replace(" ", "_")
        val['type'] = "serial" if column == "id" else self.map_type(type(element.loc[column]).__name__)
        val['size'] = None if column == "id" else self.map_size(type(element.loc[column]).__name__)
        val['auto_increment'] = True if column == 'id' else False

        return dict(val)

    def get_dataframe_keys(self, data, parent_table_name=None, parent_id=None, remove_complex_keys=None):
        first_element = data.iloc[0]
        first_element['id'] = 0
        columns = data.columns.values
        columns = list(filter(lambda x: not isinstance(first_element.loc[x], pd.core.frame.DataFrame), columns))

        keys_prefix = np.array(['id'])
        columns = np.concatenate((keys_prefix, columns), axis=0)
        if parent_table_name:
            first_element["{tableName}_id".format(tableName=parent_table_name)] = 0
            keysSuffix = np.array(["{tableName}_id".format(tableName=parent_table_name)])
            columns = np.concatenate((columns, keysSuffix), axis=0)

        keys = list()
        for column in columns:
            keys.append(self.parse_type(first_element, column))

        return keys

    @staticmethod
    def get_dataframe_keys_type(data, parent_table_name=None, parent_id=None):
        first_element = data.iloc[0]
        columns = data.columns.values

        original_keys = list(filter(lambda x: not isinstance(first_element.loc[x], pd.core.frame.DataFrame), columns))
        keys_prefix = np.array(['id'])
        keys = np.array([x.lower().replace(" ", "_") if isinstance(x, str) else x for x in original_keys])
        keys = np.concatenate((keys_prefix, keys), axis=0)
        if parent_table_name:
            keys_suffix = np.array(["{tableName}_id".format(tableName=parent_table_name)])
            keys = np.concatenate((keys, keys_suffix), axis=0)

        original_complex_keys = list(
            filter(lambda x: isinstance(first_element.loc[x], pd.core.frame.DataFrame), columns))
        complex_keys = np.array(
            [x.lower().replace(" ", "_") if isinstance(x, str) else x for x in original_complex_keys])
        return keys, complex_keys, original_complex_keys

    def get_last_id_from_table(self, table_name):
        if table_name not in self.last_elements:
            self.last_elements[table_name] = 0

        self.last_elements[table_name] = self.last_elements[table_name] + 1

        return self.last_elements[table_name]

    def create_table(self, table_name: string, headers):
        def build_fields(x):
            return "{column} {type}{size}{auto}".format(
                column=x['name'],
                type=x['type'],
                size="" if x['size'] is None else "({size})".format(size=x['size']),
                auto=" PRIMARY KEY" if x['auto_increment'] == True else "")

        params = list(map(build_fields, headers))
        separator = ', '

        columns = separator.join(params)
        if self.db is not None:
            sql = "CREATE TABLE IF NOT EXISTS {tableName}({columns})".format(tableName=table_name, columns=columns)
            self.db.execute(sql)
        return True

    @staticmethod
    def build_row_to_insert(values):
        row = list()

        for i in values:
            if isinstance(i, pd._libs.tslibs.timestamps.Timestamp) or isinstance(i, datetime.datetime):
                row.append(i.strftime("%Y-%m-%d %H:%M:%S"))
            elif isinstance(i, np.ndarray) or isinstance(i, list):
                row.append(str(pd.Series(i, dtype='float64').to_json(orient='values')))
            else:
                row.append(i)

        return str(tuple(row))

    @staticmethod
    def chunks(old_list, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(old_list), n):
            yield old_list[i:i + n]

    def insert_values_on_database(self):
        for key in self.import_data.keys():
            values = self.import_data[key]

            values_chunks = list(self.chunks(values, 1000))
            for val in values_chunks:
                statements = ",".join(val)
                if self.db is not None:
                    sql = "INSERT INTO {tableName} VALUES {columns}".format(tableName=key, columns=statements)
                    self.db.execute(sql)

    @deprecation.deprecated(deprecated_in="1.0", removed_in="2.0",
                            current_version=2,
                            details="Use the insert_values_on_database instead")
    def insert_values_on_database(self, table_name, values):
        row = list()

        for i in values:
            if isinstance(i, pd._libs.tslibs.timestamps.Timestamp) or isinstance(i, datetime.datetime):
                row.append(i.strftime("%Y-%m-%d %H:%M:%S"))
            elif isinstance(i, np.ndarray) or isinstance(i, list):
                row.append(str(pd.Series(i, dtype='float64').to_json(orient='values')))
            else:
                row.append(i)

        if self.db is not None:
            sql = "INSERT INTO {tableName} VALUES {columns}".format(tableName=table_name, columns=tuple(row))
            self.db.execute(sql)

    def process_dataframe(self, data, table_name: string, parent_table_name: string = None, parent_id: string = None):
        original_complex_keys = None
        table_name_lower = table_name.lower()
        if table_name_lower not in self.entities:
            headers = self.get_dataframe_keys(data, parent_table_name, parent_id, True)
            self.create_table(table_name_lower, headers)

        if table_name_lower not in self.entities or table_name_lower not in self.complex_keys:
            keys, cKeys, original_complex_keys = self.get_dataframe_keys_type(data, parent_table_name, parent_id)
            if table_name_lower not in self.entities:
                self.entities[table_name_lower] = keys
            if table_name_lower not in self.complex_keys:
                print("Storing complexing keys for", table_name_lower, "values", original_complex_keys)
                self.complex_keys[table_name_lower] = original_complex_keys

        if table_name_lower in self.complex_keys and original_complex_keys is None:
            original_complex_keys = self.complex_keys[table_name_lower]

        for i, values in data.iterrows():
            array_prefix = pd.Series([self.get_last_id_from_table(table_name_lower)], index=['id'])
            row = values[values.apply(lambda x: not isinstance(x, pd.core.frame.DataFrame))]
            row = pd.concat([array_prefix, row])
            if parent_table_name:
                row["{tableName}_id".format(tableName=parent_table_name)] = str(parent_id)

            if table_name_lower not in self.import_data:
                self.import_data[table_name_lower] = list()

            self.import_data[table_name_lower].append(self.build_row_to_insert(row))

            for head in original_complex_keys:
                self.process_dataframe(values.loc[head], head, table_name_lower, i)

    def process(self):
        schema_repo = SchemaRepository(self.db, self.schema_name)
        self.db = schema_repo.create_schema()
        data_frame = self.get_dataframe(self.file_path)
        self.process_dataframe(data_frame, 'main')
        self.insert_values_on_database()
        self.db.commit()

        # print(self.import_data);
        return self.entities, self.complex_keys

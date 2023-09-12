import string
from io import StringIO, BytesIO
import deprecation
import pandas as pd
import numpy as np
from astropy.io import fits
import os
import sys
import datetime
from astropy.time import Time
from json import JSONEncoder
import json

from connection.database import generate_session
from s3.bucket import generate_s3_session
from repository.schema_repository import SchemaRepository

sys.path.append("..")

pd.options.mode.chained_assignment = None

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)


class Builder:

    def __init__(self, schema_name: string, file_paths: string, attributes: string, bucket_name: string = None, extra = None):
        session = generate_session()
        s3_session, s3_bucket_name = generate_s3_session(bucket_name)
        self.s3 = s3_session
        self.bucket_name = s3_bucket_name
        self.db = session()
        self.schema_name = schema_name.lower()
        self.file_paths = file_paths
        self.attributes = attributes
        self.entities = {}
        self.complex_keys = {}
        self.last_elements = {}
        self.extra = extra
        self.import_data = dict()

    def time_to_jd(self, date, time):
        date = date.replace('/', '-')
        formatted_time_string = f"{date[:4]}-{date[4:6]}-{date[6:8]} {time[:2]}:{time[2:4]}:{time[4:]}"
        return Time(formatted_time_string, format='iso', scale='utc').jd

    def get_dataframe_from_fits(self, fits_data) -> pd.DataFrame:
        hdul = fits.open(fits_data)
        header_data = hdul[0].header
        initial_time = self.time_to_jd(header_data['DATE-OBS'], header_data['TIME-OBS'])
        final_time = self.time_to_jd(header_data['DATE-END'], header_data['TIME-END'])
        delta_t = (final_time - initial_time) / len(hdul[0].data)

        wfall = np.transpose(hdul[0].data)

        rows = []
        for i, wf in enumerate(wfall):
            row = {}
            for key in header_data.keys():
                val = header_data[key]
                if isinstance(val, (int, float, str, bool)):  # você pode adicionar mais tipos aqui se necessário
                    row[key] = val
            row['water_fall'] = json.dumps(wf.tolist())
            row['delta_t'] = float(initial_time + delta_t * i)
            rows.append(row)

        df = pd.DataFrame(rows)
        return df


    def get_dataframe(self, object_path) -> pd:
        print(self.bucket_name)
        data = self.s3.get_object(self.bucket_name, object_path)
        headers = data.getheaders()
        mime_type = headers['Content-Type'];
        print("MIME", mime_type)
        if mime_type == "image/fits":
            print(type(data.data));
            fits_data = BytesIO(data.data)
            return self.get_dataframe_from_fits(fits_data)
        else:
            content = data.data.decode();
            if mime_type == "application/octet-stream":
                return pd.read_pickle(content)
            elif mime_type == "text/plain":
                return pd.read_json(content)
            elif mime_type == "application/json":
                return pd.read_json(content)
            elif mime_type == "text/csv":
                return pd.read_csv(StringIO(content), sep=',')
            else:
                print(mime_type, "is an unknown file format.")

        return None

    @staticmethod
    def map_type(element_type, element):
        types = dict({
            "str": 'varchar',
            "DataFrame": "DataFrame",
            "ndarray": "jsonb",
            "list": "jsonb",
            "Timestamp": "timestamp",
            "datetime": "timestamp",
            "float64": "numeric",
            "int": "int",
            "int64": "int",
            "bool": 'boolean',
            "bool_": 'boolean',
        })

        type = types[element_type] if types[element_type] else element_type;

        return "text" if type == 'varchar' and len(element) >= 255 else type

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
            "bool": None,
            "bool_": None,
        })

        return sizeMapper[element_type] if sizeMapper[element_type] else None

    def parse_type(self, element, column):
        type_data = "serial" if column == "id" else self.map_type(type(element.loc[column]).__name__, element.loc[column]);
        type_data = "jsonb" if column == 'water_fall' else type_data
        print("COLUMN", column, type_data)
        val = dict()
        val['name'] = column.lower().replace(" ", "_").replace("-", "_")
        val['type'] = type_data
        val['size'] = None if column == "id" or type_data == 'text' or type_data == 'jsonb' else self.map_size(type(element.loc[column]).__name__)
        val['auto_increment'] = True if column == 'id' else False

        return dict(val)

    def get_dataframe_keys(self, data, parent_table_name=None, parent_id=None, remove_complex_keys=None):
        first_element = data.iloc[0]
        first_element['id'] = 0
        columns = data.columns.values
        columns = list(filter(lambda x: not isinstance(first_element.loc[x], pd.core.frame.DataFrame) and not isinstance(first_element.loc[x], pd.core.series.Series), columns))

        keys_prefix = np.array(['id'])
        columns = np.concatenate((keys_prefix, columns), axis=0)
        if parent_table_name:
            first_element["{tableName}_id".format(tableName=parent_table_name)] = 0
            keysSuffix = np.array(["{tableName}_id".format(tableName=parent_table_name)])
            columns = np.concatenate((columns, keysSuffix), axis=0)

        keys = list()
        for column in columns:
            keys.append(self.parse_type(first_element, column))

        return keys, columns

    @staticmethod
    def get_dataframe_keys_type(data, parent_table_name=None, parent_id=None):
        first_element = data.iloc[0]
        columns = data.columns.values

        original_keys = list(filter(lambda x: not isinstance(first_element.loc[x], pd.core.frame.DataFrame) and not isinstance(first_element.loc[x], pd.core.series.Series), columns))
        keys_prefix = np.array(['id'])
        keys = np.array([x.lower().replace(" ", "_") if isinstance(x, str) else x for x in original_keys])
        keys = np.concatenate((keys_prefix, keys), axis=0)
        if parent_table_name:
            keys_suffix = np.array(["{tableName}_id".format(tableName=parent_table_name)])
            keys = np.concatenate((keys, keys_suffix), axis=0)

        original_complex_keys = list(
            filter(lambda x: isinstance(first_element.loc[x], pd.core.frame.DataFrame) or isinstance(first_element.loc[x], pd.core.series.Series), columns))
        complex_keys = np.array(
            [x.lower().replace(" ", "_") if isinstance(x, str) else x for x in original_complex_keys])
        return keys, complex_keys, original_complex_keys

    def get_last_id_from_table(self, table_name):
        if table_name not in self.last_elements:
            result = self.db.execute(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1;")
            last_id = result.scalar()
            self.last_elements[table_name] = 0 if last_id is None else last_id

        self.last_elements[table_name] = self.last_elements[table_name] + 1

        return self.last_elements[table_name]

    def create_table(self, table_name: string, headers):
        def build_fields(x):
            return "{column} {type}{size}{auto}".format(
                column=x['name'].replace("(%)", ""),
                type=x['type'],
                size="" if x['size'] is None else "({size})".format(size=x['size']),
                auto=" PRIMARY KEY" if x['auto_increment'] == True else "")

        params = list(map(build_fields, headers))
        separator = ', '

        columns = separator.join(params)
        if self.db is not None:
            sql = "CREATE TABLE IF NOT EXISTS {tableName}({columns})".format(tableName=table_name, columns=columns)
            print("LOG CREATE TABLE", sql)
            self.db.execute(sql)
        return True

    @staticmethod
    def build_row_to_insert(values):
        row = list()

        for i in values:
            if isinstance(i, pd._libs.tslibs.timestamps.Timestamp) or isinstance(i, datetime.datetime):
                row.append(i.strftime("%Y-%m-%d %H:%M:%S"))
            elif isinstance(i, np.ndarray) or isinstance(i, list):
                print("VALOR I",i);
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

    # @deprecation.deprecated(deprecated_in="1.0", removed_in="2.0",
    #                         current_version=2,
    #                         details="Use the insert_values_on_database instead")
    # def insert_values_on_database(self, table_name, values):
    #     row = list()
    #
    #     for i in values:
    #         if isinstance(i, pd._libs.tslibs.timestamps.Timestamp) or isinstance(i, datetime.datetime):
    #             row.append(i.strftime("%Y-%m-%d %H:%M:%S"))
    #         elif isinstance(i, np.ndarray) or isinstance(i, list):
    #             row.append(str(pd.Series(i, dtype='float64').to_json(orient='values')))
    #         else:
    #             row.append(i)
    #
    #     if self.db is not None:
    #         sql = "INSERT INTO {tableName} VALUES {columns}".format(tableName=table_name, columns=tuple(row))
    #         self.db.execute(sql)

    def process_dataframe(self, data, attributes, table_name: string, parent_table_name: string = None, parent_id: string = None):
        original_complex_keys = None
        print("NEW ATTRIBUTE", attributes, table_name.lower(), self.entities);
        # attribute = attributes[table_name]
        table_name_lower = table_name.lower()
        if table_name_lower not in self.entities:
            headers, columns = self.get_dataframe_keys(data, parent_table_name, parent_id, True)
            print("COLIUMNS", columns)
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
            # print("VALS", values);
            row = values.loc[[col for col in columns if col != 'id' and col in values.index]]
            # row = values[values.apply(lambda x: not isinstance(x, (pd.core.frame.DataFrame, list, pd.core.series.Series)))]
            row = pd.concat([array_prefix, row])
            if parent_table_name:
                row["{tableName}_id".format(tableName=parent_table_name)] = str(parent_id)

            if table_name_lower not in self.import_data:
                self.import_data[table_name_lower] = list()

            self.import_data[table_name_lower].append(self.build_row_to_insert(row))

            for head in original_complex_keys:
                if isinstance(values.loc[head], pd.core.frame.DataFrame):
                    self.process_dataframe(values.loc[head],attributes, head, table_name_lower, i)

    def process(self):
        schema_repo = SchemaRepository(self.db, self.schema_name)
        self.db = schema_repo.create_schema()
        print(self.file_paths)
        for file_path in self.file_paths:
            self.entities = {}
            self.complex_keys = {}
            print(file_path)
            print("FILE_PATH", file_path, file_path['path'])
            data_frame = self.get_dataframe(file_path['path'])
            print("Attribute", self.attributes, file_path);
            if self.extra is None:
                attributes = list(filter(lambda attribute: attribute['file_id'] == file_path['id'], self.attributes))[0]
            else:
                attributes = self.attributes[0]
            self.process_dataframe(data_frame, attributes['attributes'], 'main')
            self.insert_values_on_database()
            self.db.commit()

from hdfs import InsecureClient
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np
import time
import json
import io
import os


class DataPersistenceLoader:
    def __init__(self, client):
        self.client = client

    def persist(self, temp_data_dir_hdfs='./data', root_dir=None, compression=None, drop=False):
        """
        Persist data from source directory to target directory in HDFS
        :param temp_data_dir_hdfs: source directory (temporal data directory)
        :param root_dir: root directory of persistence to maintain directory structure
        :param compression: compression type for Parquet files
        :param drop: "boolean" to drop source directory after persistence
        :return: None
        """
        start = time.time()
        if root_dir is None:
            parts = temp_data_dir_hdfs.split('/')
            root_dir = '/'.join(parts[:2])
        if compression == 'None' or compression == 'none' or compression == 'n':
            compression = None
        json_keys, csv_keys = self.get_keys(temp_data_dir_hdfs)
        self.to_parquet_hdfs(temp_data_dir_hdfs, json_keys, compression, root_dir)
        items = self.client.list(temp_data_dir_hdfs)
        for item in items:
            if self.client.status(f'{temp_data_dir_hdfs}/{item}', strict=False)["type"] == 'DIRECTORY':
                self.persist(f'{temp_data_dir_hdfs}/{item}', root_dir=root_dir)
        if drop == 'True' or drop == 'true' or drop == 't':
            self.drop(temp_data_dir_hdfs)
        if drop == 'False' or drop == 'false' or drop == 'f':
            self.client.rename(root_dir, root_dir + "_temporal")
            print(f"{root_dir} renamed to {root_dir}_temporal")
        end = time.time()
        print(f"{temp_data_dir_hdfs} data persistence completed in {end-start} seconds")

    def to_parquet_hdfs(self, hdfs_dir, keys, compression=None, root_dir=None):
        """
        Convert data from CSV or JSON to Parquet and save to HDFS target directory (parent function)
        :param hdfs_dir: source directory
        :param keys: keys to be included in the Parquet file
        :param compression: compression type for Parquet files
        :param root_dir: root directory for data persistence
        :return: None
        """
        hdfs_target_dir = (root_dir.replace(root_dir.split('/')[-1], root_dir.split('/')[-1] + "_persistent")
                           + hdfs_dir[len(root_dir):])
        files = self.client.list(hdfs_dir)
        for file in files:
            if file.split('.')[-1] == 'json':
                if not self.client.status(hdfs_target_dir + "/" + file.split(".")[0] + ".parquet", strict=False):
                    self.json_to_parquet_hdfs(f'{hdfs_dir}/{file}',
                                              f'{hdfs_target_dir}/{file.split(".")[0]}.parquet',
                                              keys,
                                              compression)
                else:
                    print(f"{file.split('.')[0] + '.parquet'} already exists in HDFS: {hdfs_target_dir}")
            elif file.split('.')[-1] == 'csv':
                if not self.client.status(hdfs_target_dir + "/" + file.split(".")[0] + ".parquet", strict=False):
                    self.csv_to_parquet_hdfs(f'{hdfs_dir}/{file}',
                                             f'{hdfs_target_dir}/{file.split(".")[0]}.parquet',
                                             compression)
                else:
                    print(f"{file.split('.')[0] + '.parquet'} already exists in HDFS: {hdfs_target_dir}")

    def json_to_parquet_hdfs(self, json_file_path, hdfs_target_dir, keys, compression=None):
        """
        Convert JSON file to Parquet file and save to HDFS
        :param json_file_path: path to JSON file in HDFS
        :param hdfs_target_dir: target directory to save Parquet file in HDFS
        :param keys: keys to be included in the Parquet file
        :param compression: compression type for Parquet files
        """
        with self.client.read(json_file_path) as reader:
            json_str = reader.read().decode('utf-8')
            df = pd.read_json(io.StringIO(json_str), orient='records')
        if df.empty or df is None:
            print(f"{json_file_path.split('/')[-1]} is empty or could not be loaded from: {json_file_path}")
        else:
            for column in keys:
                if column not in df.columns:
                    df[column] = None
            df = df.replace(np.nan, None)
            table = pa.Table.from_pandas(df)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=compression)
            buffer.seek(0)
            with self.client.write(hdfs_target_dir, overwrite=True) as writer:
                writer.write(buffer.read())
            print(f"{json_file_path.split('/')[-1]} successfully converted to Parquet and saved to: {os.path.split(hdfs_target_dir)[0]}")

    def csv_to_parquet_hdfs(self, csv_file_path, hdfs_target_dir, compression):
        """
        Convert CSV file to Parquet file and save to HDFS
        :param csv_file_path: path to CSV file in HDFS
        :param hdfs_target_dir: target directory to save Parquet file in HDFS
        :param compression: compression type for Parquet files
        """
        with self.client.read(csv_file_path) as reader:
            csv_str = reader.read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_str))
        df = df.replace(np.nan, None)
        if df.empty or df is None:
            print(f"CSV data is empty or could not be loaded from: {csv_file_path}")
        else:
            table = pa.Table.from_pandas(df)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=compression)
            buffer.seek(0)
            with self.client.write(hdfs_target_dir, overwrite=True) as writer:
                writer.write(buffer.read())
            print(f"{csv_file_path.split('/')[-1]} successfully converted to Parquet and saved to: {os.path.split(hdfs_target_dir)[0]}")

    def get_keys(self, hdfs_dir):
        json_keys = set()
        csv_keys = set()
        files = self.client.list(hdfs_dir)
        for file in files:
            if file.split('.')[-1] == 'json':
                with self.client.read(f'{hdfs_dir}/{file}') as json_reader:
                    data = json.load(json_reader)
                    for doc in data:
                        json_keys.update(doc.keys())
            elif file.split('.')[-1] == 'csv':
                with self.client.read(f'{hdfs_dir}/{file}') as csv_reader:
                    data = pd.read_csv(io.StringIO(csv_reader.read().decode('utf-8')))
                    csv_keys.update(data.columns)
        return json_keys, csv_keys

    def drop(self, hdfs_target_dir):
        """
        Delete all files and subfolders in a given directory
        :param hdfs_target_dir: directory to delete
        :return: None
        """
        if hdfs_target_dir == 'nuke' or hdfs_target_dir == '/user/bdm':
            self.client.delete('/user/bdm', recursive=True)
            self.client.makedirs('/user/bdm')
            print(f"Directory /user/bdm deleted")
        else:
            self.client.delete(hdfs_target_dir, recursive=True)
            print(f"Directory {hdfs_target_dir} deleted")


if __name__ == '__main__':
    data_persistor = DataPersistenceLoader(InsecureClient('http://10.4.41.45:9870', 'bdm'))
    data_persistor.persist('./data/idealista')

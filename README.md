# Big Data Management Project 1
This file describes the project structure and the classes used in the project.
The relevant python scripts are ``temporal_landing.py``, ``persistent_landing.py``, ``and landing_zone.py``.
The data is located in the ``/data`` folder, with each source having its own folder.
The additional source selected is from OpenDataBCN, containing demographic data of neighborhoods in Barcelona 
(https://opendata-ajuntament.barcelona.cat/data/dataset/pad_imm_mdbas_sexe_edat-q_nacionalitat-g).

## Landing Zone
The landing zone is the first step in the data pipeline. The landing zone is used to store the raw data that is 
collected from the data sources. The landing zone is implemented using the `LandingZone` class. The `LandingZone` 
class has the following methods:

### LandingZone.run(local_dir, hdfs_target_dir): 
    Run the full data landing zone process
### LandingZone.nuke(hdfs_target_dir):
    Delete all files in a given HDFS directory

Additionally, the LandingZone class initializes the DataCollector and DataPersistenceLoader class which are defined below.

## Data Collection
The data collection process is done using the `DataCollector` class. This class is used to upload data to HDFS. 
The data is uploaded to a given directory in HDFS. The `DataCollector` class has the following methods:

### DataCollector.upload_folder(local_dir, hdfs_target_dir): 
    Upload a data folder to HDFS
### DataCollector.upload_file(local_path, hdfs_path): 
    Upload a file to HDFS

## Data Persistence
The data persistence process is done using the `DataPersistenceLoader` class. This class
is used to persist data in HDFS to Parquet files. The `DataPersistenceLoader` class has the following methods:

### DataPersistenceLoader.persist(compression_type): 
    Persist data in HDFS to Parquet (no compression by default)
    Compression types: 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'
### DataPersistenceLoader.get_keys(hdfs_dir): 
    Get keys from JSON and CSV files in a given directory
### DataPersistenceLoader.to_parquet_hdfs(file_path, hdfs_target_dir, keys): 
    Convert file to Parquet file and save to HDFS
### DataPersistenceLoader.csv_to_parquet_hdfs(csv_file_path, hdfs_target_dir): 
    Convert CSV file to Parquet file and save to HDFS
### DataPersistenceLoader.json_to_parquet_hdfs(json_file_path, hdfs_target_dir, keys): 
    Convert JSON file to Parquet file and save to HDFS

# How to run the project
Run the whole landing zone pipe (for data folder) using cmd with the following command:
```python landing_zone.py url user execute local_dir hdfs_target_dir compression_type drop_temporal_dir```

Run upload_folder command using cmd with the following command:
```python landing_zone.py url user upload local_dir hdfs_target_dir```

Run persist command using cmd with the following command:
```python landing_zone.py url user persist hdfs_target_dir compression_type drop_temporal_dir```

Run drop command using cmd with the following command:
```python landing_zone.py url user drop hdfs_target_dir```

The arguments are:
- url: the URL of the HDFS namenode (http://10.4.41.45:9870)
- user: the user to connect to HDFS (bdm)
- local_dir: the local directory where the data is stored (./data)
- hdfs_target_dir: the HDFS directory where the data will be stored; temporal landing (./data)
- compression_type: the Parquet compression type to use when persisting the data
  - Compression types: 'n' (no compression), 'snappy' (optimal for speed and size), 'gzip' (size optimal), 'brotli', 'lz4', 'zstd'
- drop_temporal_dir: whether to drop the temporal landing directory after persisting the data
  - Options: 't' (true), 'f' (false)

***NOTE: PLEASE RUN DROP COMMAND, "python landing_zone.py http://10.4.41.45:9870 bdm /user/bdm", BEFORE RUNNING TO ENSURE A CLEAN PIPE***
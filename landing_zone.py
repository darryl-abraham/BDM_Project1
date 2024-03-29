import os.path

from temporal_landing import DataCollector
from persistent_landing import DataPersistenceLoader
from hdfs import InsecureClient
import time
import sys

# HDFS client
url = 'http://10.4.41.45:9870'
user = 'bdm'

# Data collector
local_dir = './data'
hdfs_target_dir = './data_temporal'

# Data persistor
temp_data_dir = './data_temporal'  # by default set to ./data_temporal


class LandingZone:

    def __init__(self, url, user):
        self.url = url
        self.user = user
        self.client = InsecureClient(url=self.url, user=self.user)
        self.data_collector = DataCollector(self.client)
        self.data_persistor = DataPersistenceLoader(self.client)

    def run(self, local_dir, temp_data_dir_hdfs, compression=None, drop=False):
        """
        Run full data landing zone process
        :param local_dir: local directory to upload to HDFS
        :param hdfs_target_dir: target directory in HDFS
        :return:
        """
        self.data_collector.upload_folder(local_dir, hdfs_target_dir)
        self.data_persistor.persist(temp_data_dir_hdfs, compression=compression, drop=drop)


if __name__ == '__main__':
    start = time.time()
    url = sys.argv[1]
    user = sys.argv[2]
    landing_zone = LandingZone(url, user)
    if sys.argv[3] == 'drop':
        dir = sys.argv[4]
        landing_zone.data_persistor.drop(dir)
        sys.exit(0)
    elif sys.argv[3] == 'upload':
        local_item = sys.argv[4]
        hdfs_target_dir = sys.argv[5]
        if os.path.isdir(local_item):
            landing_zone.data_collector.upload_folder(local_item, hdfs_target_dir)
        else:
            print(f"Error: {local_item} is not a valid directory")
        sys.exit(0)
    elif sys.argv[3] == 'persist':
        dir = sys.argv[4]
        compression = sys.argv[5]
        drop = sys.argv[6]
        landing_zone.data_persistor.persist(dir, compression, drop)
    elif sys.argv[3] == 'execute':
        local_dir = sys.argv[4]
        hdfs_target_dir = sys.argv[5]
        compression = sys.argv[6]
        drop = sys.argv[7]
        landing_zone.run(local_dir, hdfs_target_dir, compression, drop)
        end = time.time()
        print(f"Landing zone process completed in {end - start} seconds")
        sys.exit(0)


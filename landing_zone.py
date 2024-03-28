from temporal_landing import DataCollector
from persistent_landing import DataPersistenceLoader
from hdfs import InsecureClient

# HDFS client config
namenode = '10.4.41.45'
port = '9870'
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

    def run(self, local_dir, hdfs_target_dir):
        """
        Run full data landing zone process
        :param local_dir: local directory to upload to HDFS
        :param hdfs_target_dir: target directory in HDFS
        :return:
        """
        self.data_collector.upload_folder(local_dir, hdfs_target_dir)
        self.data_persistor.persist()

    def nuke(self, hdfs_target_dir='/user/bdm'):
        """
        Delete all files and subfolders in a given directory
        :param hdfs_target_dir: directory to delete
        :return: None
        """
        self.client.delete(hdfs_target_dir, recursive=True)
        if hdfs_target_dir == '/user/bdm':
            self.client.makedirs('/user/bdm')
        print(f"Directory {hdfs_target_dir} nuked")


if __name__ == '__main__':
    landing_zone = LandingZone('http://10.4.41.45:9870', 'bdm')
    # landing_zone.nuke()  # uncomment to delete all files and run entire landing zone again
    landing_zone.run(local_dir, hdfs_target_dir)
    # landing_zone.nuke(temp_data_dir)  # uncomment to delete all temporal data


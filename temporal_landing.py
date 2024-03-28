from hdfs import InsecureClient
import time
import os


class DataCollector:
    """
    Data collector class to collect data from any source directory and store in HDFS
    """
    def __init__(self, client):
        self.client = client

    def upload_file(self, local_path, hdfs_path):
        """
        Upload a file to HDFS (irrespective of file extension)
        :param local_path: path to local file
        :param hdfs_path: target directory to upload to in HDFS
        :return: None
        """
        filename = local_path.split('/')[-1]
        filename = filename.split('\\')[-1]
        if not self.client.status(hdfs_path + '/' + filename, strict=False):
            self.client.upload(hdfs_path, local_path, overwrite=True)
            print(f"Uploaded file: {local_path.split('/')[-1]} to HDFS: {hdfs_path}")
        else:
            print(f"File: {local_path.split('/')[-1]} already exists in HDFS: {hdfs_path}")

    def upload_folder(self, local_dir, hdfs_target_dir):
        """
        Upload a data folder to HDFS
        :param local_dir: path to folder (with data) to be uploaded
        :param hdfs_target_dir: target directory to upload to in HDFS
        :return: None
        """
        start = time.time()
        for item in os.listdir(local_dir):
            if os.path.isdir(os.path.join(local_dir, item)):
                if not self.client.status(hdfs_target_dir + "/" + item, strict=False):
                    self.client.makedirs(hdfs_target_dir + "/" + item)
                    print(f"Created folder: {item} in HDFS: {hdfs_target_dir}")
                else:
                    print(f"Folder: {item} already exists in HDFS: {hdfs_target_dir}")
                self.upload_folder(os.path.join(local_dir, item), hdfs_target_dir + "/" + item)
            elif os.path.isfile(os.path.join(local_dir, item)):
                self.upload_file(os.path.join(local_dir, item), hdfs_target_dir)
        local_dir = local_dir.replace('/', '\\')
        end = time.time()
        print(f"{local_dir} folder upload completed in {end - start} seconds")


if __name__ == '__main__':
    data_collector = DataCollector(InsecureClient('http://10.4.41.45:9870', 'bdm'))
    data_collector.upload_folder('./data', './data_temporal')

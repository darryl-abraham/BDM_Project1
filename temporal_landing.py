from hdfs import InsecureClient
import os


class DataCollector:
    """
    Data collector class to collect data from any source directory and store in HDFS
    """
    def __init__(self, namenode, port, user):
        self.namenode = namenode
        self.port = port
        self.user = user
        self.client = InsecureClient(url='http://'+self.namenode+':'+self.port, user=self.user)

    def upload_file(self, local_path, hdfs_path):
        """
        Upload a file to HDFS (irrespective of file extension)
        :param local_path: path to local file
        :param hdfs_path: target directory to upload to in HDFS
        :return: None
        """
        if not self.client.status(hdfs_path + '/' + local_path.split('\\')[-1], strict=False):
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
    data_collector = DataCollector(namenode='10.4.41.45', port='9870', user='bdm')
    data_collector.upload_folder('./data', r'/user/bdm/data_temporal')
    #data_collector.nuke()

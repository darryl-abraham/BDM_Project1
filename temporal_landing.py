from hdfs import InsecureClient
import os

class HadoopManager:
    def __init__(self, namenode, port, user):
        self.namenode = namenode
        self.port = port
        self.user = user
        self.client = InsecureClient(url='http://'+self.namenode+':'+self.port, user=self.user)

    def upload_file(self, local_path, hdfs_target_dir):
        self.client.upload(hdfs_target_dir, local_path, overwrite=True)
        print("Uploaded file: " + local_path.split('/')[-1] + " to HDFS: " + hdfs_target_dir)

    def upload_folder(self, local_dir, hdfs_target_dir):
        for item in os.listdir(local_dir):
            if os.path.isdir(os.path.join(local_dir, item)):
                self.client.makedirs(hdfs_target_dir + "/" + item)
                print("Created folder: " + item + " in HDFS: " + hdfs_target_dir)
                self.upload_folder(os.path.join(local_dir, item), hdfs_target_dir + "/" + item)
            elif os.path.isfile(os.path.join(local_dir, item)):
                self.upload_file(os.path.join(local_dir, item), hdfs_target_dir)

    def nuke(self, hdfs_target_dir='/user'):
        self.client.delete(hdfs_target_dir, recursive=True)
        if hdfs_target_dir == '/user':
            self.client.makedirs('/user')
            self.client.makedirs('/user/bdm')

if __name__ == '__main__':
    hadoop_manager = HadoopManager(namenode='10.4.41.45', port='9870', user='bdm')
    #hadoop_manager.upload_folder(r'.\data', r'/user/bdm')
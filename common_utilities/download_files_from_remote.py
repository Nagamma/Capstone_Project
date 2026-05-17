import paramiko
from io import StringIO
import boto3

from project_config.etlconfig import *


def download_file_from_linux(self,logger):
    logger.info("product file download from linux server has started...")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(LINUX_HOST,username=LINUX_USER,password=LINUX_PASSWORD)
    sftp= ssh_client.open_sftp()
    sftp.get(LINUX_REMOTE_PRODUCT_DATA_FILE_PATH,LOCAL_PRODUCT_DATA_FILE_PATH)
    sftp.close()
    logger.info("product file download from linux server has finished...")



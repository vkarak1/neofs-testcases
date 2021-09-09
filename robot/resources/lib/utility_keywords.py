#!/usr/bin/python3.8

import docker
import os
import tarfile
import uuid

from neo3 import wallet
from robot.api.deco import keyword
from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn

from common import *

ROBOT_AUTO_KEYWORDS = False

@keyword('Generate file of bytes')
def generate_file_of_bytes(size: str) -> str:
    """
    Function generates big binary file with the specified size in bytes.
    :param size:        the size in bytes, can be declared as 6e+6 for example
    """
    size = int(float(size))
    filename = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size))
    logger.info(f"file with size {size} bytes has been generated: {filename}")
    return filename

@keyword('Get Docker Logs')
def get_container_logs(testcase_name: str) -> None:
    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    logs_dir = BuiltIn().get_variable_value("${OUTPUT_DIR}")
    tar_name = f"{logs_dir}/dockerlogs({testcase_name}).tar.gz"
    tar = tarfile.open(tar_name, "w:gz")
    for container in client.containers():
        container_name = container['Names'][0][1:]
        if client.inspect_container(container_name)['Config']['Domainname'] == "neofs.devenv":
            file_name = f"{logs_dir}/docker_log_{container_name}"
            with open(file_name,'wb') as out:
                out.write(client.logs(container_name))
            logger.info(f"Collected logs from container {container_name}")
            tar.add(file_name)
            os.remove(file_name)
    tar.close()

@keyword('WIF to Binary')
def wif_to_binary(wif: str) -> str:
    priv_key = wallet.Account.private_key_from_wif(wif)
    path = f"{os.getcwd()}/{ASSETS_DIR}/{str(uuid.uuid4())}"
    with open(path, "wb") as f:
        f.write(priv_key)
    return path

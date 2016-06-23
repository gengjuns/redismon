#! /bin/env python
# -*- coding:utf8 -*-

import sys
import time
import commands

import logging
from  logging.config import logging


class RedisClusterInfo(object):
    """Fetches the Redis Cluster metrice, "cluster info".
  
	Attributes:
                addr: Redis server hostname,as well as the Endpoint.
		port: Redis tcp port number.
                password: Redis require password, if not empty string.
 		logger: logging
          """

    def __init__(self, addr, port, password):
        self.addr = addr
        self.port = port
        self.password = password
        self.tags = "redis=" + str(port)

        logging.config.fileConfig("../conf/logging.ini")
        self.logger = logging.getLogger(__name__)

    @property
    def collect_cluster_info(self):
        """Collect cluster info metrics.
			"The redis-cli must be in the command PATH!!!"

		Returns:
			cluster_info_dict: redis cluster metrics dict.
		"""
        cluster_command = "redis-cli -h " + self.addr + " -p " + str(
            self.port) + " --password " + self.password + "cluster info"
        if (self.password == ""):  # If password is empty, replcate the password arg
            cluster_command = cluster_command.replace("--password", " ")
        cluster_info = commands.getoutput(cluster_command)
        # self.logger.info(cluster_info)
        cluster_info_list = cluster_info.replace("\r\n", " ").replace("\r", "").split(" ")

        cluster_info_dict_all = {}
        cluster_info_dict = {}
        for cluster_info_time in cluster_info_list:
            item_list = cluster_info_time.split(":")
            # self.logger.info(len(item_list))
            if (len(item_list) == 2):
                cluster_info_dict_all[item_list[0]] = item_list[1]
        # clear the cluster info
        if cluster_info_dict_all.has_key("cluster_state"):

            if (cluster_info_dict_all["cluster_state"] == "ok"):
                cluster_info_dict["cluster_state"] = 1
        else:
            cluster_info_dict["cluster_state"] = 0
        cluster_info_dict["cluster_slots_assigned"] = cluster_info_dict_all["cluster_slots_assigned"]
        cluster_info_dict["cluster_slots_ok"] = cluster_info_dict_all["cluster_slots_ok"]
        cluster_info_dict["cluster_slots_pfail"] = cluster_info_dict_all["cluster_slots_pfail"]
        cluster_info_dict["cluster_slots_fail"] = cluster_info_dict_all["cluster_slots_fail"]
        cluster_info_dict["cluster_known_nodes"] = cluster_info_dict_all["cluster_known_nodes"]
        cluster_info_dict["cluster_size"] = cluster_info_dict_all["cluster_size"]
        cluster_info_dict["cluster_nodes_status"] = 1

        # cluster nodes info
        cluster_command_node = "redis-cli -h " + self.addr + " -p " + str(
            self.port) + " --password " + self.password + "cluster nodes"
        if (self.password == ""):  # If password is empty, replcate the password arg
            cluster_command_node = cluster_command_node.replace("--password", " ")
        cluster_node = commands.getoutput(cluster_command_node)
        self.logger.info(cluster_node)
        cluster_node_list = cluster_node.split("\n")

        for cluster_node_time in cluster_node_list:
            item_list = cluster_node_time.split(" ")
            self.logger.info(item_list)
            self.logger.info(len(item_list))
            self.logger.info(item_list[2].find("fail"))
            self.logger.info(item_list[2].find("pfail"))

            if (len(item_list) >= 3) and (item_list[2].find("fail") != -1 or item_list[2].find("pfail") != -1):
                self.logger.info("ininin")
                cluster_info_dict["cluster_nodes_status"] = 0
                break

        return cluster_info_dict

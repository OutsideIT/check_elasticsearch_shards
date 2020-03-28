#!/usr/bin/env python
# Script name:  check_elasticsearch_shards.py
# Version:      v2.04.200327
# Created on:   24/02/2017
# Author:       Denny Zhang, Willem D'Haese
# Purpose:      Nagios plugin to check how many days since last update
# On GitHub:    https://github.com/willemdh/
# Copyright:
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details. You should have received a copy of the
# GNU General Public License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

import argparse
import logging
import re
import requests
import socket
import sys
from elasticsearch import Elasticsearch
from ssl import create_default_context

logger = logging.getLogger('check_elasticsearch_shards')
#logging.basicConfig(level=logging.DEBUG)

NAGIOS_OK=0
NAGIOS_WARNING=1
NAGIOS_CRITICAL=2
NAGIOS_UNKNOWN=3

def get_gb_size_from_string(string):
    # 10.5gb ->   10.5
    # 1.2tb  -> 1200
    val = 0
    if string.endswith("gb"):
        val = float(string.replace("gb", ""))
    elif string.endswith("tb"):
        val = float(string.replace("tb", "")) * 1000
    elif string.endswith("mb"):
        val = float(string.replace("mb", "")) * 0.001
    elif string.endswith("kb"):
        val = float(string.replace("kb", "")) * 0.001 * 0.001
    elif string.endswith("b"):
        val = float(string.replace("b", "")) * 0.001 * 0.001 * 0.001
    else:
        print("ERROR: unexpected. size string: %s" % (string))
        sys.exit(NAGIOS_CRITICAL)
    return val

def parse_index_info(es_index_info):
    index_list = []
    for line in es_index_info.split("\n"):
        if line == '' or " index " in line  or " close " in line:
            continue
        else:
            line = " ".join(line.split())
            l = line.split()
            index = l[2]
            pri = l[3]
            pri_store_size = l[7]
            index_list.append([index, pri, pri_store_size])
    return index_list

def confirm_es_shard_count(es_host, es_port, es_index_list, min_shard_count):
    failed_index_list = []
    for l in es_index_list:
        index_name = l[0]
        number_of_shards = int(l[1])
        if number_of_shards < min_shard_count:
            failed_index_list.append(index_name)
    return failed_index_list

def confirm_es_shard_size(es_host, es_port, es_index_list, max_shard_size):
    failed_index_list = []
    for i in es_index_list:
        index_name = i[0]
        number_of_shards = int(i[1])
        pri_store_size = i[2]
        avg_shard_size_gb = get_gb_size_from_string(pri_store_size)/number_of_shards
        if avg_shard_size_gb > max_shard_size:
            failed_index_list.append(index_name)
    return failed_index_list

def parse_args():
    parser = argparse.ArgumentParser(description="Check Elasticsearch Shards")
    group = parser.add_mutually_exclusive_group()
    parser.add_argument('--es_host', required=True, help="ES ip or hostname", type=str)
    parser.add_argument('--es_port', default='9200', required=False, help="ES port", type=str)
    parser.add_argument('--es_user', default='monitoring_user', required=True, help="ES user", type=str)
    parser.add_argument('--es_pass', default='', required=True, help="ES password for user", type=str)
    parser.add_argument('--es_index', required=False, default='', help="ES index name", type=str)
    parser.add_argument('--ca_file', required=False, default='', help="CA certificate file", type=str)
    parser.add_argument('--action', required=False, default='check_shard_size', help="Check to execute", type=str, choices=['check_shard_size', 'check_shard_count'])
    group.add_argument('--min_shard_count', default='3', required=False, help='minimal shards', type=str, action='store')
    group.add_argument('--max_shard_size', default='50gb', required=False, help='maximum shards size', type=str, action='store')
    return parser.parse_args()

if __name__ == '__main__': 
    l = parse_args()
    es_host = l.es_host
    es_port = l.es_port
    es_user = l.es_user
    es_pass = l.es_pass
    es_index = l.es_index
    ca_file = l.ca_file
    action = l.action
    min_shard_count = int(l.min_shard_count)
    max_shard_size = get_gb_size_from_string(l.max_shard_size)
    context = create_default_context(cafile=str(ca_file))
    es = Elasticsearch(
        [str(es_host)],
        http_auth = (str(es_user), str(es_pass)),
        scheme = "https",
        port = int(es_port),
        ssl_context = context
    )
    es_index_cat_raw = es.cat.indices(es_index, h=("h","s","i","p","r","dc","ss","pri.store.size","creation.date.string"), s="i")
    es.transport.close()
    es_index_cat_list = parse_index_info(es_index_cat_raw)

    if action == "check_shard_size":
        failed_index_list = confirm_es_shard_size(es_host, es_port, es_index_cat_list, max_shard_size)
        if len(failed_index_list) != 0:
            if len(failed_index_list) == 1:
                print("CRITICAL: Index %s has shards bigger than %s GB!" % (failed_index_list[0], max_shard_size))
            elif len(failed_index_list) > 1:  
                print("CRITICAL: %d indices have shards bigger than %s GB!\n%s" % (len(failed_index_list), max_shard_size, ", ".join(failed_index_list)))
            sys.exit(NAGIOS_CRITICAL)
        else:
            print("OK: All shards for indices with pattern \"%s\" are less then %s GB." % (es_index, max_shard_size))   
            sys.exit(NAGIOS_OK)
    elif action == "check_shard_count":
        failed_index_list = confirm_es_shard_count(es_host, es_port, es_index_cat_list, min_shard_count)
        if len(failed_index_list) != 0:
            if len(failed_index_list) == 1:
                print("CRITICAL: Index %s has only 1 primary shard which is less then minimum shard count %d!" % (failed_index_list[0], min_shard_count))
            elif len(failed_index_list) > 1:  
                print("CRITICAL: %d indices have less primary shards then minimum shard count %d!\n%s" % (len(failed_index_list), min_shard_count, ", ".join(failed_index_list)))
            sys.exit(NAGIOS_CRITICAL)
        else:
            print("OK: All shards or indices with pattern \"%s\" have at least %d shard count." % (es_index, min_shard_count))   
            sys.exit(NAGIOS_OK)
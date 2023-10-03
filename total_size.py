import requests
import json
#import csv
#from tabulate import tabulate
from itertools import groupby
from operator import itemgetter
#import numpy as np
#import matplotlib.pyplot as plt

url = 'https://joystream.yyagi.cloud/graphql'

def queryGrapql(query, url= 'https://joystream.yyagi.cloud/graphql' ):
  headers = {'Accept-Encoding': 'gzip, deflate, br', 'Content-Type': 'application/json',
           'Accept': 'application/json',  'Connection': 'keep-alive', 'DNT': '1',
                   'Origin': 'https://query.joystream.org' }
  response = requests.post(url, headers=headers, json=query)
  return response.json()['data']



def get_objects(start_time='',end_time='',obj_count=33000, limit=33000):
  import math
  loop= math.ceil(obj_count/limit)
  offset=0
  obj_data=[]
  for i in range(loop):
    if start_time and end_time :
      query_created = {"query":'query MyQuery {{ storageDataObjects(limit: {}, offset: {} orderBy: createdAt_ASC,where: {{createdAt_gt: "{}" , createdAt_lt: "{}"}}) {{ createdAt size id storageBagId }} }}'.format(limit, offset, start_time, end_time) }
    else :
      query_created = {"query":'query MyQuery {{ storageDataObjects(limit: {}, offset: {} orderBy: createdAt_ASC) {{ createdAt deletedAt size id storageBagId }} }}'.format(limit, offset) }
    objects_created  = queryGrapql(query_created)['storageDataObjects']
    obj_data += objects_created
    offset += limit
  return obj_data

def get_objects_count(start_time='', end_time=''):
  if start_time and end_time :
    query = {"query": 'query MyQuery {{ storageDataObjectsConnection ( where: {{createdAt_gt: "{}" , createdAt_lt: "{}"}}) {{  totalCount  }} }}'.format(start_time, end_time) }
  else:
    query = {"query": 'query MyQuery { storageDataObjectsConnection {  totalCount }} ' }
  data = queryGrapql(query)['storageDataObjectsConnection']["totalCount"]
  return data


def get_0bjects_ranges(data_created,total_size):
  for record in data_created:
    size  = int(record['size'])
    total_size += size
  return  total_size


def objects_stats(data_created):
  num_objects_created = len(data_created)
  total_size = 0
  total_size =get_0bjects_ranges(data_created,total_size)
  return num_objects_created, total_size


if __name__ == '__main__':
  objects_num_qn_ = get_objects_count()
  obj_data_=get_objects('','',objects_num_qn_)
  objects_num, total_size = objects_stats(obj_data_)
  print('Total Objects: {}\n'.format(objects_num))
  print('Total Objects Size: {}\n'.format(total_size))

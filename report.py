import requests
import json
#import csv
from tabulate import tabulate
from itertools import groupby
from operator import itemgetter

url = 'https://query.joystream.org/graphql'
file_name = "{}-12:00-objects.txt"
file_server = "http://87.236.146.74:8000/"
operators = [{'id':"0x2bc", 'bucket': 0},{'id':"alexznet", 'bucket': 2},{'id':"Craci_BwareLabs", 'bucket': 10},{'id':"GodsHunter", 'bucket': 6},{'id':"joystreamstats", 'bucket': 1},{'id':"l1dev", 'bucket': 4},{'id':"maxlevush", 'bucket': 3},{'id':"mmx1916", 'bucket': 9},{'id':"razumv", 'bucket': 11},{'id':"yyagi", 'bucket': 8}, {'id':"sieemma", 'bucket': 12} ]
credential = {'username': '', 'password' :'joystream'}


def queryGrapql(query, url= 'https://query.joystream.org/graphql' ):
  headers = {'Accept-Encoding': 'gzip, deflate, br', 'Content-Type': 'application/json',
           'Accept': 'application/json',  'Connection': 'keep-alive', 'DNT': '1', 
		   'Origin': 'https://query.joystream.org' }
  response = requests.post(url, headers=headers, json=query)
  return response.json()['data']

def get_councils_period(url):
  query = {"query":'query MyQuery{ electedCouncils { electedAtBlock endedAtBlock endedAtTime electedAtTime } }'}
  data  = queryGrapql(query, url)['electedCouncils']
  #data = sorted(data, key = itemgetter('endedAtBlock'), reverse=True)
  if data[-1]['endedAtTime'] == None:
    data.pop(-1)
  data = sorted(data, key = itemgetter('endedAtBlock'))
  period = len(data)
  return data[-1], data[-2], period

def get_backets(url, start_time = '', end_time = '', createdat = False, deletedat = False):
  if start_time and end_time :
    if createdat :
      query = {"query":'query MyQuery {{  storageBuckets ( where: {{createdAt_gt: "{}" , createdAt_lt: "{}"}}){{    id    dataObjectsSize    dataObjectsSizeLimit    dataObjectsCount    bags {{      id  createdAt  }}  }}}}'.format(start_time, end_time)}
    elif deletedat:
      query = {"query":'query MyQuery {{  storageBuckets ( where: {{deletedAt_gt: "{}" , deletedAt_lt: "{}"}}){{    id    dataObjectsSize    dataObjectsSizeLimit    dataObjectsCount    bags {{      id  createdAt  }}  }}}}'.format(start_time, end_time)}
  else:
    query = {"query":"query MyQuery {  storageBuckets {    id    dataObjectsSize    dataObjectsSizeLimit    dataObjectsCount    bags {      id  createdAt  }  }}"}
  data  = queryGrapql(query, url)['storageBuckets']
  for record in data:
    record['bags'] = len(record['bags'])
    record['Utilization'] = int(record['dataObjectsSize'])/int(record['dataObjectsSizeLimit'])
    record['dataObjectsSize, GB'] = int(record['dataObjectsSize']) / 1074790400
  #keys = list(data[0].keys())
  #file_name= 'backets_info_'+ time.strftime("%Y%m%d%H%M%S")+'.csv'
  # with open(file_name, 'w') as csvfile:
  #  writer = csv.DictWriter(csvfile, fieldnames = keys)
  #  writer.writeheader()
  #  writer.writerows(data)
  #return file_name
  return data
 
def get_bags(start_time= "2022-06-18T00:00:00.000Z", end_time= "2022-06-24T00:00:00.000Z"):
  query_created = {"query": 'query MyQuery {{ storageBags( where: {{createdAt_gt: "{}" , createdAt_lt: "{}"}}) {{  id }} }}'.format(start_time, end_time) }
  query_deleted = {"query": 'query MyQuery {{ storageBags( where: {{deletedAt_gt: "{}" , deletedAt_lt: "{}"}}) {{  id }} }}'.format(start_time, end_time) }
  data_created  = queryGrapql(query_created)['storageBags']
  data_deleted  = queryGrapql(query_deleted)['storageBags']
  num_bags_created = len(data_created)
  num_bags_deleted = len(data_deleted)
  return {"bag created": num_bags_created, "bags deleted": num_bags_deleted}
  
def get_objects(start_time='',end_time=''):
  if start_time and end_time :
    query_created = {"query":'query MyQuery {{ storageDataObjects(limit: 33000, offset: 0,where: {{createdAt_gt: "{}" , createdAt_lt: "{}"}}) {{ size id storageBagId }} }}'.format(start_time, end_time) }
  else :
    query_created = {"query":'query MyQuery { storageDataObjects(limit: 33000, offset: 0) { size id storageBagId } }' }
  objects_created  = queryGrapql(query_created)['storageDataObjects']
  for obj in objects_created: 
    obj['storageBagId'] = obj['storageBagId'].split(":")[2]
  return objects_created
  
def get_objects_files(file_server, operators, end_date, credential):
  result= []
  file = end_date+"-12:00-objects.txt" 
  for operator in operators:
    url = file_server+operator['id']+"/"+file 
    response = requests.get(url, auth=(credential['username'], credential['password']))
    if response.status_code == 200 and not response.text.startswith('<!DOCTYPE html>'):
      result.append({'operator':operator['id'], 'file': file, 'response': response.content}) 
  return result 

def load_objects(lines):
  objects_file = []	
  for line in lines:
    if line.startswith('d') or line.startswith('total') or not line.strip():
      continue
    line_split = line.split(",")
    objects_file.append({'size': line_split[4], 'id': line_split[8].strip('\n')})
  return objects_file
    
def load_objects_from_server(data):
  objects_file = []	
  for operator in data:
    opertor_response = operator['response'].decode("utf-8") 
    lines = opertor_response.split('\r\n')
    objects_file.append({'operator': operator['operator'],'objects':load_objects(lines)})
  return objects_file
  
def load_objects_from_file(file_name):
  objects_file = []	
  with open(file_name) as f:
    lines = f.readlines()
  objects_file = objects_file = load_objects(lines)
  return objects_file
  
def compare_objects(file_objects, objects):
    lost = []
    for obj in objects:
      found = False
      for file_obj in file_objects:
        if obj['id'] == file_obj['id']:
          found = True
          break
      if not found:
        lost.append(obj)
    return lost

def objects_stats(start_time='',end_time=''):
  data_created = get_objects(start_time,end_time)
  num_objects_created = len(data_created)
  total_size = 0
  sizes = {'<10': 0,'<100': 0,'<1000': 0,'<10000': 0,'<100000': 0,'<1000000': 0}
  sizes_range = {'0-10': 0,'10-100': 0,'100-1000': 0,'1000-10000': 0,'10000-100000': 0,'100000-10000000': 0}
  total_size,sizes,sizes_range =get_0bjects_ranges(data_created,total_size,sizes,sizes_range)
  bags_stats = bag_stats(data_created)
  return total_size,sizes,sizes_range,bags_stats
 
def get_0bjects_ranges(data_created,total_size,sizes,sizes_range): 
  for record in data_created:
    size  = int(record['size'])
    total_size += size
    size = size / 1048576
    if size < 10:
      sizes['<10'] += 1
      sizes['<100'] += 1
      sizes['<1000'] += 1
      sizes['<10000'] += 1
      sizes['<100000'] += 1
      sizes['<1000000'] += 1
    elif size < 100:
      sizes['<100'] += 1
      sizes['<1000'] += 1
      sizes['<10000'] += 1
      sizes['<100000'] += 1
      sizes['<1000000'] += 1
    elif size < 1000:
      sizes['<1000'] += 1
      sizes['<10000'] += 1
      sizes['<100000'] += 1
      sizes['<1000000'] += 1
    elif size < 10000:
      sizes['<10000'] += 1
      sizes['<100000'] += 1
      sizes['<1000000'] += 1
    elif size < 100000:
      sizes['<100000'] += 1
      sizes['<1000000'] += 1
    else:
      sizes['<1000000'] += 1
   
    if size < 10:
      sizes_range['0-10'] += 1
    elif size < 100:
      sizes_range['10-100'] += 1
    elif size < 1000:
      sizes_range['100-1000'] += 1
    elif size < 10000:
      sizes_range['1000-10000'] += 1
    elif size < 100000:
      sizes_range['10000-100000'] += 1
    else:
      sizes_range['100000-10000000'] += 1
  return  total_size, sizes, sizes_range


def sort_bags(data):
  bags = {}
  sorted_data = sorted(data, key = itemgetter('storageBagId'))
  for key, value in groupby(sorted_data, key = itemgetter('storageBagId')):
    #key = key.split(":")[2]
    bags[key]= list(value)
  return(bags)
 
def bag_stats(data_created): 
  bags = sort_bags(data_created)
  #print(bags)
  result= []
  for key, value in bags.items():
    bag = {}
    bag['id'] = key
    total_size = 0
    bag['objects_num'] = len(value)
    for obj in value:
      total_size += int(obj['size'])
    bag['total_size'] = total_size
    bag['average_size'] = total_size / bag['objects_num']
    result.append(bag)
  return result

def print_table(data, master_key = '', sort_key = ''):
    if sort_key:
        data = sorted(data, key = itemgetter(sort_key), reverse=True)
    headers = [*data[0]]
    if master_key:
        headers.append(master_key)
        headers.remove(master_key)
        headers = [master_key] + headers
    table = []
    for line in data:
        row = []
        if master_key:
            value = line.pop(master_key)
            row.append(value)
        for key in [*line]:
            row.append(line[key])
        table.append(row)
    try:
        print(tabulate(table, headers, tablefmt="fancy_grid"))
    except UnicodeEncodeError:
        print(tabulate(table, headers, tablefmt="grid"))       

if __name__ == '__main__':
  last_council,previous_council,period = get_councils_period(url)
  #start_date = "2022-06-18"
  #end_date   = "2022-06-24"
  start_time = last_council['electedAtTime']
  end_time   = last_council['endedAtTime']
  start_date = start_time.split('T')[0]
  end_date = end_time.split('T')[0]
  #start_time= "{}T00:00:00.000Z".format(start_date)
  #end_time  = "{}T00:00:00.000Z".format(end_date)
  print('Full report for the Term: {}'.format(period))
  print('Start date: {}'.format(start_date))
  print('End date: {}'.format(end_date))
  #print('Start Time: {}'.format(start_time))
  #print('End Time: {}'.format(end_time))
  print('Start Block: {}'.format(last_council['electedAtBlock']))
  print('End Block: {}'.format(last_council['endedAtBlock']))
  print('---------------------------BUCKETS--------------------------------------')
  buckets = get_backets(url)
  print_table(buckets)
  print('---------------------------BUCKETS CREATED--------------------------------------')
  buckets_created = get_backets(url,start_time,end_time,createdat = True)
  number_buckets_created = len(buckets_created)
  print('Bucket Created: {}'.format(number_buckets_created))
  if number_buckets_created > 0:
    print_table(buckets_created)
  print('---------------------------BUCKETS DELETED--------------------------------------')
  buckets_deleted = get_backets(url,start_time,end_time,deletedat = True)
  number_buckets_deleted = len(buckets_deleted)
  print('Bucket Deleted: {}'.format(number_buckets_deleted))
  if number_buckets_deleted > 0:
    print_table(buckets_deleted)
  print('---------------------------BAGS--------------------------------------')
  bags = get_bags(start_time, end_time)
  print('Bags Created: {}'.format(bags['bag created']))
  print('Bags Deleted: {}'.format(bags['bags deleted']))
  print('---------------------------OBJECTS WITHIN THE WINDOW--------------------------------------')
  #print(get_objects(start_time,end_time))
  total_size,sizes,sizes_range,bags_stats = objects_stats(start_time,end_time)
  print('Total Objects Size: {}'.format(total_size))
  print('Objects Size Distribution')
  print_table([sizes])
  print_table([sizes_range])
  print('Objects Size Distribution Per Bag')
  print_table(bags_stats)
  print('---------------------------OBJECTS TOTAL--------------------------------------')
  #print(get_objects(start_time,end_time))
  total_size,sizes,sizes_range,bags_stats = objects_stats()
  print('Total Objects Size: {}'.format(total_size))
  print('Objects Size Distribution')
  print_table([sizes])
  print_table([sizes_range])
  print('Objects Size Distribution Per Bag')
  print_table(bags_stats, sort_key = 'total_size')
  print('---------------------------LOST OBJECTS--------------------------------------')
  master_objects = get_objects(start_time,end_time)
  data = get_objects_files(file_server, operators, end_date, credential)
  operators = load_objects_from_server(data)
  operators_objects = []
  for operator in operators:
    operators_objects = operators_objects + operator['objects']
  lost = compare_objects(operators_objects, master_objects)
  total_objects = len(master_objects)
  lost_object = len(lost)
  print('Total Objects: {}'.format(total_objects))
  print('Total Lost Objects: {}'.format(lost_object))
  print('Percentage Lost Objects: %{}'.format(100*lost_object/total_objects))
  print_table(lost, master_key = 'id')

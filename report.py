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
  return data[-1], data[-2], data[0], period

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

def get_rewards(start_time, end_time):
  query = '{{ rewardPaidEvents(limit: 33000, offset: 0, where: {{group: {{id_eq: "storageWorkingGroup"}}, createdAt_gt: "{}", createdAt_lt: "{}"}}) {{ paymentType amount workerId }} }}'.format(start_time, end_time)
  query_dict = {"query": query}
  data = queryGrapql(query_dict,url)['rewardPaidEvents']
  total = 0
  result = []
  sorted_data = sorted(data, key = itemgetter('workerId'))
  for key, values in groupby(sorted_data, key = itemgetter('workerId')):
    worker_total = 0
    for value in list(values):
      worker_total += int(value["amount"])
    result.append({'workerId':key, 'worker_total':worker_total})
    total += worker_total
  return total,result

def get_new_hire(start_time, end_time):
  query = '{{ openingFilledEvents(where: {{group: {{id_eq: "storageWorkingGroup"}}, createdAt_gt: "{}", createdAt_lt: "{}"}}) {{ workersHired {{ id membershipId}}}}}}'.format(start_time, end_time)
  query_dict = {"query": query}
  data = queryGrapql(query_dict,url)['openingFilledEvents']
  result = []
  if len(data) == 0:
    return result
  for record in data:
    result.append(record['workersHired'][0])
  return result

def get_bags(start_time, end_time):
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
  sizes = {'<10 MB': 0,'<100 MB': 0,'<1000 MB': 0,'<10000 MB': 0,'<100000 MB': 0,'<1000000 MB': 0}
  sizes_range = {'0-10 MB': 0,'10-100 MB': 0,'100-1000 MB': 0,'1000-10000 MB': 0,'10000-100000 MB': 0,'100000-10000000 MB': 0}
  total_size,sizes,sizes_range =get_0bjects_ranges(data_created,total_size,sizes,sizes_range)
  bags_stats = bag_stats(data_created)
  return num_objects_created, total_size,sizes,sizes_range,bags_stats
 
def get_0bjects_ranges(data_created,total_size,sizes,sizes_range): 
  for record in data_created:
    size  = int(record['size'])
    total_size += size
    size = size / 1048576
    if size < 10:
      sizes['<10 MB'] += 1
      sizes['<100 MB'] += 1
      sizes['<1000 MB'] += 1
      sizes['<10000 MB'] += 1
      sizes['<100000 MB'] += 1
      sizes['<1000000 MB'] += 1
    elif size < 100:
      sizes['<100 MB'] += 1
      sizes['<1000 MB'] += 1
      sizes['<10000 MB'] += 1
      sizes['<100000 MB'] += 1
      sizes['<1000000 MB'] += 1
    elif size < 1000:
      sizes['<1000 MB'] += 1
      sizes['<10000 MB'] += 1
      sizes['<100000 MB'] += 1
      sizes['<1000000 MB'] += 1
    elif size < 10000:
      sizes['<10000 MB'] += 1
      sizes['<100000 MB'] += 1
      sizes['<1000000 MB'] += 1
    elif size < 100000:
      sizes['<100000 MB'] += 1
      sizes['<1000000 MB'] += 1
    else:
      sizes['<1000000 MB'] += 1
   
    if size < 10:
      sizes_range['0-10 MB'] += 1
    elif size < 100:
      sizes_range['10-100 MB'] += 1
    elif size < 1000:
      sizes_range['100-1000 MB'] += 1
    elif size < 10000:
      sizes_range['1000-10000 MB'] += 1
    elif size < 100000:
      sizes_range['10000-100000 MB'] += 1
    else:
      sizes_range['100000-10000000 MB'] += 1
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
    bag['total_size bytes'] = total_size
    bag['average_size bytes'] = int(total_size / bag['objects_num'])
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
        result = tabulate(table, headers, tablefmt="github")
        print(result)
        return result
    except UnicodeEncodeError:
        result = tabulate(table, headers, tablefmt="grid")
        print(result)
        return result

if __name__ == '__main__':
  last_council,previous_council,first_council, period = get_councils_period(url)
  #start_date = "2022-06-18"
  #end_date   = "2022-06-24"
  report = ''
  first_time = first_council['electedAtTime']
  start_time = last_council['electedAtTime']
  end_time   = last_council['endedAtTime']
  start_date = start_time.split('T')[0]
  end_date = end_time.split('T')[0]
  previous_start_time = previous_council['electedAtTime']
  previous_end_time   = previous_council['endedAtTime']
  file_name = 'report-'+end_time  
  #start_time= "{}T00:00:00.000Z".format(start_date)
  #end_time  = "{}T00:tabulate00:00.000Z".format(end_date)
  print('Full report for the Term: {} \n\n'.format(period))
  print('Start date: {} \n'.format(start_date))
  print('End date: {} \n'.format(end_date))
  report += 'Full report for the Term: {} \n\n'.format(period)
  report += 'Start date: {}  \n\n'.format(start_date)
  report += 'End date: {} \n\n'.format(end_date)
  #print('Start Time: {}\n'.format(start_time))
  #print('End Time: {}\n'.format(end_time))
  print('Start Block: {}\n'.format(last_council['electedAtBlock']))
  print('End Block: {}\n'.format(last_council['endedAtBlock']))
  report += 'Start Block: {} \n\n'.format(last_council['electedAtBlock'])
  report += 'End Block: {} \n\n'.format(last_council['endedAtBlock'])

  print('# Hiring')
  hired_workers = get_new_hire(start_time, end_time)
  print('Number of hired works: {}'.format(len(hired_workers)))
  report += '# Hiring\n'
  report += 'Number of hired works: {}\n'.format(len(hired_workers))
  tble = print_table(hired_workers)
  report += tble+'\n'

  print('# Rewards')
  report += '# Rewards\n'
  total_rewards,rewards =  get_rewards(start_time, end_time)
  print('Total Rewards: {}'.format(total_rewards))
  report += 'Total Rewards: {}\n'.format(total_rewards)
  tble = print_table(rewards)
  report += tble+'\n'
  
  print('# BUCKETS Info  ')
  report += '# BUCKETS Info  \n'
  buckets = get_backets(url)
  buckets_file = 'buckets_'+end_time
  with open(buckets_file, 'w') as file:
    json.dump(buckets, file)
    file.close()
  
  tble = print_table(buckets)
  report += tble+'\n'
  

  

  print('## BUCKETS CREATED')
  report += '## BUCKETS CREATED\n'
  buckets_created = get_backets(url,start_time,end_time,createdat = True)
  number_buckets_created = len(buckets_created)
  print('Bucket Created: {}'.format(number_buckets_created))
  report += 'Bucket Created: {}\n'.format(number_buckets_created)
  if number_buckets_created > 0:
    tble = print_table(buckets_created)
    report += tble+'\n'

  print('## BUCKETS DELETED')
  report += '## BUCKETS DELETED\n'
  buckets_deleted = get_backets(url,start_time,end_time,deletedat = True)
  number_buckets_deleted = len(buckets_deleted)
  print('Bucket Deleted: {}\n'.format(number_buckets_deleted))
  report += 'Bucket Deleted: {}\n'.format(number_buckets_deleted)
  if number_buckets_deleted > 0:
    tble = print_table(buckets_deleted)
    report += tble+'\n'

  print('## Bags')
  report += '## Bags\n'
  bags = get_bags(start_time, end_time)
  print('Bags Created: {} \n'.format(bags['bag created']))
  print('Bags Deleted: {} \n'.format(bags['bags deleted']))
  report += 'Bags Created: {} \n\n'.format(bags['bag created'])
  report += 'Bags Deleted: {} \n\n'.format(bags['bags deleted'])
 
  print('# Objects Info during this Council Period')
  report += '# Objects Info during this Council Period \n'
  #print(get_objects(start_time,end_time))
  objects_num, total_size,sizes,sizes_range,bags_stats = objects_stats(start_time,end_time)
  print('Total Objects Size: {}\n'.format(objects_num))
  report += 'Total Objects Size: {}\n'.format(objects_num)
  print('Total Objects Size: {}\n'.format(total_size))
  report += 'Total Objects Size: {}\n'.format(total_size)
  print('## Objects Size Distribution')
  report += '## Objects Size Distribution\n'
  tble = print_table([sizes])
  report += tble+'\n \r\n'
  print('\n')
  tble = print_table([sizes_range])
  report += tble+'\n'

  print('## Objects Size Distribution Per Bag')
  tble = print_table(bags_stats)
  report += '## Objects Size Distribution Per Bag \n'
  report += tble+'\n'

  print('# Total object Info')
  report += '# Total object Info \n'
  #print(get_objects(start_time,end_time))
  objects_num, total_size,sizes,sizes_range,bags_stats = objects_stats()
  print('Total Objects Size: {}\n'.format(objects_num))
  report += 'Total Objects Size: {}\n'.format(objects_num)
  print('Total Objects Size: {}\n'.format(total_size))
  report += 'Total Objects Size: {}\n'.format(total_size)

  print('## Objects Size Distribution')
  report += '## Objects Size Distribution \n'
  tble = print_table([sizes])
  report += tble+'\n \r\n'
  print('\n')

  tble = print_table([sizes_range])
  report += tble+'\n'
  print('## Objects Size Distribution Per Bag')
  report += '## Objects Size Distribution Per Bag \n'
  tble = print_table(bags_stats, sort_key = 'total_size bytes')
  report += tble+'\n'

  print('# Lost Objects')
  report += '# Lost Objects \n'
  master_objects = get_objects(start_time,end_time)
  data = get_objects_files(file_server, operators, end_date, credential)
  operators = load_objects_from_server(data)
  operators_objects = []
  for operator in operators:
    operators_objects = operators_objects + operator['objects']
  lost = compare_objects(operators_objects, master_objects)
  total_objects = len(master_objects)
  lost_object = len(lost)
  print('Total Objects: {}\n'.format(total_objects))
  print('Total Lost Objects: {}\n'.format(lost_object))
  print('Percentage Lost Objects: %{}\n'.format(100*lost_object/total_objects))
  tble = print_table(lost, master_key = 'id')
  report += 'Total Objects: {} \n\n'.format(total_objects)
  report += 'Total Lost Objects: {} \n\n'.format(lost_object)
  report += 'Percentage Lost Objects: %{} \n\n'.format(100*lost_object/total_objects)
  report += tble+'\n'
  file_name = 'report_'+end_time+'.md'
  with open(file_name, 'w') as file:
    file.write(report)
    file.close()

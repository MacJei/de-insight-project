#!/usr/bin/python

"""
 Script is based on kiritbasu's Fake Apache Log generator: https://github.com/
 kiritbasu/Fake-Apache-Log-Generator/blob/master/apache-fake-log-gen.py
"""

import datetime
import numpy
import random
from faker import Faker
import json
import boto3
import uuid
import threading
import time
import sys
import multiprocessing

client = boto3.client('kinesis')

# Kinesis Stream info
stream_name = 'web_traffic'

# Initialize Faker object
fake = Faker()

# Create a sample list of events
event_list = ['pageView', 'click', 'purchase', 'addToCart']
event_sample = []
for i in range(10000):
    event_sample.append(numpy.random.choice(event_list, p=[0.8, 0.08, 0.04, 0.08]))

class Event(object):
	def __init__(self, user_agent, ip, user_id, timestamp, product_id, event_type):
		self.user_agent = user_agent
		self.ip = ip
		self.user_id = user_id
		self.timestamp = timestamp
		self.product_id = product_id
		self.event_type = event_type

# Create a list of unique user_ids
def create_unique_ids(user_count):
    id_list = [None]
    for i in range(user_count):
        id_list.append(
            numpy.random.choice([None, uuid.uuid4()], p=[0.3, 0.7])
        )
    with open('user_id_list.txt', 'wb') as openfile:
        for uid in id_list:
            openfile.write(str(uid) + '\n')
    print 'File created.'


# Create user agent list
def create_user_agent_list(ua_count):
    ua_list=[]
    for i in range(ua_count):
        ua_list.append(
            fake.user_agent()
        )
    with open('ua_list.txt', 'wb') as openfile:
        for ua in ua_list:
            openfile.write(ua + '\n')
    print 'File created.'


# Function to choose an object in a list with a normal distribution
def normal_distrib(id_list):
    # using a default mean of center
    mean = (len(id_list)-1)/2
    # using a default stddev of -3 to +3
    stddev = len(id_list)/6

    while True:
        index = int(random.normalvariate(mean,stddev) + 0.5)
        if 0 <= index < len(id_list):
            return id_list[index]


# Custom random choice function due to speed (no replacement)
def random_choice(items):
    while True:
        index = int(random.randint(0, len(items)))
        if 0 <= index < len(items):
            return items[index]


# Figure out the byte size of each record
def get_byte_size(record):
    return len(record.encode('utf-8'))


# Run streaming application
def run_stream(seconds, max_batch_size):
    time_end = time.time() + seconds # Set time to run
    batch_records = []
    total_records = 0
    record_size = 0
    byte_limit = 100000
    packaged_record = ""
    batch_byte_size = 0
    while time.time() < time_end:
        dt = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')

        user_event = Event(
                        normal_distrib(user_agent_list),
                        fake.ipv4(),
                        normal_distrib(user_id_list),
                        dt,
                        normal_distrib(list(range(1,1000))),
                        random_choice(event_sample)
                        )
        data = json.dumps(user_event.__dict__)
        
        total_records += 1
        # Package multiple records into a single record up to the byte limit  
        if record_size < 100000:
            record_size += get_byte_size(data)
            packaged_record += data + '\n'
        else:
            # Package user events into a Kinesis package of up to 500 records, 
            # to increase HTTP throughput
            batch_record = {
                'Data': packaged_record,
                'PartitionKey': str(uuid.uuid4())
            }
            batch_records.append(batch_record)
            batch_byte_size += record_size

            record_size = get_byte_size(data)
            packaged_record = data + '\n'

        if batch_byte_size > max_batch_size:
            print "{0} records. Pushing to Kinesis. Current rate: {1} records per second".format(total_records, total_records/seconds)
            client.put_records(Records=batch_records, StreamName=stream_name)
            batch_records = []
            batch_byte_size = 0


if __name__ == "__main__":
	# Read in a preset user files
	with open('user_id_list.txt', 'rb') as users:
	    uid = users.readlines()
	user_id_list = [x.strip('\n') for x in uid]

	# Read in a preset user agent file
	with open('ua_list.txt', 'rb') as agents:
	    user_agents = agents.readlines()
	user_agent_list = [x.strip('\n') for x in user_agents]
	
	try:
		jobs = []
		processes = int(sys.argv[1])
		seconds = int(sys.argv[2])
		max_records = int(sys.argv[3])
		
		for x in range(processes):
			p = multiprocessing.Process(target=run_stream, args=[seconds,max_records])
			jobs.append(p)
			p.start()
	except IndexError:
		print """
        	Invalid Input. Arguments are: 
		data_generator <processes> <seconds-to-stream> <max_batch_size> (in bytes)
		Example:
		python data_generator.py 2 300 100000
		"""





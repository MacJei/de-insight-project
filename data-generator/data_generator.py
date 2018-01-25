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
from boto import kinesis
import uuid
import threading
import time
import sys

# Kinesis Stream info
stream_name = 'web_traffic'
kinesis_stream = kinesis.connect_to_region('us-east-1')

# Initialize Faker object
fake = Faker()

# Create a sample list of events
event_list = ['pageView', 'click', 'purchase', 'addToCart']
event_sample = []
for i in range(10000):
    event_sample.append(numpy.random.choice(event_list, p=[0.8, 0.08, 0.04, 0.08]))


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
def run_stream(seconds, max_records):
    time_end = time.time() + seconds # Set time to run
    records = []
    total_records = 0
    while time.time() < time_end:
        dt = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')

        # Create event log
        user_event = {
            'user_agent': normal_distrib(user_agent_list),
            'ip': fake.ipv4(),
            'user_id': normal_distrib(user_id_list),
            'timestamp': dt,
            'product_id' : normal_distrib(list(range(1, 1000))),
            'event_type': random_choice(event_sample)
        }
        data = json.dumps(user_event)

        # Package user events into a Kinesis package of up to 500 records, to increase HTTP throughput
        record = {
            'Data': data,
            'PartitionKey': "123"
        }
        records.append(record)
        total_records += 1

        if total_records % max_records == 0:
            print "{0} records. Pushing to Kinesis".format(total_records)
            kinesis_stream.put_records(records, stream_name)
            records = []


if __name__ == "__main__":
	# Read in a preset user files
	with open('user_id_list.txt', 'rb') as users:
	    uid = users.readlines()
	user_id_list = [x.strip('\n') for x in uid]

	# Read in a preset user agent file
	with open('user_id_list.txt', 'rb') as agents:
	    user_agents = agents.readlines()
	user_agent_list = [x.strip('\n') for x in user_agents]
	
    try:
        threads = int(sys.argv[1])
        seconds = int(sys.argv[2])

        for x in range(2):
            t = threading.Thread(target=run_stream, args=[seconds,max_records])
            t.start()
    except:
        print """
        Invalid Input. Arguments are:
        data_generator <threads> <seconds-to-stream>
        
        Example:
        python data_generator.py 2 300
        """





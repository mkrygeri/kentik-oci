import time
import json
import re
import requests
import yaml
import sys
import logging, sys
import certifi
from schema import Schema, SchemaError
from confluent_kafka import Consumer
from base64 import b64decode
from influx_line_protocol import Metric, MetricCollection
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import http.client as http_client

http_client.HTTPConnection.debuglevel = 0


logging.basicConfig()
logging.getLogger().setLevel(logging.ERROR)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.ERROR)
requests_log.propagate = True
sampleRate = 1

with open('/config/config.yaml', 'r') as file:
    ocicfg = yaml.safe_load(file)


#kentik API stuff
headers = {
    'X-CH-Auth-Email': ocicfg['kentik_auth']['X-CH-Auth-Email'],
    'X-CH-Auth-API-Token': ocicfg['kentik_auth']['X-CH-Auth-API-Token'],
}


url = ocicfg['kentik_auth']['flow_url'] + ocicfg['kentik_device']['company_id'] + "%3A" + ocicfg['kentik_device']['device_name'] + "%3A" + ocicfg['kentik_device']['device_id']
api_url =  ocicfg['kentik_auth']['api_url']


#OCI API stuff for Kafka
topic = ocicfg['oci_conf']['topic']  
conf = {  
        'bootstrap.servers': ocicfg['oci_conf']['bootstrap.servers'],
        'security.protocol': ocicfg['oci_conf']['security.protocol'],  
        'ssl.ca.location': certifi.where(),
        #'ssl.ca.location': ocicfg['oci_conf']['ssl.ca.location'],  # from step 6 of Prerequisites section
        # optionally instead of giving path as shown above, you can do 1. pip install certifi 2. import certifi and
        # 3. 'ssl.ca.location': certifi.where()
        'sasl.mechanism': ocicfg['oci_conf']['sasl.mechanism'],
        'sasl.username': ocicfg['oci_conf']['sasl.username'],
        'sasl.password': ocicfg['oci_conf']['sasl.password'],  # from step 7 of Prerequisites section
        'group.id': ocicfg['oci_conf']['group.id'],
 }


#creates an influx line format record for each flow then returns it
def process(log):
    vniccompartmentocid = ''
    vnicocid = ''
    log = log.replace("Null:","")
    #sys.stdout.write(log)
    parsed = json.loads(log)
    metric = Metric('oci_flow_log')
    metric.add_tag('sampleRate',sampleRate)
   #Data Fields
    metric.add_tag('action',parsed['data']['action'])
    metric.add_tag('destinationAddress',parsed['data']['destinationAddress'])
    metric.add_tag('flowid',parsed['data']['flowid'])
    metric.add_tag('protocolName',parsed['data']['protocolName'])
    metric.add_tag('sourceAddress',parsed['data']['sourceAddress'])
    metric.add_tag('status',parsed['data']['status'])
    metric.add_tag('version',str(parsed['data']['version']))
    metric.add_tag('id',parsed['id'])
    #oracle fields
    metric.add_tag('compartmentid',parsed['oracle']['compartmentid'])
    metric.add_tag('ingestedtime',parsed['oracle']['ingestedtime'])
    metric.add_tag('loggroupid',parsed['oracle']['loggroupid'])
    metric.add_tag('logid',parsed['oracle']['logid'])
    metric.add_tag('tenantid',parsed['oracle']['tenantid'])
    metric.add_tag('vniccompartmentocid',parsed['oracle']['vniccompartmentocid'])
    metric.add_tag('vnicocid',parsed['oracle']['vnicocid'])
    metric.add_tag('vnicsubnetocid',parsed['oracle']['vnicsubnetocid'])
    #record information
    metric.add_tag('source',parsed['source'])
    metric.add_tag('specversion',parsed['specversion'])
    metric.add_tag('time',parsed['time'])
    metric.add_tag('type',parsed['type'])
    #metric data
    metric.add_value('bytesOut', str(parsed['data']['bytesOut']))
    metric.add_value('destinationPort',str(parsed['data']['destinationPort']))
    metric.add_value('endTime',str(parsed['data']['endTime']))
    metric.add_value('packets', str(parsed['data']['packets']))
    metric.add_value('protocol',str(parsed['data']['protocol']))
    metric.add_value('sourcePort',str(parsed['data']['sourcePort']))
    metric.add_value('startTime',str(parsed['data']['startTime']))
    #metric.with_timestamp(time.time_ns())
    #kentik doesn't need this, set to 0 
    metric.with_timestamp(0)
    #Change to a string & remove quotes
    metric = str(metric)
    metric = re.sub('"', '', metric)
    return metric
    

#reads/writes a file. this might help with persisting data, but it's probably best to not write to disk. Performance might dictate something different 
def sendit():
    file1 = open("oci.txt", "r")
    headers['Content-Type'] = 'application/influx'
    payload= file1.read()
    response = requests.post(url, headers=headers, data=payload)
    return response

def device_check():
    headers['Content-Type'] = 'application/json'
    try:
        response = requests.get(api_url + '/api/v5/device/' + ocicfg['kentik_device']['device_id'], headers=headers, data='' )
    except:
        sys.stdout.write("failed to request verification data exiting")
        exit(0)
    resp = json.loads(response.text)
    if resp['device']['device_sample_rate']:
        global sampleRate
        #sampleRate = resp['device']['device_sample_rate']
        sampleRate = int(resp['device']['device_sample_rate'])
    if resp['device']['id'] ==  ocicfg['kentik_device']['device_id']:
        sys.stdout.write("Valid Device ID")
    else:
        sys.stdout.write("the id in the config does not match")
        exit(0)
    if resp['device']['device_name'] ==  ocicfg['kentik_device']['device_name']:
        sys.stdout.write("Valid Device name: " + resp['device']['device_name'])
    else:
        sys.stdout.write("the device_name " + resp['device']['device_name']  + " in the config does not match" + ocicfg['kentik_device']['device_name'])
        exit(0)
    if resp['device']['company_id'] ==  ocicfg['kentik_device']['company_id']:
        sys.stdout.write("Valid company_id")    
    else:
        sys.stdout.write("the company_id in the config does not match")
        exit(0)


if __name__ == '__main__':
    count = 0
    samplecount = 1
    timer = 0
    file1 = open("oci.txt", "w")
  
    device_check()

    # Create Kafka Consumer instance
    consumer = Consumer(conf)

    # Subscribe to Kafka topic
    consumer.subscribe([topic])

    # Process messages
    try:
        while True:
            
            msg = consumer.poll(10.0)
            timer =+ 10
            if msg is None:
                #sys.stdout.write("Waiting for message or event/error in poll()")
                continue
        
            elif msg.error():
                sys.stdout.write('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = "Null" if msg.key() is None else msg.key().decode('utf-8')
                record_value = msg.value().decode('utf-8')
                count += 1
                samplecount += 1
                if samplecount%sampleRate:
                    continue 
                if not samplecount%sampleRate:
                    print("no skip")
                inflx = process(record_value)
                 

                if file1.closed:
                    file1 = open("oci.txt", "w+")
                file1.write(inflx + "\n")
                if (count == 2) | (timer >= 10 ):
                    file1.close()
                    response = sendit()
                    if response.text == "GOOD":
                        print("successful")
                        count = 0
                        timer = 0

                    else:
                        sys.stdout.write("something broke!" + str(response))
    except KeyboardInterrupt:
            pass
    finally:
            sys.stdout.write("Leave group and commit final offsets")
    consumer.close()

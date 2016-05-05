import mraa
import math
import time
import boto
import boto.dynamodb2
import datetime

import os.path
import csv

from boto import kinesis
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item

from decimal import Decimal

switch_pin_number=8



ACCOUNT_ID = '758957437187'
IDENTITY_POOL_ID = 'us-east-1:57e76a23-719b-4e4e-b063-494ddb08f7e5'
ROLE_ARN = 'arn:aws:iam::758957437187:role/Cognito_edisonDemoKinesisUnauth_Role'

# Use cognito to get an identity.
cognito = boto.connect_cognito_identity()
cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
oidc = cognito.get_open_id_token(cognito_id['IdentityId'])

# Further setup your STS using the code below
sts = boto.connect_sts()
assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])
DYNAMODB_TABLE_NAME = 'rawData'
DYNAMODB_NIGHT_NAME = 'nightDate'

client_dynamo = boto.dynamodb2.connect_to_region(
    'us-east-1',
    aws_access_key_id=assumedRoleObject.credentials.access_key,
    aws_secret_access_key=assumedRoleObject.credentials.secret_key,
    security_token=assumedRoleObject.credentials.session_token)

from boto.dynamodb2.table import Table
table_dynamo = Table(DYNAMODB_TABLE_NAME, connection=client_dynamo)

tables = client_dynamo.list_tables() 
if DYNAMODB_TABLE_NAME not in tables['TableNames']:
    print DYNAMODB_TABLE_NAME+' not found, creating table, wait 15s'
    table = Table.create(DYNAMODB_TABLE_NAME, schema=[
       HashKey('timestamp'),],connection=client_dynamo)
    time.sleep(15)
else:

    table = Table(DYNAMODB_TABLE_NAME, schema=[
       HashKey('timestamp'),
       ],connection=client_dynamo)  
    print DYNAMODB_TABLE_NAME+' selected' 

today = time.strftime("%Y-%m-%d")
t_midnight = "%s 00:00:00" %(today)
t_midnight = datetime.datetime.strptime(t_midnight, "%Y-%m-%d %H:%M:%S").timetuple()
t_midnight = time.mktime(t_midnight)

def put_in_raw_table(timestamp,temp,light,sound):
    table.put_item(
                    Item(table, data={ 'timestamp': str(timestamp),
                           'temperature': str(temp),
                           'light': str(light),
                           'sound': str(sound),
                         })
                      )

def get_avg(sum, count):# two ways: 1. with DB, 2. without db
  return (sum/count)
lightSensor = mraa.Aio(0)
tempSensor = mraa.Aio(1)
soundSensor = mraa.Aio(2)

count = 0
tem_sum = 0
light_sum = 0
sound_sum = 0

try:
  while(1):
    count += 1
    tt = time.time()
    timestamp = int((tt-t_midnight))#int((tt-t_midnight)/60)
    #collect the temperature data and make temperature readable
    a=tempSensor.read()
    R=1023.0/a-1.0
    R=100000.0*R
    temperature=Decimal(1.0/(math.log(R/100000.0)/4275+1/298.15)-273.15)
    temperature2=str(round(temperature,2))

    #collect the light data
    light = lightSensor.read()

    #collect the sound data
    sound =  soundSensor.read()

    #add(ID,temperature2)
    print "temperature: %s " %(temperature2)
    print "light: %s " %(str(light))
    print "sound: %s " %(str(sound))

    tem_sum += float(temperature2)
    light_sum += light
    sound_sum += sound

    put_in_raw_table(timestamp,temperature2,light,sound)
    time.sleep(1)# formal running: change to 1 mins (60)

except KeyboardInterrupt:
  '''
  if DYNAMODB_NIGHT_NAME not in tables['TableNames']:
    print DYNAMODB_NIGHT_NAME+' not found, creating table, wait 15s'
    table = Table.create(DYNAMODB_NIGHT_NAME, schema=[
       HashKey('day'),],connection=client_dynamo)
    time.sleep(15)
  else:

    table = Table(DYNAMODB_NIGHT_NAME, schema=[
       HashKey('day'),
       ],connection=client_dynamo)  
    print DYNAMODB_NIGHT_NAME+' selected' 
  table.put_item(
                    Item(table, data={ 'day': str(today),
                           'avg temperature': str(get_avg(tem_sum,count)),
                           'avg light': str(get_avg(light_sum,count)),
                           'avg sound': str(get_avg(sound_sum,count)),
                         })
                      )
  '''
  current_night = {"timestamp":str(today),"temp":get_avg(tem_sum,count),"light":get_avg(light_sum,count),"sound":get_avg(sound_sum,count)}
  print "Writing into csv..."
  init_flag = False
  if not (os.path.exists("./night.csv")):
    init_flag = True
  with open('night.csv', 'a') as fou:
    fieldnames = ['timestamp', 'temp','light', 'sound']
    dw = csv.DictWriter(fou,fieldnames=fieldnames)
    if init_flag:
      dw.writeheader()
    dw.writerow(current_night)

  print "Done."
  exit(0)
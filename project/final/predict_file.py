import mraa
import math
import time
import boto
import boto.dynamodb2
import datetime
import numpy as np
import functools
from numpy import mean, sqrt, square, arange, var, std
from collections import deque

import os.path
import csv

from boto import kinesis
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item

from decimal import Decimal

switch_pin_number=8
lightSensor = mraa.Aio(0)
tempSensor = mraa.Aio(1)
soundSensor = mraa.Aio(2)
today = time.strftime("%Y-%m-%d")
t_midnight = "%s 00:00:00" %(today)
t_midnight = datetime.datetime.strptime(t_midnight, "%Y-%m-%d %H:%M:%S").timetuple()
t_midnight = time.mktime(t_midnight)

a=tempSensor.read()
R=1023.0/a-1.0
R=100000.0*R
temperature=Decimal(1.0/(math.log(R/100000.0)/4275+1/298.15)-273.15)
temperature2=str(round(temperature,2))
#cur_pars.append(float(temperature2))
#collect the light data
light = lightSensor.read()
#cur_pars.append(light)
#collect the sound data
sound =  soundSensor.read()
#cur_pars.append(sound)

#compute sleep state in 3 mins (sleep/wake)
t_start_tmp = time.time()
timestamp = int((t_start_tmp-t_midnight))#int((tt-t_midnight)/60)

with open('predict.csv', 'w') as fou:
      fieldnames = ['temp','light', 'sound',"time"]
      dw = csv.DictWriter(fou,fieldnames=fieldnames)
      dw.writeheader()
      current_night = {"temp":temperature2,"light":light,"sound":sound,"time":timestamp}  
      print "write predict data into predict.csv."
      dw.writerow(current_night)
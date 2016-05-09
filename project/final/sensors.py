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
'''
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
'''
today = time.strftime("%Y-%m-%d")
t_midnight = "%s 00:00:00" %(today)
t_midnight = datetime.datetime.strptime(t_midnight, "%Y-%m-%d %H:%M:%S").timetuple()
t_midnight = time.mktime(t_midnight)
W_SIZE = 40#default window size for determine noise model


res = None
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

#functions for parameter definations
def get_rms(f):
  return float(sqrt(mean(square(f))))

def get_rlh(f):
  alpha = 0.25
  fl=[]
  fl.append(f[0]*alpha)
  fh=[]
  fh.append(f[0]*alpha)

  for i in range(1,len(f)):
    sl = fl[i-1]+alpha*(f[i]-fl[i-1])
    sh = alpha*(fh[i-1]+f[i]-f[i-1])
    fl.append(sl)
    fh.append(sh)
  return (float(get_rms(fl))/get_rms(fh))  

def get_var(f):
  mu = mean(f)
  func = lambda x : math.pow((x - mu),2)
  map(func,f)
  return float(sum(f))/len(f)
  #return float(var(f))

def get_frame_std(x,mean_std,min_std):
  return (float(x-mean_std)/(mean_std-min_std))

#w: window size
def compute_sound_feature(frames,w):
  e_frames = []
  n_frames = []
  
  event_count = 0
  std_frames = [std(_) for _ in frames]
  mean_std = mean(std_frames)
  min_std = min(std_frames)
  #normalize std_frames
  std_frames = map(lambda x: get_frame_std(x, mean_std,min_std), std_frames);
  #std_frames = map(functools.partial(get_frame_std, mean_std=mean_std,min_std=min_std),std_frames)

  w_count = 0
  w_std_list = []

  #print type(std_frames[0])
  w_tmp_list = []
  if len(std_frames) != len(frames):
    print "Length Error in compute_sound_feature()"
    exit(0)
  for i in range(0, len(frames)):
    #var(f)=:
    var = get_var(frames[i])
    #rms(f):
    rms = get_rms(frames[i])
    #rlh(f):
    rlh = get_rlh(frames[i])
  
    w_count+=1
    if w_count >=W_SIZE or i>=len(frames)-1:
      std_var = get_var(w_std_list)
      if std_var<0.5:
        n_frames += w_tmp_list
      else:
        e_frames += w_tmp_list
      w_count = 0
      w_std_list = []
      w_tmp_list = []
    w_std_list.append(std_frames[i])
    w_tmp_list.append([rms,rlh,var])
  print "Result:"
  print "Noise Frames #: ",len(n_frames)
  print "Event Frames #: ",len(e_frames)
  #exit(0)
  norm = get_norm_ref(n_frames)

  e_frames = map(lambda x: event_function(x, norm), e_frames);
  #print e_frames
  for _ in e_frames:
    if _[0]<= 40 and _[1]<= 10 and _[2]<= 30:
      event_count+=1
  return event_count
  #print X
  

def get_norm_ref(n_frames):#from noise frames
  return dict([("mean_rms",mean(zip(*n_frames)[0])),("std_rms",std(zip(*n_frames)[0])),("mean_rlh",mean(zip(*n_frames)[1])),("std_rlh",std(zip(*n_frames)[1])),("mean_var",mean(zip(*n_frames)[2])),("std_var",std(zip(*n_frames)[2]))])

#def get_event_ref(f):#get normalized parameter for event frames

def event_function(sublist,norm):
  e_rms = sublist[0]
  e_rlh = sublist[1]
  e_var = sublist[2]
  rms_bar = float(e_rms-norm['mean_rms'])/norm['std_rms']
  rlh_bar = float(e_rlh-norm['mean_rlh'])/norm['std_rlh']
  var_bar = float(e_var-norm['mean_var'])/norm['std_var']
  return [rms_bar,rlh_bar,var_bar]
'''
+++++++++++++++++++++++++++++
How to compute state is not putting ecs in a list
+++++++++++++++++++++++++++++

def label_states(states):
  try:
  #states elem: temp, light, sound, time, event_count
    states[0]
    states[1]
    states[2]
    states[3]
    D = get_D(0,0,0,0,states[0][4],states[1][4],states[2][4])
    states[0].append(D)
    D = get_D(0,0,0,states[0][4],states[1][4],states[2][4],states[3][4])
    states[1].append(D)
    D = get_D(0,0,states[0][4],states[1][4],states[2][4],states[3][4],states[4][4])
    states[2].append(D)
    D = get_D(0,states[0][4],states[1][4],states[2][4],states[3][4],states[4][4],states[5][4])
    states[3].append(D)

    L = len(states)

    for i in range(4,L-2):
      D = get_D(states[i-4][4],states[i-3][4],states[i-2][4],states[i-1][4],states[i][4],states[i+1][4],states[i+2][4])
      states[i].append(D)
    D = get_D(states[L-6][4],states[L-5][4],states[L-4][4],states[L-3][4],states[L-2][4],states[L-1][4],0)
    states[L-2].append(D)
    D = get_D(states[L-5][4],states[L-4][4],states[L-3][4],states[L-2][4],states[L-1][4],0,0)
    states[L-1].append(D)
    return states
  except IndexError:
    print "Too short duration to get result!"
'''
def get_D(que):
  val=[0,0,0,0,0,0,0]
  for i in range(0,len(que)):
    if que[i] != None:
      val[i] = que[i]
  D = 0.125*(0.15*val[0]+0.15*val[1]+0.15*val[2]+0.08*val[3]+0.21*val[4]+0.12*val[5]+0.13*val[6])#val[4] cur
  if D>1:
    return "wake"
  else:
    return "sleep"

if __name__ == "__main__":
  que = deque(7*[None],7)
  dw = None
  #init file inputs 
  
  init_flag = False
  if not (os.path.exists("./night.csv")):
    print "Create csv record file..."
    init_flag = True
  with open('night.csv', 'a') as fou:
    fieldnames = ['temp','light', 'sound',"time","state"]
    dw = csv.DictWriter(fou,fieldnames=fieldnames)
    if init_flag:
      dw.writeheader()

  # get signal value from sensors 
  lightSensor = mraa.Aio(0)
  tempSensor = mraa.Aio(1)
  soundSensor = mraa.Aio(2)

  #initiate assistant values
  count = 0
  tem_sum = 0
  light_sum = 0
  sound_sum = 0

  #parameters for derive sleep Q using sound signals
  frq = 16000#HZ
  smp_rate = 1.0/frq#float
  SMP_PER_FRAME = 10
  FRAME_PER_MIN = 600

  print smp_rate
  sleep_states = []
  countcycle = 0
  try:
    while(1):
      countcycle+=1
      #cur_pars=[]

      #collect the temperature data and make temperature readable
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
      raw_frame = []
      timestamp = int((t_start_tmp-t_midnight))#int((tt-t_midnight)/60)
      #cur_pars.append(timestamp)
      print "______________________________________"
      print "Collecting signals in 1 mins..."
      while(time.time()-t_start_tmp<60):
        
        raw_signal = []
        while(len(raw_signal)<500):
          raw_signal.append(soundSensor.read())
        raw_frame.append(raw_signal)

      #got 1 cycle data raw_frame[], then computing Q
      print "--------------------------------------"
      print "Total Frames #: ",len(raw_frame)

      print "Analysis frames..."
      ec=compute_sound_feature(raw_frame,W_SIZE)
      #cur_pars.append(ec)
      que.appendleft(ec)
      if countcycle<=2:
        print "Initial value %d ready to use" % (countcycle)
        continue
      print "Compute state for the current minute..."
      state_value = get_D(que)
      print "State: ",state_value
      #print ec
      #sleep_states.append(cur_pars)
      current_night = {"temp":temperature2,"light":light,"sound":sound,"time":timestamp,"state":state_value}  
      print "write current data into night.csv."
      dw.writerow(current_night)

  except KeyboardInterrupt:
    print "Exceptions!"
    exit(0)
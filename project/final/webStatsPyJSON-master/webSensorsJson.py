#Thanks for the callback code https://gist.github.com/aisipos/1094140
#TODO: 
##break this out so there is one sensor and other stuff is in different dirs?
##write other sensors
##add a function that figures out what OS is running
import os.path
import os
import ping
import time
import subprocess
import sys
import memuse
import signal
import alcoholsensor
from functools import wraps
from flask import Flask, url_for, jsonify, redirect, request, current_app, send_from_directory

app = Flask(__name__)

global maxPing
global minPing
global child_pid
global time_start
global time_end

maxPing = 0
minPing = 100

global p

def support_jsonp(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		callback = request.args.get('callback', False)
		if callback:
			content = str(callback) + '(' + str(f(*args,**kwargs).data) + ')'
			return current_app.response_class(content,mimetype='application/javascript')
		else:
			return f(*args, **kwargs)
	return decorated_function

@app.route('/ping')
def pinginfo():
	return 'please use `host`/ping/`targetHost` to ping a specific host'

@app.route('/ping/<string:whoToPing>')
@support_jsonp
def pinger(whoToPing):
	global maxPing
	global minPing
	pingT=float(ping.pingSomeone(whoToPing))
	if (pingT > maxPing):
		maxPing = pingT
	if (pingT < minPing):
		minPing = pingT
	pingJson={"stats":[{"ping":pingT,"maxPing":maxPing,"minPing":minPing}]}
	resp = jsonify(pingJson)
	resp.status_code = 200
	return resp

@app.route('/memused')
@support_jsonp
def memav():
	memused = int(memuse.memuse())
	memJson = {"memused":memused}
	resp = jsonify(memJson)
	resp.status_code = 200
	return resp

@app.route('/alcohol')
@support_jsonp
def alc():
	alccontent = int(alcoholsensor.alcoholcontent())
	alcJson = {"alcoholppm":alccontent}
	resp = jsonify(alcJson)
	resp.status_code = 200
	return resp

@app.route('/start')
@support_jsonp
def start_monitor():
	#global p
	#p = subprocess.call(["python","../sensors.py"])
	#print p


	# Now we can wait for the child to complete
	#global child_pid
	#os.system("../sensors.py")
	#global time_start = time.time()
	newpid = os.fork()
	if newpid == 0:
		memJson = {"state":"Start monitoring..."}
		subprocess.call(["python","../sensors.py"])
	else:	
		memJson = {"state":"Start monitoring..."}
	resp = jsonify(memJson)
	resp.status_code = 200
	return resp

@app.route('/predict')
@support_jsonp
def predict():
	subprocess.call(["python","../predict_file.py"])
	#subprocess.call("use_model command")
	memJson = {"state":"Get predict data..."}
	resp = jsonify(memJson)
	resp.status_code = 200
	return resp

@app.route('/end')
@support_jsonp
def end_monitor():
	#global p = 5654
	#os.kill(p, signal.SIGTERM)
	#global child_pid
	#global time_end
	#time_end = time.time()
	#if child_pid is None:
	#	memJson = {"state":"You haven't started, yet..."}
	#else:
	#os.kill(child_pid,signal.SIGTERM)
	memJson = {"state":"End monitoring..."}
	resp = jsonify(memJson)
	resp.status_code = 200
	return resp

if __name__ == '__main__':
	app.run(debug=True, host="0.0.0.0", port=8800)

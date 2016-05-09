#Thanks for the callback code https://gist.github.com/aisipos/1094140
#TODO: 
##break this out so there is one sensor and other stuff is in different dirs?
##write other sensors
##add a function that figures out what OS is running
import obddata
import gps
from functools import wraps
from flask import Flask, url_for, jsonify, redirect, request, current_app, send_from_directory

app = Flask(__name__)

obdconn = obddata.setupOBD()

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

@app.route('/gpsloc')
@support_jsonp
def gpsloc():
	location = gps.getLocation()
	locationJson = {"gpslocation":[{"lat":location[0],"long":location[1],"altitude":location[2]}]}
	resp = jsonify(locationJson)
	resp.status_code = 200
	return resp

@app.route('/obdrpm')
@support_jsonp
def obdrpm():
	rpm = int(obddata.rpm(obdconn))
	rpmJson = {"obdrpm":rpm}
	resp = jsonify(rpmJson)
	resp.status_code = 200
	return resp

@app.route('/obdengine_load')
@support_jsonp
def obdengine_load():
	engine_load = int(obddata.engine_load(obdconn))
	engine_loadJson = {"obdengine_load":engine_load}
	resp = jsonify(engine_loadJson)
	resp.status_code = 200
	return resp

@app.route('/obdcoolant_temp')
@support_jsonp
def obdcoolant_temp():
	coolant_temp = int(obddata.coolant_temp(obdconn))
	coolant_tempJson = {"obdcoolant_temp":coolant_temp}
	resp = jsonify(coolant_tempJson)
	resp.status_code = 200
	return resp

@app.route('/obdintake_pressure')
@support_jsonp
def obdintake_pressure():
	intake_pressure = int(obddata.intake_pressure(obdconn))
	intake_pressureJson = {"obdintake_pressure":intake_pressure}
	resp = jsonify(intake_pressureJson)
	resp.status_code = 200
	return resp

@app.route('/obdspeed')
@support_jsonp
def obdspeed():
	speed = int(obddata.speed(obdconn))
	speedJson = {"obdspeed":speed}
	resp = jsonify(speedJson)
	resp.status_code = 200
	return resp	

@app.route('/obdtiming_advance')
@support_jsonp
def obdtiming_advance():
	timing_advance = int(obddata.timing_advance(obdconn))
	timing_advanceJson = {"obdtiming_advance":timing_advance}
	resp = jsonify(timing_advanceJson)
	resp.status_code = 200
	return resp	

@app.route('/obdintake_temp')
@support_jsonp
def obdintake_temp():
	intake_temp = int(obddata.intake_temp(obdconn))
	intake_tempJson = {"obdintake_temp":intake_temp}
	resp = jsonify(intake_tempJson)
	resp.status_code = 200
	return resp	

@app.route('/obdthrottle_pos')
@support_jsonp
def obdthrottle_pos():
	throttle_pos = int(obddata.throttle_pos(obdconn))
	throttle_posJson = {"obdthrottle_pos":throttle_pos}
	resp = jsonify(throttle_posJson)
	resp.status_code = 200
	return resp	

@app.route('/obdvoltage')
@support_jsonp
def obdvoltage():
	voltage = int(obddata.voltage(obdconn))
	voltageJson = {"obdvoltage":voltage}
	resp = jsonify(voltageJson)
	resp.status_code = 200
	return resp

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)

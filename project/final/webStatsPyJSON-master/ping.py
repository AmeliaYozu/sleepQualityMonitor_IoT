#first sensor sample
#each sensor gives back a json structure...webSensorsJson.py merely presents it
#...and deals with jsonp callbacks
import subprocess
import platform

def pingSomeone(whoToPing):
	if platform.system() == 'Windows':
		output = subprocess.Popen(["ping.exe","-n", "1", whoToPing],stdout = subprocess.PIPE).communicate()[0]
	if platform.system() == 'Linux':
		output = subprocess.Popen(["ping","-c","1","google.com"],stdout = subprocess.PIPE).communicate()[0]
	#print(output)
	#parse the output here
	msPos=output.find("time=")
	msEnd=output.find("ms")
	pingTime=(output[msPos+5:msEnd])
	return pingTime
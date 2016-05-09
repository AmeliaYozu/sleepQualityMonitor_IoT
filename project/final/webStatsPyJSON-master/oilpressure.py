#oil pressure sensor
#based on the autometer 2242 oil pressure sender
#voltage divider with the sensor as R2 and R1 as a 100 Ohm resistor.
#returns a value that is the actual value * 10
import mraa

def getOilPressure(pin):
	try:
		a = mraa.Aio(pin)
		tval = a.read()
		if (psival > 722):
			return 0
		if (psival < 257):
     		return 9999
 		if ((psival <= 722)and(psival > 619)):
     		return 1747 - (psival*240)/100 
     	if ((psival <= 619)and(psival > 520)):
     		return 1802 - (psival*250)/100
 		if ((psival <= 520)and(psival > 411)):
     		return 1694 - (psival*230)/100
 		if ((psival <= 411)and(psival > 257)):
     		return 1418 - (psival*160)/100
	except:
		print("can't ADC for this sensor")
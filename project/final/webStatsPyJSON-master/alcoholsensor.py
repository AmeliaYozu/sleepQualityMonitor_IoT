#grove mq303a alcohol sensor
import pyupm_mq303a as upmMq303a

myAlcoholSensor = upmMq303a.MQ303A(0, 15)

def alcoholcontent():
	return myAlcoholSensor.value()
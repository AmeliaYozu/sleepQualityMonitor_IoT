#oiltemp sensor
#based on the autometer 2258(9) fluid temp sender
#use with a 150 ohm resistor at R2 and the sensor at R1 of a voltage divider
import mraa

def getOilTemp(pin):
	try:
		a = mraa.Aio(pin)
		tval = a.read()
		if (tval < 120):
			return (0)
		if (tval < 904):
			return((-.194 * tval + 195))
		if ((tval >= 120 )and(tval < 179)):
			return (-7.111 * tval + 1981)
		if ((tval >= 179 )and(tval < 252)):
			return (-3.407 * tval + 1318)
		if ((tval >= 252 )and(tval < 293)):
			return (-2.081 * tval + 984)
		if ((tval >= 293 )and(tval < 381)):
			return (-1.375 * tval + 777)
		if ((tval >= 381 )and(tval < 473)):
			return (-.853 * tval + 578)
		if ((tval >= 473 )and(tval < 563)):
			return (-0.578 * tval + 448)
		if ((tval >= 563 )and(tval < 571)):
			return (-4.78 * tval + 392)
		if ((tval >= 571 )and(tval < 602)):
			return (-0.447 * tval + 374)
		if ((tval >= 602 )and(tval < 643)):
			return (-0.397 * tval + 344)
		if ((tval >= 643 )and(tval < 714)):
			return (-0.335 * tval + 304)
		if ((tval >= 714 )and(tval < 800)):
			return (-0.269 * tval + 257)
		if ((tval >= 800 )and(tval < 844)):
			return (-.228 * tval + 224)
		if ((tval >= 844 )and(tval < 878)):
			return (-.207 * tval + 207)
		if ((tval >= 878 )and(tval < 904)):
			return (-.194 * tval + 195)
	except:
		print("can't ADC for this sensor")
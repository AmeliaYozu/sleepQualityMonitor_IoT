#gps sensor 
#for use with the seeed grove gps sensor (SIM28 based)
#uses UART (serial) 9600 8-N-1
import mraa

def getLocation():
	u = mraa.Uart(0)
	u.setBaudRate(9600)	
	GPSData = [0,0,0]
	while(True):
		if(u.dataAvailable()):
			buff = u.readStr(256)
			if buff.find("GPGGA") != -1: #this is the easiest to parse
				smallerbuff = buff[buff.find("GPGGA"): buff.find("\n")]
				splitbuff = smallerbuff.strip().split(",")
				#print(splitbuff)
				latnmea = splitbuff[2]
				latdir = splitbuff[3]
				lonnmea = splitbuff[4]
				londir = splitbuff[5]
				lat = float(latnmea[0:2]) + float(latnmea[2:])/60
				lon = float(lonnmea[0:3]) + float(lonnmea[3:])/60
				if londir == "W":
					lon = lon * -1
				alt = float(splitbuff[9])
				GPSData = [lat,lon,alt]
				return GPSData

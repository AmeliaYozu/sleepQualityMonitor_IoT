#memory available
import subprocess
import platform
import psutil

def memuse():
	return psutil.virtual_memory()[2]

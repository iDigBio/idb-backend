import logging

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=FORMAT,
                    filename='/tmp/run.log',
                    filemode='a')
formatter = logging.Formatter(FORMAT)

def getIDigBioLogger(name=""):
	logname = name
	if logname == "":
		logname = "idigbio"	
	logger = logging.getLogger(logname)
	return logger

logger = getIDigBioLogger()
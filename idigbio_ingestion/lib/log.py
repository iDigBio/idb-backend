import logging

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
formatter = logging.Formatter(FORMAT)

def getIDigBioLogger(name=""):
	logname = name
	if logname == "":
		logname = "idigbio"	
	logger = logging.getLogger(logname)
    logger.setFormatter(formatter)
	return logger

logger = getIDigBioLogger()
logger.setFormatter(formatter)
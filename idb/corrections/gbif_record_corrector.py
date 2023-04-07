import json
from py4j.java_gateway import JavaGateway

class GbifJavaRecordCorrector(object):

    def __init__(self):
        self.gateway = JavaGateway()

        # Find out right away if the Py4J gateway isn't running.
        try:
            jvm = self.gateway.jvm
            print("JVM started: JRE version " + jvm.System.getProperty("jvm.version", "unknown"))
        except:
            print("Error connecting to Py4J gateway, is it running?")
            raise
    
    def correct_record(self, record_obj):
        record_str = json.dumps(record_obj)
        corrected_record_str = self.gateway.entry_point.correctRecord(record_str)
        corrected_obj = json.loads(corrected_record_str)

        # mmk: the second returned value is ignored, but needs to be present;
        # it is meant to contain the keys for the values that were changed
        return corrected_obj, dict()

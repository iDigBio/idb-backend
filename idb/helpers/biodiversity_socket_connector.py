import socket
import json
import logging

from config import logger

class Biodiversity(object):

    def __init__(self,host="localhost",port=4334):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.sock.connect((host,port))
        except:
            logger.error("Biodiversity socket server unavailable")
            self.sock = None

        self.__recvBuf = ""

    def _sendOne(self,namestr):
        if self.sock is not None:
            self.sock.send(namestr + "\n")

    def _sendMany(self,namestr_list):
        if self.sock is not None:
            for ns in namestr_list:
                self._sendOne(ns)

    def _recvOne(self):
        if self.sock is not None:
            while "\n" not in self.__recvBuf:
                self.__recvBuf +=  self.sock.recv(2048)            

            resp, self.__recvBuf = self.__recvBuf.split("\n")

            return json.loads(resp)
        else:
            return { "scientificName": {"parsed": False }}

    def _recvMany(self,count):
        resp_l = []
        while count > 0:
            resp_l.append(self._recvOne())
            count -= 1
        return resp_l

    def _parseResp(self,r_obj):
        gs = {}

        if r_obj["scientificName"]["parsed"]:
            d_obj = r_obj["scientificName"]["details"][0]
            if "genus" in d_obj:
                gs["genus"] = d_obj["genus"]["string"].lower()

            if "species" in d_obj:
                gs["species"] = d_obj["species"]["string"].lower()

        return gs
    def get_genus_species(self,namestr):
        self._sendOne(namestr)
        return self._parseResp(self._recvOne())

    def get_genus_species_list(self,namestr_list):
        self._sendMany(namestr_list)
        resp_l = self._recvMany(len(namestr_list))
        return map(self._parseResp, resp_l)


def main():
    b = Biodiversity()

    print b.get_genus_species("Puma concolor")

    print b.get_genus_species_list([
            "Puma concolor",
            "Quercus alba",
            "Acer floridanum"
        ])

if __name__ == '__main__':
    main()
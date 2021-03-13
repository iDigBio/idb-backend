from __future__ import absolute_import
import socket
import json

from idb.config import logger


class Biodiversity(object):
    #: the socket object, None: not yet connected; False: failed connection
    _sock = None
    __recvBuf = ""

    def __init__(self, host="localhost", port=4334):
        self.host = host
        self.port = port

    def _connect(self):
        if not self._sock:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._sock.connect((self.host, self.port))
        except socket.error as e:
            logger.error("Biodiversity socket server unavailable: %s", e)
            self._sock = False

    @property
    def sock(self):
        if self._sock is None:
            self._connect()
        return self._sock

    @sock.setter
    def sock(self, value):
        if value is None and self._sock:
            try:
                self._sock.close()
            except:
                logger.exception("Failed closing")
        self._sock = value

    def _sendOne(self, namestr):
        if self.sock:
            try:
                self.sock.send(namestr.encode("utf-8") + "\n")
            except socket.error as e:
                logger.warn("Biodiversity send error: %s; retrying", e)
                self.sock = None
                self._sendOne(namestr)

    def _sendMany(self, namestr_list):
        if self.sock:
            for ns in namestr_list:
                self._sendOne(ns)

    def _recvOne(self):
        if self.sock:
            try:
                self.__recvBuf += self.sock.recv(2048)
            except socket.error as e:
                logger.warning("Biodiversity recv error: %s", e)
                self.sock = None
            else:
                try:
                    resp, self.__recvBuf = self.__recvBuf.split("\n", 1)
                    return json.loads(resp)
                except ValueError as e:
                    logger.warning("Error parsing json: %s", e)

        return {"scientificName": {"parsed": False}}

    def _recvMany(self, count):
        resp_l = []
        while count > 0:
            resp_l.append(self._recvOne())
            count -= 1
        return resp_l

    def _parseResp(self, r_obj):
        gs = {}

        if r_obj["scientificName"]["parsed"]:
            d_obj = r_obj["scientificName"]["details"][0]
            if "genus" in d_obj:
                gs["genus"] = d_obj["genus"]["string"].lower()

            if "species" in d_obj:
                gs["species"] = d_obj["species"]["string"].lower()

        return gs

    def get_genus_species(self, namestr):
        self._sendOne(namestr)
        return self._parseResp(self._recvOne())

    def get_genus_species_list(self, namestr_list):
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

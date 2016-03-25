from __future__ import absolute_import
from Crypto.Cipher import AES
from hashlib import md5
import binascii

BLOCK_SIZE = 16

def pad (data):
    pad = BLOCK_SIZE - len(data) % BLOCK_SIZE
    return data + pad * chr(pad)

def unpad (padded):
    pad = ord(padded[-1])
    return padded[:-pad]

def _encrypt(data, password):
    m = md5()
    m.update(password)
    key = m.hexdigest()

    m = md5()
    m.update(password + key)
    iv = m.hexdigest()

    data = pad(data)

    aes = AES.new(key, AES.MODE_CBC, iv[:16])

    encrypted = aes.encrypt(data)
    return binascii.b2a_hex(encrypted)

def _decrypt(edata, password):
    edata = binascii.a2b_hex(edata)

    m = md5()
    m.update(password)
    key = m.hexdigest()

    m = md5()
    m.update(password + key)
    iv = m.hexdigest()

    aes = AES.new(key, AES.MODE_CBC, iv[:16])
    return unpad(aes.decrypt(edata))

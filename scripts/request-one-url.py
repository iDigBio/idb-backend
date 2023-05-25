from __future__ import print_function
import requests

URL='https://ipt.amnh.org/rss.do'
USER_AGENT='HTTPie/1.0.3'

# USER_AGENT='iDigBio Update Publisher Recordset Service (idigbio@acis.ufl.edu https://www.idigbio.org/wiki/index.php/CYWG_iDigBio_DwC-A_Pull_Ingestion)'

req = requests.Request('GET',URL,headers={'Host':'ipt.amnh.org','Accept': '*/*', 'Accept-Encoding': 'gzip, deflate', 'Connection': 'keep-alive', 'User-Agent':USER_AGENT})
ipcheck_req = requests.Request('GET','https://danstoner.me/whatsmyip/plaintext.php',headers={'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate', 'Connection': 'keep-alive', 'User-Agent':USER_AGENT})
prepared = req.prepare()
ipcheck_prepared = ipcheck_req.prepare()
s1=requests.Session()
s2=requests.Session()
result = s1.send(prepared)
ipcheck_result = s2.send(ipcheck_prepared)
print("URL: ",URL)
print("HTTP Status Code: ",result.status_code)
print()
print("IP check: ")
print(ipcheck_result.content)

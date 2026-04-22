#!/usr/bin/env python3
"""
ftp-server-acuity.py  —  DDNL FTPS drop server for Acuity instance
Listens on 0.0.0.0:2122 with explicit TLS (FTPS).
All files land under ./ftp-drop-acuity/
Credentials: ftpuser / ddnl!

Connect with any FTPS client:
  Host     : shinkansen.proxy.rlwy.net
  Port     : 23806
  User     : ftpuser
  Password : ddnl!
  Protocol : FTP over explicit TLS (AUTH TLS)
"""

import os
from pyftpdlib.handlers.ftps.control import TLS_FTPHandler as FTPSHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.authorizers import DummyAuthorizer

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DROP_DIR  = os.path.join(BASE_DIR, 'ftp-drop-acuity')
CERT_FILE = os.path.join(BASE_DIR, 'ftps-acuity.crt')
KEY_FILE  = os.path.join(BASE_DIR, 'ftps-acuity.key')
FTP_HOST  = '0.0.0.0'
FTP_PORT  = 2122
FTP_USER  = 'ftpuser'
FTP_PASS  = 'ddnl!'

os.makedirs(DROP_DIR, exist_ok=True)

authorizer = DummyAuthorizer()
authorizer.add_user(FTP_USER, FTP_PASS, DROP_DIR, perm='elradfmw')

handler = FTPSHandler
handler.authorizer           = authorizer
handler.certfile             = CERT_FILE
handler.keyfile              = KEY_FILE
handler.tls_control_required = True    # require AUTH TLS on control channel
handler.tls_data_required    = False   # allow unencrypted data (FileZilla compat)
handler.passive_ports        = range(60020, 60030)
handler.masquerade_address   = os.environ.get('RAILWAY_PUBLIC_DOMAIN', None)
handler.banner               = 'DDNL FTPS drop — acuity channel'

server = FTPServer((FTP_HOST, FTP_PORT), handler)

print(f'FTPS server ready on {FTP_HOST}:{FTP_PORT}')
print(f'Drop folder : {DROP_DIR}')
print(f'Credentials : {FTP_USER} / {FTP_PASS}')
print(f'Cert        : {CERT_FILE}')
print('Ctrl+C to stop\n')

server.serve_forever()

#!/usr/bin/env python3
"""
ftp-server.py  —  DDNL local FTP drop server
Listens on localhost:2121 (non-privileged port, no root needed).
All files land under ./ftp-drop/sales/
Credentials: ftpuser / ddnl!

Usage:
    python3 ftp-server.py
"""

import os
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.authorizers import DummyAuthorizer

DROP_DIR = os.path.join(os.path.dirname(__file__), 'ftp-drop', 'sales')
FTP_HOST = '127.0.0.1'
FTP_PORT = 2121
FTP_USER = 'ftpuser'
FTP_PASS = 'ddnl!'

os.makedirs(DROP_DIR, exist_ok=True)

authorizer = DummyAuthorizer()
# perm: e=cwd, l=list, r=retr, a=append, d=delete, f=rename, m=mkdir, w=store
authorizer.add_user(FTP_USER, FTP_PASS, DROP_DIR, perm='elradfmw')

handler = FTPHandler
handler.authorizer = authorizer
handler.passive_ports = range(60000, 60010)
handler.banner = 'DDNL FTP drop — sales channel'

server = FTPServer((FTP_HOST, FTP_PORT), handler)

print(f'FTP server ready on ftp://{FTP_HOST}:{FTP_PORT}')
print(f'Drop folder : {DROP_DIR}')
print(f'Credentials : {FTP_USER} / {FTP_PASS}')
print('Ctrl+C to stop\n')

server.serve_forever()

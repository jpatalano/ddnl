#!/usr/bin/env python3
"""Called by generate-sales-csv.js: ftp-upload.py <host> <port> <user> <pass> <local_path> <remote_name>"""
import sys, ftplib
host, port, user, pw, local, remote = sys.argv[1:]
ftp = ftplib.FTP(); ftp.connect(host, int(port), timeout=15); ftp.login(user, pw); ftp.set_pasv(False)
with open(local, 'rb') as f: ftp.storbinary('STOR ' + remote, f)
ftp.quit(); print('OK')

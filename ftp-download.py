#!/usr/bin/env python3
"""Called by ftp-poller.js: ftp-download.py <host> <port> <user> <pass> <remote_name> <local_path>"""
import sys, ftplib
host, port, user, pw, remote, local = sys.argv[1:]
ftp = ftplib.FTP(); ftp.connect(host, int(port), timeout=30); ftp.login(user, pw); ftp.set_pasv(False)
with open(local, 'wb') as f: ftp.retrbinary('RETR ' + remote, f.write)
ftp.quit(); print('OK')

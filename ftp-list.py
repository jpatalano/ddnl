#!/usr/bin/env python3
"""Called by ftp-poller.js: ftp-list.py <host> <port> <user> <pass> — prints CSV filenames, one per line"""
import sys, ftplib, json
host, port, user, pw = sys.argv[1:]
ftp = ftplib.FTP(); ftp.connect(host, int(port), timeout=15); ftp.login(user, pw); ftp.set_pasv(False)
files = [f for f in ftp.nlst() if f.endswith('.csv')]
ftp.quit()
print('\n'.join(files))

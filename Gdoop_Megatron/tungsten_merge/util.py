#!/usr/bin/env python

"""
"""

from collections import Counter, namedtuple
from docopt import docopt
import logging
import os
import re
import sys
import yaml
import tempfile
import datetime
import subprocess

from shared import os_util



def run_zr(yml_file, conf):
    zr = os_util.which("zombie_runner")
    workflow_id = conf['--workflow_id']
    dryrun = conf['--dryrun']
    dryrun_str = ""
    if dryrun:
        dryrun_str = "--dryrun=true"
    yml_dir = os.path.dirname(yml_file)
    cmd = "{zr} run {yml_dir} --workflow_id={workflow_id} {dryrun_str}".format(**locals())
    logging.info("Running "+cmd)
    (ret, stdout, stderr) = os_util.execute_command(cmd.split(), do_sideput=True)
    if ret != 0:
        raise Exception("failed run cmd %s stdout=%s stderr=%s"%(cmd, stdout, stderr))
    #logging.info("cmd=%s stdout=%s stderr=%s ret=%d", cmd, stdout, stderr, ret)
    return

def make_dir(dir):
    mkdir = os_util.which("mkdir")
    cmd = mkdir +" -p %s" % dir
    logging.info("Running "+cmd)
    (ret, stdout, stderr) = os_util.execute_command(cmd.split(), do_sideput=True)
    logging.info("cmd=%s stdout=%s stderr=%s ret=%d", cmd, stdout, stderr, ret)
    if ret != 0:
        raise Exception("Failed create dir %s"%dir)
    return 
    
def email(to, subj, body):
    import socket
    import smtplib
    sender = 'etlalert@gmail.com'
    password = '3tl@lert'
    smtpserver = 'smtp.gmail.com'
    port = 587
    hostname = socket.gethostbyaddr(socket.gethostname())[0]
    headers = ["From: " + sender,
               "Subject: " + subj + " (" + hostname + ")",
               "To: " + to,
               "MIME-Version: 1.0",
               "Content-Type: text/plain"]
    headers = "\r\n".join(headers)
    session = smtplib.SMTP(smtpserver, port)
    session.ehlo()
    session.starttls()
    session.ehlo()
    session.login(sender, password)
    session.sendmail(sender, to.split(","), headers + "\r\n\r\n" + body)
    session.quit()

    


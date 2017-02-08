#!/usr/bin/env python

from collections import OrderedDict

import subprocess
import tempfile
import select
import logging
import collections
import os
import re
import shlex


def is_exe(fpath):
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)


def which(program):
    """
    :param program
    Checks for existence of program on command line and runs full path if it
    exists, None if it does not.
    """
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    raise Exception('%s could not be resolved to an executable' % program)


def get_hadoop():
    return which("hadoop")


def get_hive():
    return which("hive")


def collectify(item):
    """
    :param item
    Forces any argument into a collection.

    If an item is iterable, return it, else wrap it in an array and return
    it. If the item is None, return an empty array.
    """

    if isinstance(item, collections.Iterable) and not isinstance(item, str):
        return item
    elif item is None:
        return []
    else:
        return [item]


def execute_command(command, stdout=None, stderr=None, stdin=None,
                    shell=False, env=None, do_sideput=True):
    """
    :param command;
    :param stdout
    :param stderr
    :param stdin
    :param shell
    :param env
    :param do_sideput
    Invokes command (passed as an array with its arguments) as a subprocess.
    Blocks until command completes. Returns (process result, stdout, stderr).
    """

    if do_sideput:
        (result, stdout, stderr) = execute_command_fork_sideput(command, stdout, stderr, stdin, shell, env)
    else:
        p = execute_command_fork(command, stdout, stderr, stdin, shell, env)
        (stdout, stderr) = p.communicate()
        result = p.returncode

    if stdout is not None: stdout = stdout.strip()
    if stderr is not None: stderr = stderr.strip()

    return result, stdout, stderr


def execute_command_fork(command, stdout=None, stderr=None, stdin=None,
                         shell=False, env=None):
    """
    :param command;
    :param stdout
    :param stderr
    :param stdin
    :param shell
    :param env
    Invokes command (passed as an array with its arguments) as a subprocess.

    Does not wait for output. Returns process object.
    """

    command = map(str, collectify(command))  # coerce args to strings
    if shell:
        command = ' '.join(command)

    if stdout is None: stdout = subprocess.PIPE
    if stderr is None: stderr = subprocess.PIPE

    p = subprocess.Popen(command, stdout=stdout, stderr=stderr, stdin=stdin,
                         shell=shell, bufsize=-1, env=env)
    return p


def execute_command_fork_sideput(command, stdout=None, stderr=None, stdin=None,
                                 shell=False, env=None):
    """
    :param command;
    :param stdout
    :param stderr
    :param stdin
    :param shell
    :param env
    Invokes command (passed as an array with its arguments) as a subprocess.

    Does not wait for output. Returns process object.
    """

    command = map(str, command)  # coerce args to strings
    if shell:
        command = ' '.join(command)

    if stdout is None:
        stdout = tempfile.NamedTemporaryFile(prefix='stdout_', suffix='.log', dir='/tmp')
    if stderr is None:
        stderr = tempfile.NamedTemporaryFile(prefix='stderr_', suffix='.log', dir='/tmp')

    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=stdin,
                         shell=shell, bufsize=-1, env=env)

    while p.poll() is None:
        reads = [p.stdout.fileno(), p.stderr.fileno()]
        try:
            ret = select.select(reads, [], [])
        except select.error as ex:
            if ex[0] == 4:
                continue
            else:
                raise
        for fd in ret[0]:
            if fd == p.stdout.fileno():
                read = p.stdout.readline()
                logging.info(read.strip("\n"))
                stdout.write(read)
            if fd == p.stderr.fileno():
                read = p.stderr.readline()
                logging.info(read.strip("\n"))
                stderr.write(read)

    p.wait()
    read = p.stdout.read()
    logging.info(read.strip("\n"))
    stdout.write(read)

    read = p.stderr.read()
    logging.info(read.strip("\n"))
    stderr.write(read)

    stdout.seek(0)
    stderr.seek(0)

    result = p.returncode
    stdo = stdout.read()
    stde = stderr.read()

    return result, stdo, stde


def hdfs_mkdir(path):
    """
    :param path
    Executes 'hadoop fs -ls [path]' and returns list of fully-qualified
    subpaths
    """
    cmd = [get_hadoop(), "fs", '-mkdir', '-p', path]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_rm(path):
    cmd = [get_hadoop(), "fs", '-rm', '-r', '-skipTrash', path]
    logging.info("Running " + " ".join(cmd))
    execute_command(cmd)
    # (result, stdout, stderr) = execute_command(cmd)
    # if result is not 0:
    #     raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_put(local, hdfspath):
    cmd = [get_hadoop(), "fs", '-put', local, hdfspath]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_mv(source, destination):
    cmd = [get_hadoop(), "fs", '-mv', source, destination]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_touch(path):
    mkdir_path = path[:path.rfind('/')]
    hdfs_mkdir(mkdir_path)
    cmd = [get_hadoop(), "fs", '-touchz', path]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_test(path):
    cmd = [get_hadoop(), "fs", '-test', '-e', path]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        return False
    return True


def hdfs_ls(path):
    lst = []
    cmd = [get_hadoop(), "fs", '-ls', path]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        logging.error("File not found " + stderr)
        return lst

    for line in stdout.strip().split("\n"):
        line = line.strip()
        if "Found " in line:
            continue
        if not line:
            continue
        lst.append(line.split(" ")[-1])
    return lst


def hdfs_cat(path):
    cmd = [get_hadoop(), "fs", '-cat', path]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)

    return stdout.strip()


def exec_hive_query(query):
    cmd = [get_hive(), "-e", '"%s"' % query]
    iterations = 1

    for i in range(0, iterations):
        logging.info("Running " + " ".join(cmd))
        (result, stdout, stderr) = execute_command(cmd)
        if result != 0:
            raise Exception("Process ended abnormally: [%s]" % stderr)
        else:
            return result, stdout, stderr

    # logging.info("...Sending email unable to execute HIVE")
    # send_email('HIVE ERROR', 'unable to execute HQL repeatedly %s \n STDOUT: %s \n STDERR: %s '
    #                   % (query, stdout, stderr))


def exec_hive_file(f):
    cmd = [get_hive(), "-f", '"%s"' % f]
    logging.info("Running " + " ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result != 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    else:
        num_rows = 0
        output = stdout + "\n" + stderr
        for line in output.splitlines():
            m = re.search('numRows=(\d+),', line)
            if m:
                num_rows += int(m.group(1))
        logging.info("Hive File executed Successfully")
        logging.info("NUM_ROWS:%s" % str(num_rows))
        set('target_count', num_rows)
        return result


def table_schema(db_table):
    cmd = [get_hive(), "-S", "-e", "DESC FORMATTED %s" % db_table]
    logging.info("Running " + " ".join(cmd))
    schema_hash = dict()
    schema_hash['cols'] = OrderedDict()
    schema_hash['part_cols'] = OrderedDict()
    schema_hash['details'] = {}
    schema_hash['storage'] = {}
    (result, stdout, stderr) = execute_command(cmd)
    if result != 0:
        return schema_hash
    part_info = "# Partition Information"
    detailed_info = "# Detailed Table Information"
    storage_info = "# Storage Information"
    curr_key = 'cols'
    start = True
    for line in stdout.split('\n'):
        line = line.strip()
        if not line or (line[0] != "#" and start):
            continue
        start = False
        if line[0] == "#":
            if part_info in line:
                curr_key = 'part_cols'
            if detailed_info in line:
                curr_key = 'details'
            if storage_info in line:
                curr_key = 'storage'
            continue
        arr = shlex.split(line)
        if len(arr) >= 2:
            schema_hash[curr_key][arr[0]] = arr[1]

    schema_hash['location'] = schema_hash['details']['Location:']
    schema_hash['format'] = schema_hash['storage']['InputFormat:']

    return schema_hash


# def send_email(subject, message):
#     msg = MIMEText(message)  # Create a text/plain message
#
#     sender = 'edw-infra@groupon.com'
#     recipients = ['aguyyala@groupon.com']
#     msg['Subject'] = subject
#     msg['From'] = sender
#     msg['To'] = ", ".join(recipients)
#
#     s = smtplib.SMTP('localhost')
#     s.sendmail(sender, recipients, msg.as_string())
#     s.quit()

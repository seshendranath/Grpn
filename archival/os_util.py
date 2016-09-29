#!/usr/bin/env python


import subprocess
import tempfile
import select
import logging
import collections
import os


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

    command = map(str, collectify(command)) # coerce args to strings
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

    command = map(str, command) # coerce args to strings
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
        try: ret = select.select(reads, [], [])
        except select.error as ex:
            if ex[0] == 4: continue
            else: raise
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
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_rm(path):
    cmd = [get_hadoop(), "fs", '-rm', '-r', '-skipTrash',   path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_put(local, hdfspath):
    cmd = [get_hadoop(), "fs", '-put', local, hdfspath]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_mv(source, destination):
    cmd = [get_hadoop(), "fs", '-mv', source, destination]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_touch(path):
    mkdir_path=path[:path.rfind('/')]
    hdfs_mkdir(mkdir_path)
    cmd = [get_hadoop(), "fs", '-touchz',   path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)
    return True


def hdfs_test(path):
    cmd = [get_hadoop(), "fs", '-test', '-e',  path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        return False
    return True


def hdfs_ls(path):
    lst = []
    cmd = [get_hadoop(), "fs", '-ls', path]
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        logging.error("File not found "+stderr)
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
    logging.info("Running "+" ".join(cmd))
    (result, stdout, stderr) = execute_command(cmd)
    if result is not 0:
        raise Exception("Process ended abnormally: [%s]" % stderr)

    return stdout.strip()

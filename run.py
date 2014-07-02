#!/usr/bin/python


# Standard Library
import os
import sys
import time
import subprocess
from threading import Thread
from Queue import Queue

# Third Party
import yaml

# Local


waitTime = 10

services = [
    {
        "name": "frontend",
        "command": ["./main.py", "-e", "-d"],
        "color": "1"
    },
    {
        "name": "markcuban",
        "command": ["./main.py", "-e", "-d"],
        "color": "2"
    },
    {
        "name": "jaunt",
        "command": ["./main.py", "-e", "-d"],
        "color": "3"
    },
    {
        "name": "redshirt",
        "command": ["./main.py", "-e", "-d"],
        "color": "6"
    },
    {
        "name": "flint",
        "command": ["./main.py", "-e", "-d"],
        "color": "5"
    },
    {
        "name": "lego",
        "command": ["./main.py", "-e", "-d"],
        "color": "20"
    },
    {
        "name": "wf-run",
        "path": ["lego"],
        "command": ["celery","-A","lib.runner","worker"],
        "color": 22
    }
]


def getTerminalSize():
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
        '1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))

        ### Use get(key[, default]) instead of a try/catch
        #try:
        #    cr = (env['LINES'], env['COLUMNS'])
        #except:
        #    cr = (25, 80)
    return int(cr[1]), int(cr[0])


if __name__ == "__main__":

    conf = yaml.load(open("conf.yml"))
    os.environ["ZKHOSTS"] = "|".join(conf["zkhosts"])
    
    import mixingboard

    for key, value in conf.items():
        mixingboard.setConf(key, value)


    currentDirectory = os.path.dirname(os.path.realpath(__file__))
    logDirectory = os.path.join(currentDirectory, "logs")

    os.environ["PYTHONPATH"] += ":%s:" % currentDirectory

    try:
        os.mkdir(logDirectory)
    except OSError:
        pass

    def runService(service):

        logFileName = os.path.join(logDirectory, "%s.log" % service['name'])
        with open(logFileName,"a") as logFile:

            baseDirectory = currentDirectory
            serviceDirectory = ""
            if "path" in service:
                serviceDirectory = os.path.join(*([baseDirectory] + service['path']))
            else:
                serviceDirectory = os.path.join(baseDirectory, service['name']) 
            while True:
                proc = subprocess.Popen(service['command'], cwd=serviceDirectory, bufsize=0,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                print "\033[48;5;%smStarted %s...\033[0m" % (service['color'], service['name'])
                while True:
                    nextline = proc.stdout.readline()
                    if nextline == '' and proc.poll() != None:
                        break
                    (width, _) = getTerminalSize()
                    line = "\033[48;5;%sm%s:\033[0m " % (service['color'], service['name'])
                    extra = 13
                    while True:
                        nextLinePos = width - (len(line) - extra)
                        line += nextline[:nextLinePos]
                        sys.stdout.write(line)
                        nextline = nextline[nextLinePos:]
                        if len(nextline) > 0:
                            extra = 14
                            line = "\n\033[48;5;%sm%s \033[0m " % (service['color'], ' '*len(service['name']))
                        else:
                            sys.stdout.write('\n')
                            break
                    sys.stdout.flush()
                    logFile.write(nextline)
                    logFile.flush()
                    

                print "\033[48;5;%smProcess %s exited. Waiting %s seconds and restarting\033[0m" % (
                    service['color'],
                    service['name'],
                    waitTime
                )
                time.sleep(waitTime)

    for service in services:
        t = Thread(target=runService, args=(service,))
        t.daemon = True
        t.start()
        time.sleep(0.5)
         
    while True:
        try:
            time.sleep(10)
        except:
            sys.exit(0)

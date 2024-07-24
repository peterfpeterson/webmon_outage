#!/usr/bin/env python
import numpy as np
import pandas as pd
import json

######################################################################
# This is focused on the period from just before to just after
# 2024-07-11T12:00 and 2024-07-15T10:00 (EDT)
#
# The docker image for workflow manager has been up for 4 weeks before July 16
#
# activemq has been running since May 23/25
######################################################################

##### load when runs finished
runs_save = pd.read_csv('runs-for-peterson.csv') # , skiprows=1)
runs_save = runs_save.rename(columns={
    'Instrument': 'instr',
    'Experiment': 'exp',
    'Run Number': 'runnum',
    'Duration': 'duration',
    'End Time': 'endtime'
    })
runs_save['endtime'] = pd.to_datetime(runs_save['endtime'])

##### load list of AR restarts
ar_restarts = {}
with open('AR_restarts', 'r') as handle:
    key = 'junk'
    for line in handle:
        # strip whitespace and tld
        line = line.strip().replace('.sns.gov:', '')

        if line.startswith('autoreducer'):
            key = line
            ar_restarts[key] = []
        elif line.startswith('2024-07-'):
            line = line.split(',')[0]
            ar_restarts[key].append(pd.to_datetime(line))
        else:
            pass # don't bother with the other lines

def loadPostProcLog(filename):
    def hasRunID(msg):
        if "Input queues:" in msg:
            return False

        queues = ["/queue/CATALOG.ONCAT.DATA_READY:",
                  "/queue/CATALOG.ONCAT.STARTED:",
                  "/queue/CATALOG.ONCAT.COMPLETE:",
                  "/queue/REDUCTION.DATA_READY:",
                  "/queue/REDUCTION.STARTED:",
                  "/queue/REDUCTION.COMPLETE:",
                  "/queue/REDUCTION.DISABLED:",
                  "/queue/REDUCTION_CATALOG.DATA_READY",
                  "/queue/REDUCTION_CATALOG.STARTED",
                  "/queue/REDUCTION_CATALOG.COMPLETE"]
        for queue in queues:
            if queue and queue in msg:
                return True
        return False

    def getRunID(msg):
        # find the just with the run info
        try:
            start = msg.index("{")
            stop = msg.index("}")
            msg = msg[start:stop+1]
        except ValueError as e:
            raise RuntimeError(msg) from e

        # load into json
        try:
            stuff = json.loads(msg)
        except json.JSONDecodeError as e:
            raise RuntimeError(msg) from e

        # reduce the text and return
        msg = f"{stuff['facility']} {stuff['instrument']} {stuff['ipts']} {stuff['run_number']}"
        return msg

    def cleanMsg(msg):
        if msg.endswith("Sending frame: 'SEND'"):
            return "HEARTBEAT" # RETURN EARLY
        elif msg.startswith("SUBPROCESS/"):
            return "SUB-" + cleanMsg(" ".join(msg.split()[3:]))

        if msg.startswith("INFO/"):
            if hasRunID(msg):
                msg = " ".join(msg.split()[1:2]) + " " + getRunID(" ".join(msg.split()[2:]))
            else:
                msg = " ".join(msg.split()[1:])
            msg =  "INFO " + msg
        elif msg.startswith("WARNING/"):
            msg = "WARN " + " ".join(msg.split()[1:])

        # shorten commands
        if msg.startswith("WARN Command"):
            msg = "WARN PostProcessAdmin.py -q" + msg.split(',')[3] + " " + getRunID(msg)
        elif msg.startswith("INFO Created thread"):
            msg = "INFO Created thread to start daemon"
        elif msg.startswith("INFO Starting receiver loop"):
            msg = "INFO Starting receiver loop"

        # return the message we have
        return msg

    timestamps = []
    messages = []
    with open(filename) as handle:
        for line in handle:
            if line.startswith('2024-07-'):
                # split up the line
                line = line.strip()
                timestamp=' '.join(line.split()[:2])
                message = line.replace(timestamp, '').strip()
                # parse the timestampe
                timestamps.append(timestamp.split(',')[0])
                messages.append(cleanMsg(message))

    timestamps = pd.to_datetime(np.asarray(timestamps), format=r'%Y-%m-%d %H:%M:%S')
    for stmp, msg in zip(timestamps, messages):
        if True: # msg.startswith("WARN"):
            print(stmp, msg)
    print(f"THERE ARE {len(timestamps)} messages")
    return timestamps, messages

stuff = loadPostProcLog('AR1_postprocessing.log')

#!/usr/bin/env python
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

######################################################################
# This is focused on the period from just before to just after
# 2024-07-11T12:00 and 2024-07-15T10:00 (EDT)
#
# The docker image for workflow manager has been up for 4 weeks before July 16
#
# activemq has been running since May 23/25
######################################################################

##### load when runs finished
runs_save = pd.read_csv("runs-for-peterson.csv")  # , skiprows=1)
runs_save = runs_save.rename(
    columns={
        "Instrument": "instr",
        "Experiment": "exp",
        "Run Number": "runnum",
        "Duration": "duration",
        "End Time": "endtime",
    }
)
runs_save["endtime"] = pd.to_datetime(runs_save["endtime"])
runs_save.duration /= 3600.0

instruments = np.unique(runs_save.instr)

fig = plt.figure(layout="constrained")
fig.subplots_adjust(wspace=0)
ax = fig.subplots(2, 1, sharex="col")
fig.tight_layout()
for instr in instruments:
    mask = runs_save.instr == instr
    if np.count_nonzero(mask) < 15:  # only show things with a minimum number of runs
        continue
    ax[0].scatter(runs_save.endtime[mask], runs_save.duration[mask], label=instr)
ax[0].set_ylabel("acquisition (in hour)")
ax[0].legend()

ax[1].set_xlabel("time-of-day")


time_range = runs_save.endtime[runs_save.endtime.size - 1] - runs_save.endtime[0]
for instr in instruments:
    mask = runs_save.instr == instr
    number = np.count_nonzero(mask)
    print(f"{instr:7} - {number:4d} runs - 1 run every " + str(time_range / number))
print("Overall 1 run every " + str(time_range / runs_save.size))
print(f"    observed over {time_range} for {runs_save.size}")

##### load list of AR restarts
ar_restarts = {}
with open("AR_restarts", "r") as handle:
    key = "junk"
    for line in handle:
        # strip whitespace and tld
        line = line.strip().replace(".sns.gov:", "")

        if line.startswith("autoreducer"):
            key = line
            ar_restarts[key] = []
        elif line.startswith("2024-07-"):
            line = line.split(",")[0]
            ar_restarts[key].append(pd.to_datetime(line))
        else:
            pass  # don't bother with the other lines


CAT_STARTED = "/queue/CATALOG.ONCAT.STARTED:"
REDUX_STARTED = "/queue/REDUCTION.STARTED:"
REDUX_DISABLED = "/queue/REDUCTION.DISABLED:"
REDUX_CAT_STARTED = "/queue/REDUCTION_CATALOG.STARTED:"
REDUX_CREATE_SCRIPT = "/queue/REDUCTION.CREATE_SCRIPT:"


def loadPostProcLog(filename):
    def hasRunID(msg):
        if "Input queues:" in msg:
            return False

        if REDUX_CREATE_SCRIPT in msg:
            return False  # not going to both with these

        queues = [
            "/queue/CATALOG.ONCAT.DATA_READY:",
            CAT_STARTED,
            "/queue/CATALOG.ONCAT.COMPLETE:",
            "/queue/REDUCTION.DATA_READY:",
            REDUX_STARTED,
            "/queue/REDUCTION.COMPLETE:",
            REDUX_DISABLED,
            "/queue/REDUCTION_CATALOG.DATA_READY",
            REDUX_CAT_STARTED,
            "/queue/REDUCTION_CATALOG.COMPLETE",
        ]
        for queue in queues:
            if queue in msg:
                return True
        return False

    def getRunID(msg):
        # find the just with the run info
        try:
            start = msg.index("{")
            stop = msg.rindex("}")
            msg = msg[start : stop + 1]
        except ValueError as e:
            raise RuntimeError(msg) from e

        try:
            # load into json
            stuff = json.loads(msg)

            # reduce the text and return
            msg = f"{stuff['facility']} {stuff['instrument']} {stuff['ipts']} {stuff['run_number']}"
        except Exception as e:
            raise RuntimeError(msg) from e

        return msg

    def cleanMsg(msg):
        if msg.endswith("Sending frame: 'SEND'"):
            return "HEARTBEAT"  # RETURN EARLY
        elif msg.startswith("SUBPROCESS/"):
            return "SUB-" + cleanMsg(" ".join(msg.split()[3:]))

        if msg.startswith("INFO/"):
            if hasRunID(msg):
                msg = (
                    " ".join(msg.split()[1:2])
                    + " "
                    + getRunID(" ".join(msg.split()[2:]))
                )
            else:
                msg = " ".join(msg.split()[1:])
            msg = "INFO " + msg
        elif msg.startswith("WARNING/"):
            msg = "WARN " + " ".join(msg.split()[1:])

        # shorten commands
        if msg.startswith("WARN Command"):
            if hasRunID(msg):
                msg = (
                    "WARN PostProcessAdmin.py -q"
                    + msg.split(",")[3]
                    + " "
                    + getRunID(msg)
                )
            else:
                pass
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
            if line.startswith("2024-07-"):
                # split up the line
                line = line.strip()
                timestamp = " ".join(line.split()[:2])
                message = line.replace(timestamp, "").strip()
                # parse the timestampe
                timestamps.append(timestamp.split(",")[0])
                messages.append(cleanMsg(message))

    timestamps = pd.to_datetime(np.asarray(timestamps), format=r"%Y-%m-%d %H:%M:%S")
    # for stmp, msg in zip(timestamps, messages):
    #    if True:  # msg.startswith("WARN"):
    #        print(stmp, msg)
    print(f"THERE ARE {len(timestamps)} messages")
    # df = pd.DataFrame((timestamps, messages), columns=["time", "message"])
    df = pd.DataFrame({"time": timestamps, "message": messages})
    return df


for AR_NUM in (1, 2, 4):
    stuff = loadPostProcLog(f"AR{AR_NUM}_postprocessing.log")

    mask = stuff.message == "HEARTBEAT"
    ax[1].scatter(
        stuff.time[mask], np.zeros(np.count_nonzero(mask), dtype=float) + AR_NUM
    )

    for keyword, offset in (
        (CAT_STARTED, 0.1),
        (REDUX_STARTED, 0.2),
        (REDUX_DISABLED, 0.2),
        (REDUX_CAT_STARTED, 0.3),
    ):
        mask = np.zeros(stuff.message.size, dtype=bool)
        for i, msg in enumerate(stuff.message):
            mask[i] = bool("/queue/REDUCTION.STARTED: " in msg)
        ax[1].scatter(
            stuff.time[mask],
            np.zeros(np.count_nonzero(mask), dtype=float) + (AR_NUM + offset),
        )

for axis in ax:
    axis.label_outer()

fig.show()

#!/usr/bin/env python
import json
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import numpy as np
import pandas as pd
import matplotlib.dates as mdates

######################################################################
# This is focused on the period from just before to just after
# 2024-07-11T12:00 and 2024-07-15T10:00 (EDT)
#
# The docker image for workflow manager has been up for 4 weeks before July 16
#
# activemq has been running since May 23/25
######################################################################


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
            REDUX_DATA_READY,
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
            msg = f"{stuff['facility']} {stuff['instrument'].upper()} {stuff['ipts']} {stuff['run_number']}"
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
    kinds = []
    with open(filename) as handle:
        for line in handle:
            if line.startswith("2024-07-"):
                # split up the line
                line = line.strip()
                timestamp = " ".join(line.split()[:2])
                message = line.replace(timestamp, "").strip()
                # parse the timestampe
                timestamps.append(timestamp.split(",")[0])
                # set the type of the message
                if REDUX_STARTED in message:
                    kinds.append(REDUX_STARTED)
                elif REDUX_COMPLETE in message:
                    kinds.append(REDUX_COMPLETE)
                elif REDUX_DATA_READY in message:
                    kinds.append(REDUX_DATA_READY)
                else:
                    kinds.append("UNKNOWN")
                # set the message log
                messages.append(cleanMsg(message))

    timestamps = pd.to_datetime(np.asarray(timestamps), format=r"%Y-%m-%d %H:%M:%S")
    # for stmp, msg in zip(timestamps, messages):
    #    if True:  # msg.startswith("WARN"):
    #        print(stmp, msg)
    print(f"THERE ARE {len(timestamps)} messages")
    return pd.DataFrame({"time": timestamps, "message": messages, "kind": kinds})


##### errors reported by workflow-manager logs
ONCAT_ERROR = "CATALOG.ONCAT.ERROR"
CATALOG_ERROR = "CATALOG.ERROR"
REDUCTION_ENV = "REDUCTION: Failed to find launcher:"
REDUCTION_ERROR = "REDUCTION.ERROR"
NO_FILE = "Data file does not exist or is not readable"
CALVERA = "CALVERA.RAW.ERROR"

CAT_STARTED = "/queue/CATALOG.ONCAT.STARTED:"
REDUX_STARTED = "/queue/REDUCTION.STARTED:"
REDUX_COMPLETE = "/queue/REDUCTION.COMPLETE:"
REDUX_DISABLED = "/queue/REDUCTION.DISABLED:"
REDUX_CAT_STARTED = "/queue/REDUCTION_CATALOG.STARTED:"
REDUX_CREATE_SCRIPT = "/queue/REDUCTION.CREATE_SCRIPT:"
REDUX_DATA_READY = "/queue/REDUCTION.DATA_READY:"


def parseWkflwMgr(filename):
    with open(filename, "r") as handle:
        times = []
        messages = []
        kinds = []
        for line in handle:
            if "error" in line or "ERROR" in line or "Error" in line:
                item = json.loads(line.strip())
                if item["log"].startswith("SyntaxError"):
                    continue
                times.append(item["time"])
                messages.append(item["log"])

                if ONCAT_ERROR in item["log"]:
                    kinds.append(ONCAT_ERROR)
                elif CATALOG_ERROR in item["log"]:
                    kinds.append(CATALOG_ERROR)
                elif CALVERA in item["log"]:
                    kinds.append(CALVERA)
                elif NO_FILE in item["log"]:
                    kinds.append(NO_FILE)
                elif REDUCTION_ENV in item["log"]:
                    kinds.append(REDUCTION_ENV)
                elif REDUCTION_ERROR in item["log"]:
                    kinds.append(REDUCTION_ERROR)
                else:
                    kinds.append("UNKNOWN")
        times = pd.to_datetime(np.asarray(times), format="ISO8601")
        times -= np.timedelta64(4, "h")
        return pd.DataFrame({"time": times, "message": messages, "kind": kinds})


if __name__ == "__main__":
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

    fig, ax = plt.subplots(
        3, 1, sharex="col", layout="constrained", gridspec_kw={"wspace": 0, "hspace": 0}
    )
    fig.tight_layout()
    for instr in instruments:
        mask = runs_save.instr == instr
        if np.count_nonzero(mask) < 5:  # only show things with a minimum number of runs
            continue
        ax[0].scatter(
            runs_save.endtime[mask], runs_save.duration[mask], label=instr, marker="."
        )
    ax[0].set_ylabel("DAQ duration (in hour)")
    ax[0].legend(ncol=2)
    # ax[0].set_ylim(0, 800./3600)

    ax[2].set_xlabel("time-of-day")

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

    wkflw = parseWkflwMgr("workflow_manager_20240711_20240715.log")
    print("There are", wkflw.size, "data manager errors")

    for kind, offset in (
        ("UNKNOWN", 3.0),
        (ONCAT_ERROR, 3.1),
        (CATALOG_ERROR, 3.1),
        (CALVERA, 3.1),
        (REDUCTION_ENV, 3.2),
        (REDUCTION_ERROR, 3.3),
        (NO_FILE, 3.6),
    ):
        mask = wkflw.kind == kind
        count = np.count_nonzero(mask)
        ax[2].scatter(wkflw.time[mask], np.zeros(count) + offset, marker=".")

    # ax3 = ax[0].twinx()
    ax[1].set_ylabel("redux duration (in s)")

    ######################################################################

    ##### information from AR logs
    for AR_NUM in (1, 2, 4):
        # when messages come through
        stuff = loadPostProcLog(f"AR{AR_NUM}_postprocessing.log")

        mask = stuff.message == "HEARTBEAT"
        ax[2].scatter(
            stuff.time[mask],
            np.zeros(np.count_nonzero(mask), dtype=float) + AR_NUM,
            color="red",
            marker=".",
        )

        for keyword, offset, color in (
            (CAT_STARTED, 0.1, "orange"),
            (REDUX_DISABLED, 0.2, "green"),
            (REDUX_STARTED, 0.2, "blue"),
            (REDUX_CAT_STARTED, 0.3, "orange"),
        ):
            mask = np.zeros(stuff.message.size, dtype=bool)
            for i, msg in enumerate(stuff.message):
                mask[i] = bool("/queue/REDUCTION.STARTED: " in msg)
            ax[2].scatter(
                stuff.time[mask],
                np.zeros(np.count_nonzero(mask), dtype=float) + (AR_NUM + offset),
                color=color,
                marker=".",
            )

        # add information about reduction times
        mask = np.logical_or(
            stuff.kind == REDUX_DATA_READY, stuff.kind == REDUX_COMPLETE
        )
        kind = np.asarray(stuff.kind[mask])
        timestamp = np.asarray(stuff.time[mask])
        message = np.asarray(stuff.message[mask])
        print("BEFORE:", kind.size, "of", stuff.size)

        # clean out things before the first start
        index = np.where(kind == REDUX_DATA_READY)[0][0]
        kind = kind[index:]
        timestamp = timestamp[index:]
        message = message[index:]

        times = []
        durations = []

        counter = 0
        skipped = 0
        while counter < kind.size:  # everything should be the same size
            if counter + 1 >= kind.size:
                break
            if (
                kind[counter] == REDUX_DATA_READY
                and kind[counter + 1] == REDUX_COMPLETE
            ):
                runid = message[counter].split()[-4:]
                runid_next = message[counter + 1].split()[-4:]
                if runid == runid_next:
                    # print(counter, timestamp[counter], kind[counter], runid)
                    # print("  to", timestamp[counter+1], kind[counter+1], runid)
                    times.append(timestamp[counter])
                    durations.append(
                        (timestamp[counter + 1] - timestamp[counter])
                        / np.timedelta64(1, "s")
                    )
                    counter += 2
                else:
                    print(
                        f"SOMETHING IS WRONG with {AR_NUM}",
                        timestamp[counter],
                        runid,
                        runid_next,
                    )
                    counter += 1
            else:
                # print("skipping" , timestamp[counter], kind[counter], message[counter])
                skipped += 1
                counter += 1
        print("AFTER:", kind.size, "skipped", skipped)
        times = np.asarray(times)
        durations = np.asarray(durations)
        ax[1].scatter(times, durations, marker=".", label=f"AR{AR_NUM}")

    ######################################################################

    ax[1].legend()

    for ar in (1, 2, 4):
        ymin, ymax = float(ar) - 0.1, float(ar) + 0.6
        for timestamp in ar_restarts[f"autoreducer{ar}"]:
            ax[2].vlines(timestamp, ymin, ymax, color="black")

    # add annotation about extra nodes giving POSTPROCESSING.ERROR
    # adding 4 hours for time-zone shift
    start, stop = (
        np.datetime64("2024-07-13T10:22:06"),
        np.datetime64("2024-07-13T12:45:10"),
    )
    ax[2].plot((start, stop), (3.5, 3.5), linewidth=4)
    ax[2].text(start, 3.6, "AR5/AR6")

    # ax[1].set_ylabel("node")
    ax[2].yaxis.set_major_formatter(FormatStrFormatter("AR%d"))
    ax[2].set_yticks((1, 2, 3, 4))
    ax[2].xaxis.set_major_formatter(mdates.DateFormatter("%dT%H:%M"))

    time_min, time_max = np.min(runs_save.endtime), np.max(runs_save.endtime)
    for axis in ax:
        axis.label_outer()
        axis.set_xlim((time_min, time_max))

    fig.show()

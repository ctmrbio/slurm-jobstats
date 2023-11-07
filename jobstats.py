#!/usr/bin/env python3
"""Slurm jobstats on Gandalf."""
__author__ = "Fredrik Boulund"
__date__ = "2023"
__version__ = "0.5"

from sys import argv, exit, stdout
from collections import defaultdict
import os
import datetime
import shlex
import argparse
import subprocess

import pandas as pd

SACCT_FORMAT = ",".join([
    "Jobid",
    "Partition",
    "AllocCPUS",
    "TotalCPU",
    "ReqMem",
    "MaxRSS",
    "Start",
    "End",
    "Elapsed",
    "State",
    "Jobname",
])

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-u", "--user", default=os.environ.get('USER'),
            help="Username [%(default)s].")
    parser.add_argument("-s", "--start", default="now-1week",
            help="Start of time interval [%(default)s].")
    parser.add_argument("-o", "--outfile", default="jobstats.csv",
            help="Output data to csv table. Use special filename STDOUT "
                 "to print output to terminal instead, try piping into "
                 "'| column -t -s, | less -S' [%(default)s].")

    return parser.parse_args()


def call_sacct(user, start):
    result = subprocess.run(
        shlex.split(f"sacct --parsable2 --format={SACCT_FORMAT} --start {start} -u {user}"),
        capture_output=True,
    )
    return result.stdout.decode("utf-8").split("\n")[1:]


def parse_sacct(results):
    jobs = defaultdict(dict)
    for row in results:
        job = dict(zip(SACCT_FORMAT.split(","), row.split("|")))
        if not job["Jobid"]:
            continue
        if not job["State"] == "COMPLETED":
            continue
        job["AllocCPUS"] = int(job["AllocCPUS"])
        job["TotalCPU"] = parse_timedelta(job["TotalCPU"])
        job = parse_mem(job)
        job["Start"] = pd.to_datetime(job["Start"])
        job["End"] = pd.to_datetime(job["End"])
        job["Elapsed"] = parse_timedelta(job["Elapsed"])

        if ".batch" in job["Jobid"]:
            # .batch lines from sacct contain memory usage info in 
            # MaxRSS column that is not populated in normal lines
            job["Jobid"] = job["Jobid"].split(".")[0]
            jobs[job["Jobid"]]["MaxRSS"] = job["MaxRSS"]
            continue

        jobs[job["Jobid"]] = job

    if len(jobs) < 1:
        print("ERROR: Found no jobs!")
        exit(1)

    df = pd.DataFrame(jobs.values())

    df.dropna(inplace=True)

    return df


def parse_timedelta(timestring):
    if "-" in timestring:
        days, rest = timestring.split("-")
        td = pd.to_timedelta(rest) + datetime.timedelta(days=int(days))
    elif "." in timestring:
        t = datetime.datetime.strptime(timestring, "%M:%S.%f")
        td = datetime.timedelta(minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
    else:
        td = pd.to_timedelta(timestring) 
    seconds = td / datetime.timedelta(seconds=1)
    return seconds


def parse_mem(job):
    # ReqMem
    if "Gc" in job["ReqMem"]:
        reqmem = int(job["ReqMem"].split("G")[0])
        requested_GB = int(job["AllocCPUS"]) * reqmem
    elif "Gn" in job["ReqMem"]:
        requested_GB = int(job["ReqMem"].split("G")[0])
    elif "Mn" in job["ReqMem"]:
        reqmem = int(job["ReqMem"].split("M")[0])
        requested_GB = reqmem / 1024
    job["ReqMem"] = requested_GB

    # MaxRSS
    try:
        job["MaxRSS"] = int(job["MaxRSS"].strip("K")) / 1024 / 1024  # GB
    except ValueError as e:
        job["MaxRSS"] = 0 

    return job


def print_summary(jobs):
    print(f"Found {jobs.shape[0]} COMPLETED jobs since {args.start}, summary:")
    print(jobs.describe())
    cols = [
        "Jobid", "AllocCPUS","TotalCPU", "Elapsed", "MaxRSS", "ReqMem", "CPU_Efficiency", "MEM_Efficiency"
    ]
    if jobs.shape[0] < 10:
        print("Found less than 10 jobs:")
        print(jobs[cols])
    else:
        print(f"Showing random subsample of found jobs (10/{jobs.shape[0]}):")
        print(jobs[cols].sample(10))


if __name__ == "__main__":
    args = parse_args()

    results = call_sacct(args.user, args.start)
    jobs = parse_sacct(results)

    jobs["CPU_Efficiency"] = jobs["TotalCPU"] / (jobs["AllocCPUS"] * jobs["Elapsed"])
    jobs["MEM_Efficiency"] = jobs["MaxRSS"] / jobs["ReqMem"] 

    if args.outfile == "STDOUT":
        jobs.to_csv(stdout, index=False)
    else:
        print_summary(jobs)
        jobs.to_csv(args.outfile, index=False)
        print(f"Wrote complete output to {args.outfile}")








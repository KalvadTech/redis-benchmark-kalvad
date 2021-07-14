#!/usr/bin/env python3

from celery import Celery
import os
import subprocess
import redis
import csv
import time
import requests

broker_url = os.getenv("BROKER_URL")
app = Celery("hello", broker=broker_url)


@app.task
def redis_benchmark(port, flavour, req=1000000):
    r = redis.Redis(host="127.0.0.1", port=port, db=0)
    r.flushall()
    gts = []
    info = {}
    try:
        info = r.info()
    except:
        return False
    process = subprocess.Popen(
        [
            "redis-benchmark",
            "-q",
            "-n",
            str(req),
            "--csv",
            "-d",
            "1000",
            "--threads",
            "2",
            "-p",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    ustime = str(int(time.time_ns() / 1000))
    csv_reader = csv.reader(stdout.decode("ascii").splitlines(), delimiter=",")
    line_count = 0
    definitions = {}
    for row in csv_reader:
        if line_count == 0:
            definition = row
            line_count += 1
        else:
            j = 1
            while j < len(row):
                line = (
                    "{}// {}-{}(redis_version={},os={},cc_version={},redis_build_id={},tcp_port={},flavour={},req={}) {}".format(
                        ustime,
                        row[0].split(" ")[0].lower(),
                        definition[j].split(" ")[0].split("_")[0],
                        info["redis_version"],
                        info["os"].replace(" ", "_"),
                        info["gcc_version"],
                        info["redis_build_id"],
                        info["tcp_port"],
                        flavour,
                        req,
                        row[j],
                    )
                    .replace("(", "{")
                    .replace(")", "}")
                )
                gts.append(line)
                j += 1
            line_count += 1
    return send_warp10("\n".join(gts))


def send_warp10(gts):

    url = os.getenv("WARP10_URL")
    headers = {
        "X-Warp10-Token": os.getenv("WARP10_WRITE_TOKEN"),
        "Content-Type": "text/plain",
    }
    response = requests.request("POST", url, headers=headers, data=gts)
    if response.status_code == 200:
        print("Success")
        return True
    print("Failed")
    return False

"""
Fault capture
=============

Watches your pipeline and, the moment it sees a fault state, saves the
COMPLETE health record to a file. No filtering, no trimming - everything,
so you can go looking later for fields we haven't thought of yet.

Run this, then break the pipeline. Leave it broken for a couple of
minutes. It will write fault_<status>_<time>.json into this folder.

    python striim_capture.py
"""

import json
import os
import time
from datetime import datetime

import requests

STRIIM_URL = "https://developer.striim.com"
APP = "sarah_moolai.Trial1"
POLL_SECONDS = 60

FAULT_STATES = {"HALT", "TERMINATED", "CRASH"}


def get_token():
    r = requests.post(
        f"{STRIIM_URL}/security/authenticate",
        data={"username": os.environ["STRIIM_USER"],
              "password": os.environ["STRIIM_PASS"]},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["token"]


def main():
    token = get_token()
    saved_states = set()

    print(f"Watching {APP}. Break it now - I'll save whatever I see.\n")

    while True:
        try:
            response = requests.get(f"{STRIIM_URL}/health",
                                    params={"token": token}, timeout=30)
            if response.status_code in (401, 403):
                token = get_token()
                continue
            response.raise_for_status()
            record = response.json()["healthRecords"][0]
        except Exception as error:
            print(f"  fetch failed: {error}")
            time.sleep(POLL_SECONDS)
            continue

        status = record.get("appHealthMap", {}).get(APP, {}).get("status", "?")
        clock = datetime.now().strftime("%H:%M:%S")

        if status in FAULT_STATES and status not in saved_states:
            filename = f"fault_{status}_{datetime.now():%H%M%S}.json"
            # Save the WHOLE record, not just our pipeline. Fields we
            # ignore today may turn out to matter tomorrow.
            with open(filename, "w") as handle:
                json.dump(record, handle, indent=2)
            saved_states.add(status)
            print(f"[{clock}] {status}  ->  SAVED to {filename}")
        else:
            print(f"[{clock}] {status}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")

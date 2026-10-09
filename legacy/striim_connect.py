"""
Striim connection test
======================

One job: prove your machine can talk to Striim.
It logs in, gets a token, and lists your pipelines.

Fill in the three values below, then run:

    pip install requests
    python striim_connect.py
"""

import requests

# ------------------------------------------------ FROM THE ENVIRONMENT
# These used to be literals here. A real password sat in this file, which
# is why the rest of the project reads every secret from the environment
# instead - see setup.example.bat. `python -m agent doctor` supersedes
# this script anyway.
import os

STRIIM_URL = os.environ.get("STRIIM_URL", "https://developer.striim.com")
USERNAME = os.environ["STRIIM_USER"]    # the same one you log in with
PASSWORD = os.environ["STRIIM_PASS"]
# ---------------------------------------------------------------


def get_token():
    """Log in and receive a token.

    A token is a temporary pass. Your script sends your username and
    password once, and gets back a string it can use for every later
    request instead of logging in again.
    """
    print("STEP 1 - logging in...")

    response = requests.post(
        f"{STRIIM_URL}/security/authenticate",
        data={"username": USERNAME, "password": PASSWORD},
        timeout=30,
    )

    print("   status code:", response.status_code)
    print("   response   :", response.text[:300])

    response.raise_for_status()
    token = response.json()["token"]
    print("   got a token:", token[:8] + "..." + token[-4:])
    return token


def list_pipelines(token):
    """Ask for the health of everything, then print each pipeline."""
    print("\nSTEP 2 - asking for cluster health...")

    response = requests.get(
        f"{STRIIM_URL}/health",
        params={"token": token},
        timeout=30,
    )

    print("   status code:", response.status_code)
    response.raise_for_status()

    record = response.json()["healthRecords"][0]
    apps = record.get("appHealthMap", {})

    print("\nSTEP 3 - your pipelines:")
    if not apps:
        print("   (none found - the cluster reported no applications)")
    for name, info in apps.items():
        print(f"   {name:<40} {info.get('status')}")

    return record


if __name__ == "__main__":
    try:
        token = get_token()
        list_pipelines(token)
        print("\nDone. Copy the exact pipeline name above into your agent script.")
    except Exception as error:
        print("\nSomething failed. Copy everything printed above and share it.")
        print("Error:", type(error).__name__, "-", error)

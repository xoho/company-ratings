from csv import DictReader, DictWriter
import json
import os
import time
import cityhash
import requests
from typer import Typer
from pydantic import BaseModel
from rich import print
import sqlite3

from src.models import generate_id

# Connect to SQLite database (or create it if it doesn't exist)
FINCH_DB_MODE = os.getenv("FINCH_DB_MODE", "prod")
db_fn = "cache.db" if FINCH_DB_MODE != "dev" else "dev-cache.db"
conn = sqlite3.connect(db_fn)
cursor = conn.cursor()

# Create table for key/value pairs
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS orgs (
        key TEXT PRIMARY KEY,
        value INTEGER
    )
    """
)
conn.commit()


BASE_URL = os.getenv("FINCH_API_BASE_URL", "http://localhost:5000/api")
JWT_TOKEN = os.getenv("FINCH_JWT_TOKEN", "")

app = Typer()


def get_headers():
    return dict(Authorization=f"Bearer {JWT_TOKEN}")


def post(route: str, data: dict):
    url = f"{BASE_URL}/{route}"

    tries = 0
    max_tries = 3
    while tries < max_tries:
        try:
            r = requests.post(url, json=data, headers=get_headers())
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if r.status_code > 499:
                tries += 1
                sleep_time = 2**tries
                print(f"Server error, sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)
                continue
        if r.status_code != 200:
            raise Exception(f"Error posting to {url}: {r.status_code} {r.text}")
        if r.status_code == 200:
            return r.json()


def get(route: str):
    url = f"{BASE_URL}/{route}"

    r = requests.get(url, headers=get_headers())
    if r.status_code != 200:
        raise Exception(f"Error posting to {url}: {r.status_code} {r.text}")
    return r.json()


class PPP(BaseModel):
    description: str = "PPP Loan"
    name: str
    dba: str
    street1: str
    street2: str = ""
    city: str
    state: str
    postal_code: str
    country: str = "US"
    phone: str = ""
    email: str = ""
    change: str
    social_rating_change: int = 0
    antisocial_rating_change: int = 0
    tags: list[str] = []


ppp_mapping = dict(
    name="BorrowerName",
    dba="BorrowerName",
    street1="BorrowerAddress",
    city="BorrowerCity",
    state="BorrowerState",
    postal_code="BorrowerZip",
)


def get_cached_orgs():
    cursor.execute("SELECT key FROM orgs")
    return {x[0]: True for x in cursor.fetchall() if x}


def is_org_cached(org_name):
    cursor.execute("SELECT value FROM orgs WHERE key=?", (org_name.upper(),))
    result = cursor.fetchone()
    if result:
        return True
    return False


def add_org_to_cache(org_name):
    cursor.execute("INSERT OR REPLACE INTO orgs (key, value) VALUES (?, ?)", (org_name.upper(), 1))
    conn.commit()


@app.command()
def loadorgscache():
    """loads the orgs cache"""
    orgs = get("organizations")
    for org in orgs:
        cursor.execute("INSERT OR REPLACE INTO orgs (key, value) VALUES (?, ?)", (org.get("name").upper(), 1))
    conn.commit()


@app.command()
def orgs():
    orgs = get("organizations")
    print(orgs)


@app.command()
def loadppp(fn: str, chunk_size: int = 100, limit: int = 0, quick_mode: bool = False):
    if os.path.exists("stop.flg"):
        os.remove("stop.flg")

    cached_orgs = get_cached_orgs()

    cycles = 0
    chunk = []
    for row in DictReader(open(fn)):
        if os.path.exists("stop.flg"):
            break
        start = time.time()
        # check if we've already processed this org
        if row["BorrowerName"] in ["NOT AVAILABLE", "NOT APPLICABLE"]:
            continue
        if row["BorrowerName"] in cached_orgs:
            continue
        chunk.append(get_ppp(row, quick_mode))

        if len(chunk) == chunk_size:
            res = post("organizations", chunk)
            if res and "status" in res and "error" in res["status"]:
                # retry in single mode
                for org in chunk:
                    post("organizations", [org])
            if not quick_mode:
                for org in chunk:
                    add_org_to_cache(org["name"])
            if FINCH_DB_MODE == "dev":
                print(f"posted{' (quick mode)' if quick_mode else ''}: {[x['name'] for x in chunk]}")
            print(f"posted {len(chunk)} - {len(chunk)/(time.time()-start)} per second")
            chunk = []

        cycles += 1
        if limit and cycles >= limit:
            break

    if chunk:
        post("organizations", chunk)
        if not quick_mode:
            for org in chunk:
                add_org_to_cache(org["name"])
        if FINCH_DB_MODE == "dev":
            print(f"posted: {[x['name'] for x in chunk]}")
        print(f"posted {len(chunk)}")


def get_ppp(row, quick_mode=False):
    """
    {'LoanNumber': '3509338307', 'DateApproved': '01/22/2021',
    'SBAOfficeCode': '', 'ProcessingMethod': 'PPS',
    'BorrowerName': 'Exemption 6', 'BorrowerAddress': '',
    'BorrowerCity': '', 'BorrowerState': '',
    'BorrowerZip': '', 'LoanStatusDate': '02/18/2022',
    'LoanStatus': 'Paid in Full', 'Term': '60',
    'SBAGuarantyPercentage': '100', 'InitialApprovalAmount': '149360.6',
    'CurrentApprovalAmount': '149360.6', 'UndisbursedAmount': '0',
    'FranchiseName': '', 'ServicingLenderLocationID': '',
    'ServicingLenderName': '', 'ServicingLenderAddress': '',
    'ServicingLenderCity': '', 'ServicingLenderState': '',
    'ServicingLenderZip': '', 'RuralUrbanIndicator': 'U',
    'HubzoneIndicator': 'N', 'LMIIndicator': 'N',
    'BusinessAgeDescription': 'Existing or more than 2 years old',
    'ProjectCity': '', 'ProjectCountyName': '', 'ProjectState': '',
    'ProjectZip': '', 'CD': '', 'JobsReported': '15', 'NAICSCode': '624190',
    'Race': 'Unanswered', 'Ethnicity': 'Unknown/NotStated',
    'UTILITIES_PROCEED': '1', 'PAYROLL_PROCEED': '149357.6',
    'MORTGAGE_INTEREST_PROCEED': '', 'RENT_PROCEED': '',
    'REFINANCE_EIDL_PROCEED': '', 'HEALTH_CARE_PROCEED': '',
    'DEBT_INTEREST_PROCEED': '', 'BusinessType': 'Non-Profit Organization',
    'OriginatingLenderLocationID': '', 'OriginatingLender': '',
    'OriginatingLenderCity': '', 'OriginatingLenderState': '',
    'Gender': 'Unanswered', 'Veteran': 'Unanswered', 'NonProfit': 'Y',
    'ForgivenessAmount': '150775.38', 'ForgivenessDate': '01/13/2022'}
    """
    kwargs = dict()
    if quick_mode:
        for k, v in [(x, y) for x, y in ppp_mapping.items() if x in ["name", "dba"]]:
            kwargs[k] = row[v].title()
    else:
        for k, v in ppp_mapping.items():
            kwargs[k] = row[v].title()

    if " DBA " in row["BorrowerName"].upper():
        kwargs["name"] = row["BorrowerName"].upper().split(" DBA ")[0]
        kwargs["dba"] = row["BorrowerName"].upper().split(" DBA ")[1]

    ppp = None

    if not quick_mode:
        kwargs["change"] = json.dumps(row)
        kwargs["tags"] = ["ppp"]
        kwargs["antisocial_rating_change"] = int(float(row.get("ForgivenessAmount", "").strip() or "0"))
        ppp = PPP(**kwargs).dict()
    else:
        ppp = dict(name=kwargs["name"], dba=kwargs["dba"])
    return ppp


@app.command()
def loaduser(fn: str):
    """loads a user from a json file"""
    if not os.path.exists(fn):
        raise Exception(f"File {fn} does not exist")

    user = json.load(open(fn))
    post("user", user)
    print("posted user")


@app.command()
def showuserformat():
    """shows the format for a user json file"""
    user_format = {
        "user": {
            "username": "username",
            "given_name": "first_name",
            "family_name": "last_name",
            "social_media_accounts": [{"platform": "platform", "handle": "handle"}],
            "email_addresses": ["email_address"],
            "physical_addresses": [
                {
                    "street1": "street1",
                    "street2": "street2",
                    "city": "city",
                    "state": "state",
                    "postal_code": "postal_code",
                    "country": "country",
                }
            ],
            "telephone_numbers": ["telephone_number"],
        }
    }
    print(user_format)


@app.command()
def stats():
    """shows stats"""
    stats = get("stats")
    print(stats)


@app.command()
def pruneppp(fn: str):
    if not os.path.exists(fn):
        raise Exception(f"File {fn} does not exist")

    data = []
    processed_names = get_cached_orgs()
    print("number of cached orgs:", len(processed_names))
    for row in DictReader(open(fn)):
        if row["BorrowerName"] in ["NOT AVAILABLE", "NOT APPLICABLE"]:
            continue
        if row["BorrowerName"] in processed_names:
            continue
        data.append(row)

    if not data:
        print("no new data to process")
        return
    new_fn = "pruned.csv"
    with open(new_fn, "w") as f:
        writer = DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)


@app.command()
def prepppp(fn: str):
    if not os.path.exists(fn):
        raise Exception(f"File {fn} does not exist")

    cached_orgs = get_cached_orgs()
    data = []
    for row in DictReader(open(fn)):
        if row["BorrowerName"] in ["NOT AVAILABLE", "NOT APPLICABLE"]:
            continue
        if row["BorrowerName"] in cached_orgs:
            continue
        _row = dict(
            id=generate_id(12),
            name=row["BorrowerName"].title(),
            dba=row["BorrowerName"].title(),
            social_rating=0,
            antisocial_rating=int(float(row["ForgivenessAmount"].strip() or "0")),
            is_active=True,
        )
        data.append(_row)

    if not data:
        print("no new data to process")
        return

    # dump the data to a csv file
    with open("preppp.csv", "w") as f:
        writer = DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    print("wrote preppp.csv")


@app.command()
def loadpppbyname(target_orgs_fn: str, source_fn: str):
    if not os.path.exists(target_orgs_fn):
        raise Exception(f"File {target_orgs_fn} does not exist")
    if not os.path.exists(source_fn):
        raise Exception(f"File {source_fn} does not exist")
    # for each name in target_orgs_fn .csv, add to target orgs
    target_orgs = {}
    for row in DictReader(open(target_orgs_fn)):
        target_orgs[row["name"].title()] = True

    for row in DictReader(open(source_fn)):
        name = row["BorrowerName"].title()
        if name not in target_orgs:
            continue
        ppp = get_ppp(row, quick_mode=False)
        post("organizations", [ppp])
        add_org_to_cache(ppp["name"])
        print(f"Processed {ppp['name']}")


if __name__ == "__main__":
    app()

import json
import os
from io import BytesIO
import gzip
from random import randint
from csv import DictReader
from redis import StrictRedis
from hashlib import sha256
from time import time, sleep
from typer import Typer
from string import punctuation

import boto3

app = Typer()

r = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"), port=os.environ.get("REDIS_PORT", 6379), db=2)

s3 = boto3.client("s3")
s3_bucket = "socialcredit-prod-ohchae7p"
s3_base_key = "data/words"


stop_words = [
    "THE",
    "AND",
    "OF",
    "TO",
    "IN",
    "FOR",
    "A",
    "IS",
    "THAT",
    "ON",
    "WITH",
    "AS",
    "AT",
    "BY",
    "FROM",
    "BE",
    "&",
]


@app.command()
def processcsv(fn: str):
    # get latest processed record from redis
    basename = os.path.basename(fn)
    latest_processed = r.get(f"processed:{basename}")
    if latest_processed is None:
        latest_processed = -1
    else:
        latest_processed = int(latest_processed.decode("utf-8"))
    reader = DictReader(open(fn, "r"))
    count = 0
    start = time()
    chunk_size = 100
    pipeline = r.pipeline()
    for idx, row in enumerate(reader):
        if idx <= latest_processed:
            continue
        org = row.get("BorrowerName", None)
        # print(f"{idx}: {org}")
        if org is None:
            continue
        for word in [x for x in org.split() if x not in stop_words]:
            for p in punctuation:
                word = word.replace(p, "")
            if len(word) == 0:
                continue
            word = f"word:{word.lower()}"
            # print(f"adding {org} to {word}")
            # add org to word set
            pipeline.sadd(word, org)

        if count % chunk_size == 0:
            pipeline.execute()
            # set latest processed record
            r.set(f"processed:{basename}", idx)

            key = f"linecount:{fn}"
            linecount = 0
            data = r.get(key)
            if data is None:
                reader = DictReader(open(fn, "r"))
                linecount = len(list(reader))
                r.set(key, linecount)
            else:
                linecount = int(data.decode("utf-8"))
            percent_complete = "0%"
            if linecount > 0:
                percent_complete = f"{idx/linecount*100:.2f}%"

            duration = time() - start
            if duration > 0:
                print(
                    f"{basename}: {count/duration:.2f} records/sec ({r.get(f'processed:{basename}').decode('utf-8')} - {percent_complete} complete)"
                )

            if os.path.exists("halt.flg"):
                print("halt.flg found, halting")
                break
            count = 0
            sleep(randint(5, 10))
            start = time()

        count += 1


@app.command()
def rename():
    base_folder = "data/ppp"
    for fn in [x for x in os.listdir(base_folder) if x.endswith(".csv")]:
        key = f"processed:{base_folder}/{fn}"
        print(key, fn)
        if r.exists(key):
            r.rename(key, f"processed:{fn}")


@app.command()
def progress():
    base_folder = "data/ppp"
    for fn in [x for x in os.listdir(base_folder) if x.endswith(".csv")]:
        key = f"linecount:{fn}"
        linecount = 0
        data = r.get(key)
        if data is None:
            reader = DictReader(open(f"{base_folder}/{fn}", "r"))
            linecount = len(list(reader))
            r.set(key, linecount)
        else:
            linecount = int(data.decode("utf-8"))
        key = f"processed:{fn}"
        processed = r.get(key)
        if processed is None:
            processed = 0
        else:
            processed = int(processed.decode("utf-8"))
        if linecount > 0:
            print(f"{fn}: {processed}/{linecount} ({processed/linecount*100:.2f}%)")


@app.command()
def loadwordindex():
    words = r.keys("word:*")
    pipeline = r.pipeline()
    for word in words:
        pipeline.sadd("wordindex", word.decode("utf-8"))
    pipeline.execute()


@app.command()
def wordindexcount():
    if os.path.exists("halt.flg"):
        print("0")
    else:
        print(r.scard("wordindex"))


@app.command()
def loadtos3():
    chunk_size = 100
    count = 0
    start = time()
    while r.scard("wordindex") > 0:
        word_key = r.spop("wordindex").decode("utf-8")
        if not word_key:
            break
        data = [x.decode("utf-8") for x in r.smembers(word_key)]

        if len(data) == 0:
            continue
        word = word_key.split(":")[1]
        try:
            data = dict(word=word, organizations=list(data))
            s3_fn = f"{s3_base_key}/{word}.json.gz"
            # gzip the data and send to s3
            buffer = BytesIO()
            with gzip.GzipFile(mode="wb", fileobj=buffer) as f:
                f.write(json.dumps(data).encode("utf-8"))
            s3.put_object(
                Bucket=s3_bucket,
                Key=s3_fn,
                Body=buffer.getvalue(),
                ContentEncoding="gzip",
                ContentType="application/json",
            )
        except Exception as e:
            r.sadd("wordindex", word_key)
            print(f"Could not process {word_key}: {e}")

        if count > chunk_size:
            if os.path.exists("halt.flg"):
                print("halt.flg found, halting")
                break
            duration = time() - start
            if duration > 0:
                print(f"{count/duration:.2f} records/sec - {r.scard('wordindex')} remaining")
            # count = 0
            # start = time()
            break

        count += 1


if __name__ == "__main__":
    app()

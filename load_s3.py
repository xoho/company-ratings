from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import gzip
from io import BytesIO
import json
import os
import re
import sqlite3
from string import punctuation
from time import time, sleep
from typer import Typer

import boto3
from filelock import FileLock
from sqlalchemy import create_engine, text

app = Typer()

conn = sqlite3.connect("tracking.db")
conn.cursor().execute("CREATE TABLE IF NOT EXISTS tracking (fn TEXT, offset INT, completed INT, worker INT)")

conn_word = sqlite3.connect("words.db")

engine = create_engine(os.getenv("SQLALCHEMY_DATABASE_URI"))

# Initialize DynamoDB
table_name = "socialcredit-organizations-dev-oow4yeit"
# table_name = "socialcredit-wordindex-dev-ohgha0ei"
# dynamodb = boto3.resource("dynamodb")
# table = dynamodb.Table(table_name)
# fn = "data/public_150k_plus_230630.csv"
data_folder = "data/ppp"
word_folder = os.path.join(data_folder, "words")
batch_folder = os.path.join(data_folder, "batches")
staging_folder = os.path.join(data_folder, "staging")

s3_bucket = "socialcredit-prod-ohchae7p"
s3_base_key = "data/words"
s3_base_org_key = "data/organizations"

max_worker_count = os.cpu_count() * 5
loop_sleep_time = 0

os.makedirs(word_folder, exist_ok=True)
os.makedirs(batch_folder, exist_ok=True)
os.makedirs(staging_folder, exist_ok=True)

stop_words = [
    "the",
    "and",
    "of",
    "to",
    "in",
    "for",
    "a",
    "is",
    "that",
    "on",
    "with",
    "as",
    "at",
    "by",
    "from",
    "be",
    "&",
]
hash_key = "OrganizationName"

# sqlite = sqlite3.connect("tracking.db")
# sqlite.execute("CREATE TABLE IF NOT EXISTS tracking (key TEXT)")


def extract_words(text):
    words = []
    for word in [x for x in re.findall(r"\w+", text) if x not in stop_words]:
        words.append("".join([x for x in word if x not in punctuation]))
    return list(set(words))


def write_batch(batch, offset):
    data = []
    for row in batch:
        words = extract_words(row["BorrowerName"])
        for word in words:
            data.append([word, row["BorrowerName"]])

    fn = os.path.join(batch_folder, f"{offset}.json")
    with open(fn, "w") as f:
        json.dump(data, f)


if os.path.exists("stop.flg"):
    os.remove("stop.flg")


def rip_words():
    for fn in [os.path.join(data_folder, x) for x in os.listdir(data_folder) if x.endswith(".csv")]:
        print("Starting", fn)
        with open(fn, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            start = time()
            count = 0
            for row in reader:
                if os.path.exists("stop.flg"):
                    print("Stop flag found, exiting")
                    exit(0)

                batch.append(row)
                if len(batch) >= batch_size:
                    # write batch
                    write_batch(batch, offset)

                    duration = time() - start
                    if duration > 0:
                        print(f"{len(batch)/duration} records/sec")
                        start = time()
                        batch = []
                        offset += batch_size

        print("Finished", fn)


class Item:
    def __init__(self, word, orgs):
        self.word = word
        self.orgs = orgs

    def __dict__(self):
        """
        {"Item":{"Name": {"S": "Amazon Baz"},"BorrowerNames": {"L": [{"S": "aa INC."}, {"S": "dd, INC,"}]}}}
        """
        return {"Item": {"Word": {"S": self.word}, "BorrowerNames": {"L": [{"S": x} for x in self.orgs]}}}


def write_compress_s3_object(s3, data):
    prefix = data["BorrowerName"]
    buffer = BytesIO()
    with gzip.GzipFile(mode="wb", fileobj=buffer) as f:
        f.write(json.dumps(data).encode("utf-8"))

    s3.put_object(
        Bucket=s3_bucket,
        Key=f"{s3_base_org_key}/{prefix}.json.gz",
        Body=buffer.getvalue(),
        ContentEncoding="gzip",
        ContentType="application/json",
    )


def upload_rows_concurrently(s3, rows):
    with ThreadPoolExecutor(max_workers=max_worker_count) as executor:
        futures = {executor.submit(write_compress_s3_object, s3, row): row for idx, row in enumerate(rows)}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Failed to upload row: {e}")


def get_offset_from_db(fn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT offset FROM tracking WHERE fn = '{fn}'")
    row = cursor.fetchone()
    if row:
        return row[0] or 0
    else:
        return 0


def get_completed_from_db(fn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT completed FROM tracking WHERE fn = '{fn}'")
    row = cursor.fetchone()
    return True if row else False


def get_completed_fns_from_db():
    cursor = conn.cursor()
    cursor.execute(f"SELECT fn FROM tracking WHERE completed = 1")
    rows = cursor.fetchall()
    return [x[0] for x in rows]


def upsert_offset_in_db(fn, offset):
    cursor = conn.cursor()
    cursor.execute(f"SELECT offset FROM tracking WHERE fn = '{fn}'")
    row = cursor.fetchone()
    if row:
        conn.execute(f"UPDATE tracking SET offset = {offset} WHERE fn = '{fn}'")
    else:
        conn.execute(f"INSERT INTO tracking (fn, offset) VALUES ('{fn}', {offset})")
    conn.commit()


def upsert_completed_in_db(fn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT completed FROM tracking WHERE fn = '{fn}'")
    row = cursor.fetchone()
    if row:
        conn.execute(f"UPDATE tracking SET completed = 1 WHERE fn = '{fn}'")
    else:
        conn.execute(f"INSERT INTO tracking (fn, completed) VALUES ('{fn}', 1)")
    conn.commit()


def get_worker_fn_from_db(worker_id):
    cursor = conn.cursor()
    cursor.execute(f"SELECT fn FROM tracking WHERE worker = ? and (completed is null or completed != 1)", (worker_id,))
    row = cursor.fetchone()
    return row[0] if row else None


def get_assigned_worker_for_fn(fn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT worker FROM tracking WHERE fn = ? and (completed is null or completed != 1)", (fn,))
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_worker_fn_in_db(worker_id, fn):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT fn FROM tracking WHERE fn = ?",
        (fn,),
    )
    row = cursor.fetchone()
    if row:
        conn.execute(
            f"UPDATE tracking SET worker = ? WHERE fn = ?",
            (
                worker_id,
                fn,
            ),
        )
    else:
        conn.execute(
            f"INSERT INTO tracking (worker, fn) VALUES (?,?)",
            (
                worker_id,
                fn,
            ),
        )
    conn.commit()


@app.command()
def getcompletedorgs():
    print(get_completed_fns_from_db())


@app.command()
def getorgstats():
    cursor = conn.cursor()
    cursor.execute("SELECT fn,offset,completed,worker FROM tracking")
    rows = cursor.fetchall()
    print("offset\tfin\twkr\tfn")
    for row in rows:
        print(f"{row[1]}\t{row[2]==1}\t{row[3]}\t{row[0]}")


@app.command()
def loadorgs(chunk_size: int = 3840, speed_threshold: int = 70, skip_files: str = "", worker_id: int = 0):
    halt_fn = "halt.flg"
    if os.path.exists(halt_fn):
        print("halt flag found, exiting")
        sleep(100)
        exit(0)
    print(f"loading orgs with chunksize {chunk_size} and speed threshold {speed_threshold} for worker {worker_id}")

    # check if worker is already assigned a file
    fn = get_worker_fn_from_db(worker_id)
    if fn is not None:
        print(f"worker {worker_id} already assigned {fn} - processing...")
        process_fn(chunk_size, speed_threshold, fn)
    else:
        skip_files = skip_files.split(",")
        for fn in [
            os.path.join(data_folder, x) for x in os.listdir(data_folder) if x.endswith(".csv") and x not in skip_files
        ]:
            if fn in get_completed_fns_from_db():
                print("Skipping", fn)
                continue
            # check to see if this fn is already assigned to a worker
            if get_assigned_worker_for_fn(fn) is not None:
                print(f"Skipping {fn} - already assigned to another worker")
                continue
            print(f"Assigned {fn} to worker {worker_id} - processing...")
            upsert_worker_fn_in_db(worker_id, fn)
            process_fn(chunk_size, speed_threshold, fn)
    sleep(10)


def process_fn(chunk_size, speed_threshold, fn):
    print("Starting", fn)
    count = 1
    with open(fn, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        start = time()
        rows = []
        s3 = boto3.client("s3")
        previous_offset = get_offset_from_db(fn)
        for idx, row in enumerate(reader):
            if os.path.exists("stop.flg"):
                print("Stop flag found, exiting")
                exit(0)
            if idx <= previous_offset:
                start = time()
                continue

            # write row to compressed s3 file
            row["schema"] = "ppp"
            rows.append(row)

            if count % chunk_size == 0:
                upload_rows_concurrently(s3, rows)

                duration = time() - start
                if duration > 0:
                    print(f"{chunk_size/duration} records/sec - uploaded so far: {idx}")
                    upsert_offset_in_db(fn, idx)
                    previous_offset = idx

                    if chunk_size / duration < speed_threshold:
                        print("too slow, exiting")
                        exit(0)
                    if loop_sleep_time:
                        sleep(loop_sleep_time)
                    start = time()
                    count = 0

            count += 1
    print("Finished", fn)
    upsert_completed_in_db(fn)


@app.command()
def prepload():
    base2 = {}
    processed_fn = "processed.json"
    processed = []
    if os.path.exists(processed_fn):
        with open(processed_fn, "r") as f:
            processed = json.load(f)
    for top_folder in os.listdir(word_folder):
        prefix = top_folder[0:2]
        if prefix not in base2:
            base2[prefix] = []
        folder = os.path.join(word_folder, top_folder)
        if not os.path.exists(folder) or not os.path.isdir(folder):
            continue
        for fn in [os.path.join(folder, x) for x in os.listdir(folder) if x and x.endswith(".json")]:
            base2[prefix].append(fn)

    for prefix in [x for x in base2 if x not in processed]:
        start = time()
        data = []

        for fn in base2[prefix]:
            word = os.path.basename(fn).split(".")[0]
            with open(fn, "r") as f:
                orgs = json.load(f)
            data.append(json.dumps(Item(word, orgs).__dict__()))

        # write data to s3 in compressed format
        buffer = BytesIO()
        with gzip.GzipFile(mode="wb", fileobj=buffer) as f:
            f.write("\n".join(data).encode("utf-8"))

        s3.put_object(
            Bucket=s3_bucket,
            Key=f"{s3_base_key}/{prefix}.json.gz",
            Body=buffer.getvalue(),
            ContentEncoding="gzip",
            ContentType="application/json",
        )

        # with open(os.path.join(staging_folder, f"{prefix}.json"), "w") as f:
        #     json.dump(data, f)
        print("wrote", prefix)
        processed.append(prefix)
        with open(processed_fn, "w") as f:
            json.dump(processed, f)
        print(f"{len(data)/(time()-start)} records/sec")


@app.command()
def consolidate():
    for fn in [os.path.join(batch_folder, x) for x in os.listdir(batch_folder) if x.endswith(".json")]:
        print("Starting", fn)
        words = {}
        rec_count = 0
        start = time()
        with open(fn, "r") as f:
            data = json.load(f)
            rec_count = len(data)
            for row in data:
                word = row[0].lower()
                org = row[1]
                if word not in words:
                    words[word] = []
                words[word].append(org)

        for word in words:
            base = os.path.join(word_folder, word[0:3])
            os.makedirs(base, exist_ok=True)
            word_fn = os.path.join(base, f"{word}.json")
            word_data = []
            if os.path.exists(word_fn):
                with open(word_fn, "r") as f:
                    word_data = json.load(f)
            word_data.extend(words[word])
            with open(word_fn, "w") as f:
                json.dump(list(set(word_data)), f)

        os.rename(fn, f"{fn}.done")
        print(rec_count / (time() - start), "records/sec")
        print("Finished", fn)


@app.command()
def allocatewords(worker_count: int = 10):
    cursor = conn_word.cursor()
    cursor.execute("SELECT COUNT(1) FROM words WHERE completed is null and worker is null")
    row = cursor.fetchone()
    total_unallocated = row[0]
    rows_per_worker = total_unallocated // worker_count

    for worker_id in range(5, worker_count):
        print("allocating words for worker", worker_id)

        # select the range of unallocated words for this worker
        cursor.execute(f"SELECT word FROM words WHERE completed is null and worker is null LIMIT {rows_per_worker}")
        rows = cursor.fetchall()
        words = [x[0] for x in rows]
        if len(words) == 0:
            print("no words found for worker", worker_id)
            continue
        # update the worker column for these words in batches of size 999 until complete
        for i in range(0, len(words), 999):
            cursor.execute(
                f"UPDATE words SET worker = {worker_id} WHERE word in ({','.join(['?']*len(words[i:i+999]))})",
                words[i : i + 999],
            )
            conn_word.commit()
            print("allocated", len(words[i : i + 999]), "words for worker", worker_id)


def write_error(msg, worker_id):
    # write error to words.db
    print(msg, worker_id)
    cursor = conn_word.cursor()
    cursor.execute("INSERT INTO errors (msg, worker) VALUES (?,?)", (msg, worker_id))
    conn_word.commit()


@app.command()
def fixwords(chunk_size: int = 1000, worker_id: int = 0):
    print(f"fixing words with chunksize {chunk_size} for worker {worker_id}")
    halt_fn = "halt.flg"
    if os.path.exists(halt_fn):
        print("halt flag found, exiting")
        sleep(100)
        exit(0)
    # check to see if any words have completed is null
    cursor = conn_word.cursor()

    # get the chunk of target words
    cursor.execute(
        f"SELECT word FROM words WHERE (completed=0 or completed is null) and worker=? LIMIT {chunk_size}",
        (worker_id,),
    )
    rows = cursor.fetchall()
    target_words = [x[0] for x in rows]
    if not target_words:
        print("No words to fix for worker", worker_id)
        exit(0)
    s3 = boto3.client("s3")
    start = time()
    with engine.connect() as conn:
        for word in target_words:
            # get data from postgres db engine for this word
            sql = """
                select o.* 
                from word w
                join organization_words ow 
                    on ow.word_id=w.id
                join organization o 
                    on ow.organization_id=o.id
                where w.word = :value
            """
            orgs = []

            result = conn.execute(text(sql), {"value": word})
            for row in result:
                orgs.append(row["name"].upper())

            if not orgs:
                write_error(f"no orgs found for word {word}", worker_id)
                continue
            data = dict(word=word, organizations=orgs)
            print(data)
            # save new_data back to s3 in compressed format
            # buffer = BytesIO()
            # with gzip.GzipFile(mode="wb", fileobj=buffer) as f:
            #     f.write(json.dumps(data).encode("utf-8"))
            # s3_fn = f"{s3_base_key}/{word}.json.gz"
            # s3.put_object(
            #     Bucket=s3_bucket,
            #     Key=s3_fn,
            #     Body=buffer.getvalue(),
            #     ContentEncoding="gzip",
            #     ContentType="application/json",
            # )
            # # update completed status for this word in db
            # cursor.execute("UPDATE words SET completed = 1 WHERE word = ?", (word,))
            # conn_word.commit()

    print(f"{len(target_words)/(time()-start)} records/sec")


if __name__ == "__main__":
    app()

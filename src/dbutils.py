from models import db, Organization, Word, generate_id
from csv import DictReader, DictWriter
import json
import os
from time import time
from redis import StrictRedis
from itertools import zip_longest
import arrow


class WordOrganizationIndexer:
    def __init__(self):
        self.stop_words = [
            "inc.",
            "inc",
            "llc",
            "llp",
            "ltd.",
            "ltd",
            "co.",
            "co",
            "corp.",
            "corp",
            "corporation",
            "corporation.",
            "company",
            "company.",
            "group",
            "group.",
            "association",
            "association.",
            "society",
            "society.",
            "&",
            "and",
            "the",
            "of",
            "for",
            "to",
            "in",
            "on",
            "at",
            "by",
            "with",
            "from",
            "as",
            "a",
            "an",
            "d.b.a.",
            "dba",
            "&#x26;",
            "n",
        ]
        self.word_org_index_fn = "data/word_org_index.json"
        self.load_redis()
        self.redis = StrictRedis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        self.org_name_csv_folder = "data/orgs"

    def load_redis(self):
        if os.path.exists(self.word_org_index_fn):
            self.redis.flushdb()
            self.redis.hmset("word_org_index", json.load(open(self.word_org_index_fn, "r")))

    def save_redis(self):
        json.dump(self.redis.hgetall("word_org_index"), open(self.word_org_index_fn, "w"))

    def get_org_words(self, name, dba, existing_words=[]):
        words = [x.lower().strip() for x in set(name.split(" ") + dba.split(" "))]
        words = [x for x in words if x and x not in self.stop_words and len(x) > 1 and x not in existing_words]
        replace_chars = [".", ",", ";", ":", "!", "?", "(", ")", "-", "_", "/", "\\", "'", '"']
        for char in replace_chars:
            words = [x.replace(char, "") for x in words]

        return words

    def extract_word_org_index(self):
        print(f"Extracting word-org index from files in {self.org_name_csv_folder}...")
        for fn in [
            os.path.join(self.org_name_csv_folder, x)
            for x in os.listdir(self.org_name_csv_folder)
            if x.endswith(".csv")
        ]:
            print(fn)
            if fn in self.redis.smembers("processed_files"):
                continue
            s = time()
            count = 0
            with open(fn, "r") as f:
                reader = DictReader(f, fieldnames=["id", "name", "dba"])
                for row in reader:
                    for word in self.get_org_words(row["name"], row["dba"]):
                        self.redis.sadd(f"word_{word}", str(row["id"]))
                    count += 1
            self.redis.sadd("processed_files", fn)
            print(count / (time() - s), "records per second")

    def batcher(self, iterable, n):
        args = [iter(iterable)] * n
        return zip_longest(*args)

    def build_word_output(self):
        word_csv_fn = "data/word.csv"
        org_word_csv_fn = "data/org_word.csv"

        word_csv_fieldnames = ["id", "created_at", "updated_at", "word"]
        org_word_csv_fieldnames = ["organization_id", "word_id"]

        if not os.path.exists(word_csv_fn):
            with open(word_csv_fn, "w") as f:
                writer = DictWriter(f, fieldnames=word_csv_fieldnames)
                writer.writeheader()

        if not os.path.exists(org_word_csv_fn):
            with open(org_word_csv_fn, "w") as f:
                writer = DictWriter(f, fieldnames=org_word_csv_fieldnames)
                writer.writeheader()

        for keybatch in self.batcher(self.redis.scan_iter("word_*"), 10000):
            words_batch = []
            org_words_batch = []

            processed_keys = []
            for key in keybatch:
                if not key:
                    continue
                if "_" not in key.decode("utf-8"):
                    continue
                word = key.decode("utf-8").split("_")[1]
                processed_key = f"processed:{word}"
                if self.redis.exists(processed_key):
                    continue

                org_ids = self.redis.smembers(key)
                org_ids = [x.decode("utf-8") for x in org_ids]
                word_id = generate_id()
                words_batch.append(
                    {
                        "id": word_id,
                        "created_at": arrow.utcnow().datetime,
                        "updated_at": arrow.utcnow().datetime,
                        "word": word,
                    }
                )
                for org_id in org_ids:
                    org_words_batch.append({"organization_id": org_id, "word_id": word_id})

                processed_keys.append(processed_key)

            with open(word_csv_fn, "a") as f:
                writer = DictWriter(f, fieldnames=word_csv_fieldnames)
                writer.writerows(words_batch)

            with open(org_word_csv_fn, "a") as f:
                writer = DictWriter(f, fieldnames=org_word_csv_fieldnames)
                writer.writerows(org_words_batch)

            for processed_key in processed_keys:
                self.redis.set(processed_key, 1)

            print(f"Processed {len(words_batch)} words")

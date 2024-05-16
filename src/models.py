import json
import gzip
from hashlib import sha256
import os
from random import choice
from string import ascii_lowercase, digits
from uuid import uuid4
from time import time
from urllib.parse import quote
from pydantic import BaseModel
from pydantic import validator
import boto3
from flask import current_app

from config import config

s3_config = dict(
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_DEFAULT_REGION,
)

if os.getenv("LOCALONLY", "FALSE").upper() == "TRUE":
    s3_config["endpoint_url"]=os.getenv("LOCALS3URL", "http://localhost:9000")
    # s3_config["config"]=boto3.session.Config(signature_version='v4'),
    s3_config["verify"]=False

s3 = boto3.client(
    "s3",
    **s3_config
)


class FileSystemCache:
    def __init__(self):
        self.cache_dir = config.CACHE_FOLDER

    def get(self, key):
        fn = os.path.join(self.cache_dir, key)
        if os.path.exists(fn):
            with open(fn, "r") as f:
                return f.read()
        return None

    def set(self, key, value):
        fn = os.path.join(self.cache_dir, key)
        dirname = os.path.dirname(fn)
        os.makedirs(dirname, exist_ok=True)
        with open(fn, "w") as f:
            f.write(value)

    def delete(self, key):
        fn = os.path.join(self.cache_dir, key)
        if os.path.exists(fn):
            os.remove(fn)
        # if there are no more files in the directory, remove the directory
        dirname = os.path.dirname(fn)
        if os.path.exists(dirname) and len(os.listdir(dirname)) == 0:
            os.rmdir(dirname)


r = FileSystemCache()


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
    "llc",
    "inc",
]


def generate_random_id(length=12):
    return "".join([choice(ascii_lowercase + digits) for _ in range(length)])


class PhysicalAddressProfile(BaseModel):
    street1: str
    street2: str = None
    city: str
    state: str
    postal_code: str
    country: str
    is_default: bool = True

    def items(self):
        return self.__dict__.items()

    @property
    def signature(self) -> str:
        h = sha256()
        h.update(self.street1.encode("utf-8"))
        h.update(self.street2.encode("utf-8"))
        h.update(self.city.encode("utf-8"))
        h.update(self.state.encode("utf-8"))
        h.update(self.postal_code.encode("utf-8"))
        h.update(self.country.encode("utf-8"))
        return h.hexdigest()


class TelephoneNumberProfile(BaseModel):
    phone: str
    is_default: bool = True

    @property
    def signature(self) -> str:
        h = sha256()
        h.update(self.phone.encode("utf-8"))
        return h.hexdigest()


class SocialMediaAccountProfile(BaseModel):
    handle: str
    platform: str

    @property
    def signature(self):
        return f"{self.platform}-{self.handle}"


class EmailAddressProfile(BaseModel):
    email: str
    is_default: bool = True

    @property
    def signature(self):
        return self.email


class ChangeEventProfile(BaseModel):
    change: str
    change_date: str
    changed_by_user: str
    social_rating_change: int = 0
    antisocial_rating_change: int = 0


class SocialMediaAccountProfileDiff(BaseModel):
    adds: list[SocialMediaAccountProfile] = []
    deletes: list[SocialMediaAccountProfile] = []
    update_class: str = "SocialMediaAccounts"

    @property
    def has_diff(self):
        return (len(self.adds) + len(self.deletes)) > 0


class EmailAddressProfileDiff(BaseModel):
    adds: list[EmailAddressProfile] = []
    deletes: list[EmailAddressProfile] = []
    update_class: str = "EmailAddresses"

    @property
    def has_diff(self):
        return (len(self.adds) + len(self.deletes)) > 0


class UserProfileDiff(BaseModel):
    social_media_accounts: SocialMediaAccountProfileDiff = SocialMediaAccountProfileDiff()
    email_addresses: EmailAddressProfileDiff = EmailAddressProfileDiff()

    @property
    def diff_objs(self):
        return [self.social_media_accounts, self.email_addresses]

    @property
    def has_diff(self):
        return self.social_media_accounts.has_diff or self.email_addresses.has_diff


class UserProfile(BaseModel):
    # generate random uid
    uid: str  # sso_id or random non-sso_id
    name: str  # username
    given_name: str = None
    family_name: str = None
    middle_name: str = None
    nickname: str = None
    birthdate: str = None
    pronouns: str = None
    user_since: str = None
    is_premium_user: bool = False
    is_blocked: bool = True  # default is users are blocked until they are authenticated
    block_reason: str = None
    social_rating: int = 0
    antisocial_rating: int = 0
    is_active: bool = False  # default is inactive until they are authenticated
    physical_addresses: list[PhysicalAddressProfile] = []
    telephone_numbers: list[TelephoneNumberProfile] = []
    social_media_accounts: list[SocialMediaAccountProfile] = []
    email_addresses: list[EmailAddressProfile] = []
    change_events: list[ChangeEventProfile] = []
    raw_data: dict = {}
    tags: list[str] = []
    groups: list[str] = []
    organizations: list[str] = []
    last_login: str = None
    create_method: str = "sso"  # sso or manual

    @property
    def is_admin(self):
        return "Admins" in self.groups

    class Config:
        arbitrary_types_allowed = True

    def compare_to(self, compare_profile):
        # compares a profile to another profile and returns a list of changes
        # we currently don't care about indexing physical addresses and telephone numbers
        # so we are not comparing at this time

        diff = UserProfileDiff()
        # compare social media accounts
        for account in self.social_media_accounts:
            if account.signature not in [x.signature for x in compare_profile.social_media_accounts]:
                diff.social_media_accounts.adds.append(account)
        for account in compare_profile.social_media_accounts:
            if account.signature not in [x.signature for x in self.social_media_accounts]:
                diff.social_media_accounts.deletes.append(account)
        # compare email addresses
        for email in self.email_addresses:
            if email.signature not in [x.signature for x in compare_profile.email_addresses]:
                diff.email_addresses.adds.append(email)
        for email in compare_profile.email_addresses:
            if email.signature not in [x.signature for x in self.email_addresses]:
                diff.email_addresses.deletes.append(email)

        return diff


class OrganizationProfile(BaseModel):
    name: str
    is_premium_user: bool = False
    social_rating: int = 0
    antisocial_rating: int = 0
    is_active: bool = True
    profile_type: str = "antisocial_credit"
    physical_addresses: list[PhysicalAddressProfile] = []
    telephone_numbers: list[TelephoneNumberProfile] = []
    social_media_accounts: list[SocialMediaAccountProfile] = []
    email_addresses: list[EmailAddressProfile] = []
    change_events: list[ChangeEventProfile] = []
    raw_data: dict = {}
    tags: list[str] = []

    class Config:
        arbitrary_types_allowed = True


class PPPOrganizationProfile(OrganizationProfile):
    def __init__(self, data: dict):
        try:
            forgiveness_amount = int(float(data.get("ForgivenessAmount")))
        except Exception as e:
            forgiveness_amount = 0

        kwargs = dict(
            name=data.get("BorrowerName"),
            is_premium_user=False,
            social_rating=0,
            antisocial_rating=forgiveness_amount,
            is_active=True,
            profile_type="antisocial_credit",
            raw_data=data,
            tags=["ppp"],
        )
        super().__init__(**kwargs)

        if "BorrowerAddress" in data:
            address_kwargs = dict(
                street1=data.get("BorrowerAddress"),
                street2=None,
                city=data.get("BorrowerCity"),
                state=data.get("BorrowerState"),
                postal_code=data.get("BorrowerZip"),
                country="USA",
                is_default=True,
            )
            address = PhysicalAddressProfile(**address_kwargs)
            self.physical_addresses.append(address)
        self.change_events.append(
            ChangeEventProfile(
                change="PPP Forgiveness",
                change_date=data.get("ForgivenessDate"),
                changed_by_user="system",
                social_rating_change=0,
                antisocial_rating_change=forgiveness_amount,
            )
        )


class AccessTokenModel(BaseModel):
    jti: str
    token_type: str
    access_token: str
    access_type: str
    refresh_token: str
    id_token: str
    expires_in: int
    scope: str
    created_by_user: int
    access_type: str
    expiration_datetime: int

    class Config:
        arbitrary_types_allowed = True


class GroupModel(BaseModel):
    name: str


class DAO:
    def __init__(self, object_group: str):
        self.object_group = object_group

    def get(self, name: str):
        # load from s3
        key = f"{config.AWS_S3_BASE_KEY}/{self.object_group}/{name}.json.gz"
        data = r.get(key)
        if data:
            return json.loads(data)

        # retrieve, unzip with gzip, and load json
        try:
            obj = s3.get_object(Bucket=config.AWS_S3_BUCKET_NAME, Key=key)
        except s3.exceptions.NoSuchKey as e:
            current_app.logger.debug(f"No key found at {config.AWS_S3_BUCKET_NAME}/{key}: {e}")
            return None
        except Exception as e:
            current_app.logger.error(f"Error loading {self.object_group} {name}: {e} ({e.__class__.__name__})")
            return None
        try:
            data = json.loads(gzip.decompress(obj["Body"].read()).decode("utf-8"))
        except Exception as e:
            current_app.logger.error(f"Error loading {self.object_group} {name}: {e}")
            return None
        # add to cache
        r.set(key, json.dumps(data))
        return data

    def ls(self):
        # list the objects in the group
        key = f"{config.AWS_S3_BASE_KEY}/{self.object_group}/"
        results = []
        try:
            continuation_token = None

            while True:
                list_kwargs = {"Bucket": config.AWS_S3_BUCKET_NAME, "Prefix": key}
                if continuation_token:
                    list_kwargs["ContinuationToken"] = continuation_token

                result = s3.list_objects_v2(**list_kwargs)

                if "Contents" in result:
                    results.extend(result["Contents"])

                if not result.get("IsTruncated"):  # Stop if no more objects
                    break

                continuation_token = result["NextContinuationToken"]
        except Exception as e:
            current_app.logger.error(f"Error listing {self.object_group} {self.name}: {e}")
            return None
        return results

    def rm(self, name: str):
        key = f"{config.AWS_S3_BASE_KEY}/{self.object_group}/{name}.json.gz"
        try:
            s3.delete_object(Bucket=config.AWS_S3_BUCKET_NAME, Key=key)
        except Exception as e:
            current_app.logger.error(f"Error deleting {self.object_group} {name}: {e}")
            return False
        # invalidate cache
        r.delete(key)
        # update count
        self.update_metadata_key("count", len(self.ls()))
        return True

    def update(self, name: str, data: dict):
        key = f"{config.AWS_S3_BASE_KEY}/{self.object_group}/{name}.json.gz"
        existing_data = self.get(name)
        if existing_data:
            existing_data.update(data)
            data = existing_data
        # save to s3
        try:
            s3.put_object(
                Bucket=config.AWS_S3_BUCKET_NAME,
                Key=key,
                Body=gzip.compress(json.dumps(data).encode("utf-8")),
            )
        except Exception as e:
            current_app.logger.error(f"Error saving {self.object_group} {name}: {e}")
            return False
        # invalidate cache
        r.delete(key)

        # update count
        if existing_data is None:  # increment if new
            self.update_metadata_key("count", self.count() + 1)
        return True

    def load_metadata(self):
        key = f"{config.AWS_S3_BASE_KEY}/metadata/{self.object_group}.json"
        # retrieve from cache if it exists
        data = r.get(key)
        if data:
            return json.loads(data)

        try:
            obj = s3.get_object(Bucket=config.AWS_S3_BUCKET_NAME, Key=key)
        except Exception as e:
            current_app.logger.error(f"Error loading {self.object_group} metadata: {e}")
            return None
        try:
            data = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception as e:
            current_app.logger.error(f"Error loading {self.object_group} metadata: {e}")
            return None
        # add to cache
        r.set(key, json.dumps(data))
        return data

    def update_metadata(self, data):
        key = f"{config.AWS_S3_BASE_KEY}/metadata/{self.object_group}.json"
        try:
            s3.put_object(
                Bucket=config.AWS_S3_BUCKET_NAME,
                Key=key,
                Body=json.dumps(data).encode("utf-8"),
            )
        except Exception as e:
            current_app.logger.error(f"Error saving {self.object_group} metadata: {e}")
            return False
        # invalidate cache
        r.delete(key)
        return True

    def count(self):
        metadata = self.load_metadata()
        if metadata and "count" in metadata:
            return metadata["count"]
        if not metadata:
            metadata = {}
        self.update_metadata_key("count", len(self.ls()))
        return self.count()

    def update_metadata_key(self, key, value):
        metadata = self.load_metadata()
        if metadata is None:
            metadata = {}

        metadata[key] = value
        self.update_metadata(metadata)
        return metadata


class Grouping:
    def __init__(self):
        self.dao = None

    def get(self, name):
        raise Exception("Not implemented")

    def ls(self):
        objs = self.dao.ls()
        return [x["Key"].split("/")[-1].split(".")[0] for x in objs]

    def count(self):
        return self.dao.count()

    def update(self, profile: BaseModel):
        raise Exception("Not implemented")


class Organizations(Grouping):
    def __init__(self):
        self.dao = DAO("organizations")

    def get(self, name):
        data = self.dao.get(name)
        if not data:
            return None
        if "schema" in data and data["schema"].lower() == "ppp":
            data = PPPOrganizationProfile(data)
        else:
            data = OrganizationProfile(**data)
        return data

    def ls(self):
        objs = self.dao.ls()
        return [x["Key"].split("/")[-1].split(".")[0] for x in objs]

    def count(self):
        return self.dao.count()

    def update(self, organization: OrganizationProfile):
        name = organization.name
        data = organization.dict()

        existing_org = self.get(name)
        increment_count = False
        if not existing_org:
            increment_count = True

        # update the word indices
        for word in list(set([x for x in name.split(" ") if x not in stop_words])):
            word_data = Words().get(word)
            if not word_data:
                word_data = dict(word=word, organizations=[])
            if name not in word_data["organizations"]:
                word_data["organizations"].append(name)
            Words().update(word, word_data)

        return self.dao.update(name, data)


class Users(Grouping):
    def __init__(self):
        self.dao = DAO("users")

    def get(self, name: str) -> UserProfile:
        data = self.dao.get(name)
        if data:
            return UserProfile(**data)
        return None

    def update(self, user_profile: UserProfile):
        # update the indices for social media accounts and email addresses
        classmap = {
            "social_media_accounts": SocialMediaAccounts,
            "email_addresses": EmailAddresses,
        }
        existing_user_profile = self.get(user_profile.name)
        if existing_user_profile:
            diff = user_profile.compare_to(existing_user_profile)
            if diff.social_media_accounts.has_diff:
                for add in diff.social_media_accounts.adds:
                    SocialMediaAccounts().add(add.platform, add.handle, user_profile.name)
                for delete in diff.social_media_accounts.deletes:
                    SocialMediaAccounts().rm(delete.platform, delete.handle)
            if diff.email_addresses.has_diff:
                for add in diff.email_addresses.adds:
                    EmailAddresses().add(add.email, user_profile.name)
                for delete in diff.email_addresses.deletes:
                    EmailAddresses().rm(delete.email)

        # update the tag indices
        existing_tags = set(existing_user_profile.tags) if existing_user_profile else set()
        new_tags = set(user_profile.tags)
        print(f"existing_tags: {existing_tags}")
        print(f"new_tags: {new_tags}")
        for tag in new_tags - existing_tags:
            Tags().add_profile(tag, user_profile.name)
        for tag in existing_tags - new_tags:
            Tags().rm_profile(tag, user_profile.name)

        # update the user profile
        name = user_profile.name
        data = user_profile.dict()
        return self.dao.update(name, data)

    def delete(self, user_profile: UserProfile):
        # delete the user profile
        name = user_profile.name
        return self.dao.rm(name)

    def users(self):
        # loads all users
        return [self.get(x) for x in self.ls()]


class AccessTokens(Grouping):
    def __init__(self):
        self.dao = DAO("access_tokens")

    def get(self, name: str) -> AccessTokenModel:
        data = self.dao.get(name)
        if data:
            return AccessTokenModel(**data)
        return None

    def ls(self):
        return self.dao.ls()

    def update(self, access_token):
        name = access_token.jti
        data = access_token.dict()
        return self.dao.update(name, data)

    def rm(self, name: str):
        return self.dao.rm(name)


class BlockedUsers(Grouping):
    def __init__(self) -> None:
        self.dao = DAO("blocked_users")

    def get(self, name: str) -> UserProfile:
        data = self.dao.get(name)
        if data:
            return UserProfile(**data)
        return None

    def is_blocked(self, name: str) -> bool:
        return self.dao.get(name) is not None

    def ls(self):
        return self.dao.ls()


class SocialMediaAccounts(Grouping):
    def __init__(self):
        self.dao = DAO("social_media_accounts")

    def name(self, platform: str, handle: str) -> str:
        return ":".join([platform, handle])

    def get(self, platform: str, handle: str) -> SocialMediaAccountProfile:
        data = self.dao.get(self.name(platform, handle))
        if data:
            return SocialMediaAccountProfile(platform=platform, handle=handle)
        return None

    def get_profile(self, platform: str, handle: str, profile_type: str):
        data = self.dao.get(self.name(platform, handle))
        if data and data.get("profile_name", None):
            profile_name = data.get("profile_name")
            if profile_type == "user":
                return Users().get(profile_name)
            elif profile_type == "org":
                return Organizations().get(profile_name)
            else:
                raise Exception(f"Unknown profile type {profile_type}")
        return None

    def rm(self, platform: str, handle: str) -> bool:
        return self.dao.rm(self.name(platform, handle))

    def add(self, platform: str, handle: str, profile_name: str) -> bool:
        data = dict(
            profile_name=profile_name,
            platform=platform,
            handle=handle,
        )
        return self.dao.update(self.name(platform, handle), data)


class EmailAddresses(Grouping):
    def __init__(self):
        self.dao = DAO("email_addresses")

    def get(self, email: str) -> EmailAddressProfile:
        data = self.dao.get(email)
        if data:
            return EmailAddressProfile(email=email)
        return None

    def get_profile(self, email: str, profile_type: str):
        email_data = self.dao.get(email)
        if email_data and email_data.get("profile_name", None):
            profile_name = email_data.get("profile_name")
            if profile_type == "user":
                return Users().get(profile_name)
            elif profile_type == "org":
                return Organizations().get(profile_name)
            else:
                raise Exception(f"Unknown profile type {profile_type}")
        return None

    def add(self, email: str, profile_name: str) -> bool:
        data = dict(
            profile_name=profile_name,
            email=email,
        )
        return self.dao.update(email, data)

    def rm(self, email: str) -> bool:
        return self.dao.rm(email)


class SocialMediaPlatforms(Grouping):
    def __init__(self):
        self.dao = DAO("social_media_platforms")

    def platforms(self):
        metadata = self.dao.load_metadata()
        if metadata and "platforms" in metadata:
            return metadata["platforms"]


class Words(Grouping):
    def __init__(self):
        self.dao = DAO("words")

    def get(self, word: str) -> dict:
        data = self.dao.get(word)
        if data:
            return data
        return None

    def update(self, word: str, data: dict):
        self.dao.update(word, data)


class Tags(Grouping):
    def __init__(self):
        self.dao = DAO("tags")

    def get(self, name: str) -> dict:
        data = self.dao.get(name)
        if data:
            return data
        return None

    def update(self, name: str, data: dict):
        self.dao.update(name, data)

    def add(self, name: str, description: str = None) -> bool:
        tag_data = self.get(name)
        if tag_data is None:
            tag_data = dict(name=name, profiles=[])
            if description:
                tag_data["description"] = description

    def add_profile(self, name: str, profile_name: str) -> bool:
        tag_data = self.get(name)
        if profile_name not in tag_data["profiles"]:
            tag_data["profiles"].append(profile_name)
            self.dao.update(name, tag_data)

    def rm(self, name: str) -> bool:
        return self.dao.rm(name)

    def rm_profile(self, name: str, profile_name: str) -> bool:
        tag_data = self.get(name)
        if tag_data is None:
            return False

        if profile_name in tag_data["profiles"]:
            tag_data["profiles"].remove(profile_name)
            self.dao.update(name, tag_data)

    def tags(self):
        return [self.get(x) for x in self.ls()]


def initS3():
    files = [
        f"{x}.json" for x in ["users", "organizations", "access_tokens", "social_media_platforms", "blocked_users"]
    ]
    for file in files:
        # check file exists on s3, if not, copy it from local if it exists
        key = f"{config.AWS_S3_BASE_KEY}/metadata/{file}"
        try:
            obj = s3.get_object(Bucket=config.AWS_S3_BUCKET_NAME, Key=key)
        except Exception as e:
            # not present, copy from local
            obj = None
        source_fn = os.path.join(config.METADATA_SOURCE_FOLDER, file)
        if obj is None and os.path.exists(source_fn):
            # copy from local to s3
            s3.upload_file(source_fn, config.AWS_S3_BUCKET_NAME, key)

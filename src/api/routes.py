from datetime import timedelta
import logging
import arrow
from flask import (
    Blueprint,
    render_template,
    url_for,
    redirect,
    request,
    session,
    send_file,
    abort,
    jsonify,
    g,
)
from flask_jwt_extended import jwt_required


from app import app, config, csrf
from models import (
    AccessTokens,
    Organizations,
)
from auth.utils import jwt_has_access

mod = Blueprint("api", __name__, url_prefix="/api")

memoized_tags = dict()


# info route
@mod.route("/info")
def info():
    return jsonify(
        dict(
            name=config.APP_NAME,
        )
    )


# protected route
@mod.route("/has_write_access")
@jwt_has_access("write")
def protected_write():
    return jsonify(
        dict(
            name=config.APP_NAME,
            access="write",
        )
    )


# protected route
@mod.route("/has_read_access")
@jwt_has_access("read")
def protected_read():
    return jsonify(dict(name=config.APP_NAME, access="read"))


# # route to insert and organization
# @csrf.exempt
# @mod.route("/organization", methods=["POST"])
# @jwt_has_access("write")
# def api_insert_organization():
#     try:
#         # if db.session is not in transaction, begin one
#         if not db.session.is_active:
#             db.session.begin()
#         insert_organization(request.json)
#         db.session.commit()
#     except Exception as e:
#         logging.exception(e)
#         db.session.rollback()
#         return {"status": "error", "message": str(e)}
#     finally:
#         db.session.close()
#     return {"status": "success"}


# # route to get stats from database
# @mod.route("/stats", methods=["GET"])
# @jwt_has_access("read")
# def api_get_stats():
#     data = dict(organization_count=Organization.query.count(), user_count=User.query.count())
#     return jsonify(data)


# @csrf.exempt
# @mod.route("/organizations", methods=["POST"])
# @jwt_has_access("write")
# def api_insert_organizations():
#     try:
#         # if db.session is not in transaction, begin one
#         if not db.session.is_active:
#             db.session.begin()
#         with db.session.no_autoflush:
#             for org in request.json:
#                 insert_organization(org)
#         db.session.commit()
#     except Exception as e:
#         logging.exception(e)
#         db.session.rollback()
#         return {"status": "error", "message": str(e)}
#     finally:
#         db.session.close()
#     return {"status": "success"}


# def insert_organization(data):
#     """
#     looking for the following json structure
#     {
#         "name": "name",
#         "dba": "dba",
#         "street1": "street1",
#         "street2": "street2",
#         "city": "city",
#         "state": "state",
#         "postal_code": "postal_code",
#         "country": "country",
#         "phone": "phone",
#         "email": "email",
#         "comments": "comments",
#         "social_rating_change": "social_rating_change",
#         "antisocial_rating_change": "antisocial_rating_change",
#         "tags": ["tag1", "tag2"]
#     }
#     """
#     org_fields = ["name", "dba"]
#     physical_address_fields = ["street1", "street2", "city", "state", "postal_code", "country"]
#     telephone_fileds = ["phone"]
#     email_fields = ["email"]
#     rating_change_fields = ["social_rating_change", "antisocial_rating_change", "change"]

#     # check to see if physical address exists in data
#     no_ancillary_data = data.get("street1", None) is None

#     # check to see if any organization already exists with the same name
#     org = Organization.query.filter_by(name=data["name"]).first()
#     if not org:
#         # build Organization based on org_fields and json
#         kwargs = dict()
#         for x, y in [(x, y) for x, y in data.items() if x in org_fields]:
#             # add any fields that are in the json and in the org_fields list and truncate them
#             # based on the length of the field in the database
#             if len(y) > Organization.__table__.columns[x].type.length:
#                 y = y[: Organization.__table__.columns[x].type.length]
#             kwargs[x] = y
#         org = Organization(**kwargs)
#         db.session.add(org)
#         if no_ancillary_data is False:  # have ancillary data so we have to persist this org
#             db.session.commit()

#     if no_ancillary_data is True:
#         return

#     # build OrganizationPhysicalAddress based on physical_address_fields and json
#     kwargs = dict()
#     for x, y in [(x, y) for x, y in data.items() if x in physical_address_fields]:
#         if len(y) > OrganizationPhysicalAddress.__table__.columns[x].type.length:
#             y = y[: OrganizationPhysicalAddress.__table__.columns[x].type.length]
#         kwargs[x] = y
#     kwargs["organization_id"] = org.id
#     org_physical_address = OrganizationPhysicalAddress(**kwargs)
#     db.session.add(org_physical_address)

#     kwargs = dict()
#     for x, y in [(x, y) for x, y in data.items() if x in telephone_fileds]:
#         if len(y) > OrganizationTelephoneNumber.__table__.columns[x].type.length:
#             y = y[: OrganizationTelephoneNumber.__table__.columns[x].type.length]
#         kwargs[x] = y
#     kwargs["organization_id"] = org.id
#     org_telephone_number = OrganizationTelephoneNumber(**kwargs)
#     db.session.add(org_telephone_number)

#     kwargs = dict()
#     for x, y in [(x, y) for x, y in data.items() if x in email_fields]:
#         if len(y) > OrganizationEmailAddress.__table__.columns[x].type.length:
#             y = y[: OrganizationEmailAddress.__table__.columns[x].type.length]
#         kwargs[x] = y
#     kwargs["organization_id"] = org.id
#     org_email_address = OrganizationEmailAddress(**kwargs)
#     db.session.add(org_email_address)

#     kwargs = dict()
#     for x, y in [(x, y) for x, y in data.items() if x in rating_change_fields]:
#         # no length check here because these are TEXT and INT fields
#         kwargs[x] = y
#     kwargs["changed_by_user_id"] = session["jwt_user_id"]
#     kwargs["organization_id"] = org.id
#     org_rating_change = OrganizationRatingChange(**kwargs)
#     db.session.add(org_rating_change)

#     if org.social_rating is None:
#         org.social_rating = 0
#     if org.antisocial_rating is None:
#         org.antisocial_rating = 0
#     org.social_rating += org_rating_change.social_rating_change
#     org.antisocial_rating += org_rating_change.antisocial_rating_change

#     for tag_name in list(set(data.get("tags", []))):
#         if tag_name not in memoized_tags:
#             tag = Tag.query.filter_by(name=tag_name).first()
#             if not tag:
#                 tag = Tag(name=tag_name)
#                 db.session.add(tag)
#                 db.session.commit()
#             memoized_tags[tag_name] = tag
#         tag = memoized_tags[tag_name]
#         if tag not in org.tags:
#             try:
#                 org.tags.append(tag)
#             except:
#                 pass


# # route to insert and organization
# @mod.route("/organizations", methods=["GET"])
# @jwt_has_access("read")
# def get_organizations():
#     orgs = []
#     if request.args.get("no_rating_changes"):
#         # get all organizations only if they don't have any rating change
#         orgs = Organization.query.filter(~Organization.rating_changes.any()).all()
#     else:
#         orgs = Organization.query.all()
#     return jsonify([x.to_dict() for x in orgs])


# # route to insert user
# @csrf.exempt
# @mod.route("/user", methods=["POST"])
# @jwt_has_access("write")
# def upsert_user():
#     # a full user object should look like this
#     # {
#     #     "user": {
#     #         "username": "username",
#     #         "given_name": "first_name",
#     #         "family_name": "last_name",
#     #         "social_media_accounts": [
#     #             {
#     #                 "platform": "platform",
#     #                 "handle": "handle"
#     #             }
#     #         ],
#     #         "email_addresses": [
#     #             "email_address"
#     #         ],
#     #         "physical_addresses": [
#     #             {
#     #                 "street1": "street1",
#     #                 "street2": "street2",

#     #                 "city": "city",
#     #                 "state": "state",
#     #                 "postal_code": "postal_code",
#     #                 "country": "country"
#     #             }
#     #         ],
#     #         "telephone_numbers": [
#     #             "telephone_number"
#     #         ]
#     #     }
#     # }

#     # check if user exists with this username
#     user_data = request.json.get("user", {})
#     if not user_data:
#         return {"status": "error", "message": "user is required"}

#     if not user_data.get("username"):
#         return {"status": "error", "message": "username is required"}

#     objs_to_add = []
#     user = User.query.filter_by(username=user_data.get("username")).first()
#     if not user:
#         # create new user
#         print(user_data)
#         _data = dict()
#         for f in ["username", "given_name", "family_name"]:
#             # add any fields that are in the json and in the org_fields list and truncate them
#             # based on the length of the field in the database
#             if len(user_data.get(f, "")) > User.__table__.columns[f].type.length:
#                 _data[f] = user_data.get(f)[: User.__table__.columns[f].type.length]
#             _data[f] = user_data.get(f)
#         _data["sso_id"] = generate_id(User.__table__.columns["sso_id"].type.length)
#         user = User(**_data)
#         user.groups.append(Group.query.filter_by(name="Users").first())
#         objs_to_add.append(user)

#     for _sma in user_data.get("social_media_accounts", []):
#         # sma should be a dict with the following keys
#         # platform, handle
#         platform = SocialMediaPlatform.query.filter_by(name=_sma.get("platform")).first()
#         if not platform:
#             continue
#         handle = _sma.get("handle")
#         if len(handle) > UserSocialMediaAccount.__table__.columns["handle"].type.length:
#             handle = handle[: UserSocialMediaAccount.__table__.columns["handle"].type.length]
#         sma = UserSocialMediaAccount(handle=handle, platform=platform, user=user)
#         objs_to_add.append(sma)

#     for _email in user_data.get("email_addresses", []):
#         # email should be just a string
#         if len(_email) > UserEmailAddress.__table__.columns["email"].type.length:
#             _email = _email[: UserEmailAddress.__table__.columns["email"].type.length]
#         email = UserEmailAddress(email=_email, user=user)
#         objs_to_add.append(email)

#     for _physical_address in user_data.get("physical_addresses", []):
#         # physical_address should be a dict with the following keys
#         # street1, street2, city, state, postal_code, country
#         _physical_address["user_id"] = user.id
#         for f in list(_physical_address.keys()):
#             # add any fields that are in the json and in the org_fields list and truncate them
#             # based on the length of the field in the database
#             if len(_physical_address.get(f, "")) > UserPhysicalAddress.__table__.columns[f].type.length:
#                 _physical_address[f] = _physical_address.get(f)[: UserPhysicalAddress.__table__.columns[f].type.length]
#         physical_address = UserPhysicalAddress(**_physical_address)
#         objs_to_add.append(physical_address)

#     for _telephone_number in user_data.get("telephone_numbers", []):
#         # telephone number should just be a string
#         if len(_telephone_number) > UserTelephoneNumber.__table__.columns["phone"].type.length:
#             _telephone_number = _telephone_number[: UserTelephoneNumber.__table__.columns["phone"].type.length]
#         telephone_number = UserTelephoneNumber(phone=_telephone_number, user=user)
#         objs_to_add.append(telephone_number)

#     db.session.add_all(objs_to_add)
#     db.session.commit()
#     return {"status": "success"}

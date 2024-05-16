# auth utilites
from datetime import datetime
from functools import wraps
from flask import session, redirect, url_for, request, abort, jsonify

from flask_jwt_extended import verify_jwt_in_request
import logging

from models import AccessTokens


# decorator to determine if user is logged in and is member of the group
def requires_login_and_group(group):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Check if user is logged in
            if "username" not in session or "groups" not in session:
                return redirect(url_for("auth.login"))

            # Check if user is a member of the specified group
            if not "Admins" in session["groups"] and group not in session["groups"]:
                return redirect(url_for("auth.unauthorized"))
            return f(*args, **kwargs)

        return decorated_function

    return decorator


access_type_map = dict(
    read=["read"],
    write=["read", "write"],
    admin=["read", "write", "admin"],
)


# decorator to check for revoked and level of access
def jwt_has_access(access_type: str):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # check if token exists
            hdr, data = verify_jwt_in_request()
            # get access token by jti
            access_token = AccessTokens.get(name=data["jti"])
            if not access_token:
                abort(401, response=jsonify(dict(msg="Invalid token")))

            if access_token.expiration_datetime < datetime.utcnow():
                abort(401, response=jsonify(dict(msg="Token expired")))

            if access_type not in access_type_map.get(access_token.access_type, []):
                abort(401, response=jsonify(dict(msg="Invalid access type")))

            session["jwt_user_id"] = access_token.created_by_user_id

            return f(*args, **kwargs)

        return decorated_function

    return decorator

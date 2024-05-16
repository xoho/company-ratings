import logging
import os

import arrow
from authlib.integrations.requests_client import OAuth2Session
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
)
import requests
from requests.auth import HTTPBasicAuth

from app import app, config
from auth.forms import *
from auth.utils import requires_login_and_group
from models import Users, UserProfile, EmailAddressProfile, ChangeEventProfile, BlockedUsers

mod = Blueprint("auth", __name__, url_prefix="/auth")


class FetchTokenException(Exception):
    pass


class FetchUserInfoException(Exception):
    pass


def get_redirect_uri():
    kwargs = dict(_external=True)
    if "localhost" not in request.url:
        kwargs["_scheme"] = "https"
    return url_for("auth.callback", **kwargs)


@mod.route("/logout")
@requires_login_and_group("Users")
def logout():
    kwargs = dict(
        post_logout_redirect_uri=url_for("auth.logoutconfirm", _external=True),
        id_token_hint=session.get("id_token", None),
    )
    session.clear()
    url = f"{config.KEYCLOAK_URL}/protocol/openid-connect/logout?{'&'.join([f'{k}={v}' for k,v in kwargs.items()])}"
    return redirect(url)


@mod.route("/logoutconfirm")
def logoutconfirm():
    return render_template("auth/logout.html")


@mod.route("/login")
def login():
    if os.getenv("LOCALONLY", "FALSE").upper() == "TRUE":
        load_user(
            username="admin",
            user_group_name="Admins",
            email="admin@localhost",
            id_token="fake",
            sso_id="admin",
            user_dao=Users()
        )
        return redirect(url_for("main.profile"))

    base_url = f"{config.KEYCLOAK_URL}/protocol/openid-connect/auth"
    redirect_uri = get_redirect_uri()
    url = f"{base_url}?response_type=code&client_id={config.KEYCLOAK_CLIENT_ID}&redirect_uri={get_redirect_uri()}&scope={config.KEYCLOAK_CLIENT_SCOPE}"
    return redirect(url)


@mod.route("/callback")
def callback():
    state = request.args.get("session_state")
    code = request.args.get("code")
    if state is None or code is None:
        message = "Invalid callback request"
        return render_template("error.html", message=message)

    base_error_message = "Could not authenticate. Please try again later. (error={exp})"
    user_info = None
    try:
        user_info = handle_keycloak_callback(state=state, code=code)
    except FetchTokenException as exp:
        logging.error(f"FetchTokenException: {exp}")
        return render_template("error.html", message=base_error_message.format(exp=f"FetchTokenException - {exp}"))
    except FetchUserInfoException as exp:
        logging.error(f"FetchUserInfoException: {exp}")
        return render_template("error.html", message=base_error_message.format(exp=f"FetchUserInfoException - {exp}"))
    except Exception as exp:
        logging.error(f"Exception: {exp}")
        return render_template("error.html", message=base_error_message.format(exp=f"Exception - {exp}"))

    username = user_info.get("preferred_username", None)
    sso_id = user_info.get("sub", None)
    email = user_info.get("email", None)
    id_token = user_info.get("id_token", None)
    if not username or not sso_id or not email or not id_token:  # we should always get these bits from keycloak
        message = "Could not authenticate. Please try again later."
        return render_template("error.html", message=message)

    user_dao = Users()
    # set user_group_name to "Users" if there are more than 0 existing users, otherwise set it to "Admins"
    user_group_name = "Users" if len(user_dao.ls()) > 0 else "Admins"
    load_user(username, user_group_name, email, id_token, sso_id, user_dao)
    return redirect(url_for("main.profile"))


def load_user(username, user_group_name, email, id_token, sso_id, user_dao):
    # load user from db based on username
    user = user_dao.get(name=username)
    if (user and user.is_blocked) or BlockedUsers().is_blocked(name=username):
        return render_template("error.html", message="User is blocked. Please contact the administrator.")

    if not user:
        print("Creating user")
        user_data = dict(
            name=username,
            uid=sso_id,
        )
        user = UserProfile(**user_data)
        user.groups.append(user_group_name)
        email_profile = EmailAddressProfile(email=email)
        user.email_addresses.append(email_profile)
        user.change_events.append(
            ChangeEventProfile(
                change="First login",
                changed_by_user="system",
                social_rating_change=config.RATING_CHANGE_ON_FIRST_LOGIN,
                change_date=str(arrow.utcnow()),
            )
        )
        user.social_rating = config.RATING_CHANGE_ON_FIRST_LOGIN
        user.is_premium_user = True
        user.user_since = str(arrow.utcnow())
    else:
        # verify user
        if user.uid != sso_id:
            return render_template("error.html", message="User already exists with different SSO ID")

    user.is_blocked = False
    user.is_active = True
    user.last_login = str(arrow.utcnow())
    user_dao.update(user)

    session["user_id"] = user.uid
    session["groups"] = user.groups
    session["username"] = user.name
    session["id_token"] = id_token



def handle_keycloak_callback(state: str, code: str) -> dict:
    """
    Handle the Keycloak callback
    The current version of python keycloak library has not been updated to handle new keycloak versions
    so this is a workaround to get the user info
    """
    client_id = config.KEYCLOAK_CLIENT_ID
    client_secret = config.KEYCLOAK_CLIENT_SECRET
    scope = config.KEYCLOAK_CLIENT_SCOPE
    keycloak_url = config.KEYCLOAK_URL
    client = OAuth2Session(client_id, client_secret, scope=scope)

    auth = HTTPBasicAuth(client_id, client_secret)
    access_token_url = f"{keycloak_url}/protocol/openid-connect/token"
    kwargs = dict(
        url=access_token_url,
        grant_type="authorization_code",
        auth=auth,
        redirect_uri=get_redirect_uri(),
    )
    if state is not None:
        kwargs["state"] = state
    if code is not None:
        kwargs["code"] = code
    try:
        token = client.fetch_token(**kwargs)
    except Exception as exp:
        msg = f"Could not fetch token - {exp}"
        logging.error(msg)
        raise FetchTokenException(msg)

    token_type = token.get("token_type")
    access_token = token.get("access_token")
    session["id_token"] = token.get("id_token", None)
    if token_type is None or access_token is None:
        return dict(())

    url = f"{keycloak_url}/protocol/openid-connect/userinfo?scope={scope}"
    headers = dict(Authorization=f"{token_type} {access_token}")
    res = requests.get(url, headers=headers)
    try:
        res.raise_for_status()
    except Exception as exp:
        msg = f"Could not fetch userinfo - {exp}"
        logging.error(msg)
        raise FetchUserInfoException(msg)
    data = res.json()
    data["id_token"] = token.get("id_token", None)
    return data


@mod.route("/unauthorized")
def unauthorized():
    message = "You are not authorized to access this page"
    return render_template("error.html", message=message)

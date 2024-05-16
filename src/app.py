from datetime import timedelta, datetime
import json
import logging
from logging.config import dictConfig
import os
from urllib.parse import quote

from flask import Flask, redirect, url_for, session, render_template, jsonify, Markup
from flask import has_request_context, request
from flask_jwt_extended import JWTManager
from flask.logging import default_handler
from flask_wtf.csrf import CSRFProtect
from werkzeug.middleware.proxy_fix import ProxyFix

from config import config
from models import UserProfile, OrganizationProfile, initS3

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": config.ROOT_LOG_LEVEL, "handlers": ["wsgi"]},
    }
)

app = Flask(config.APP_NAME or __name__)
app.static_folder = config.STATIC_FOLDER

app.config.from_object(config)
# fixes when behind nginx
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

csrf = CSRFProtect()
csrf.init_app(app)

jwt = JWTManager(app)

# init s3
initS3()


@app.context_processor
def inject_app():  # inject app into all templates
    return dict(
        app=app,
        is_admin="Admins" in session.get("groups", []),
        current_year=str(datetime.now().year),
        antisocial_credit_list_url=config.ANTISOCIAL_CREDIT_LIST_URL,
    )


class RequestFormatter(logging.Formatter):
    def format(self, record):
        if has_request_context():
            record.url = request.url
            record.remote_addr = request.remote_addr
        else:
            record.url = None
            record.remote_addr = None

        return super().format(record)


formatter = RequestFormatter(
    "[%(asctime)s] %(remote_addr)s requested %(url)s\n" "%(levelname)s in %(module)s: %(message)s"
)
root = logging.getLogger()
root.addHandler(default_handler)


from main.routes import mod as mainmod
from admin.routes import mod as adminmod
from auth.routes import mod as authmod
from api.routes import mod as apimod

app.register_blueprint(mainmod)
app.register_blueprint(adminmod)
app.register_blueprint(authmod)
app.register_blueprint(apimod)


@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    return jsonify({"status": 401, "sub_status": 42, "msg": "The token has expired"}), 401


# template filters
@app.template_filter("url_quote")
def url_quote(data):
    # return quote(data)
    return data


@app.template_filter("just_date")
def just_date(dt):
    return str(dt).split("T")[0]


@app.template_filter("short_datetime")
def just_time(dt):
    return str(dt).split(".")[0].replace("T", " ")


@app.template_filter("get_display_name")
def get_display_name(profile):
    is_contact = hasattr(profile, "groups") and "Contacts" in [x.name for x in profile.groups]
    if hasattr(profile, "username") and profile.username is not None:
        if is_contact:
            if profile.family_name is None and profile.given_name is None:
                return "(not set)"
            return ", ".join([profile.family_name or "", profile.given_name or ""])
        else:
            return profile.username
    return profile.name


@app.template_filter("is_user")
def is_user(profile):
    return isinstance(profile, UserProfile)


@app.template_filter("profile_type")
def profile_type(profile):
    if isinstance(profile, UserProfile):
        return "user"
    if isinstance(profile, OrganizationProfile):
        return "org"
    return "unknown"


@app.template_filter("is_org")
def is_org(profile):
    return isinstance(profile, OrganizationProfile)


@app.template_filter("is_contact")
def is_contact(profile):
    return hasattr(profile, "groups") and "Contacts" in [x.name for x in profile.groups] and isinstance(profile, User)


@app.template_filter("get_return_url")
def get_return_url(profile):
    if is_contact(profile):
        return url_for("main.profile", user_id=profile.id)
    if is_user(profile):
        return url_for("main.profile", username=profile.username)
    return url_for("main.profile_organization", org_id=profile.id)


@app.template_filter("pretty_json")
def pretty_json(value):
    if isinstance(value, dict):
        obj = value
    else:
        try:
            obj = json.loads(value)
            if not isinstance(obj, dict):
                return value
        except json.JSONDecodeError:
            return value

    result = ""
    for key, val in obj.items():
        result += f"<strong>{key}</strong>: {val}<br>"
    return Markup(result)


@app.errorhandler(404)
def page_not_found(e):
    # note that we set the 404 status explicitly
    return render_template("error.html", message="That page was not found"), 404


@app.route("/")
def index():
    return redirect(url_for("main.index"))


@app.before_request
def make_session_permanent():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=60)

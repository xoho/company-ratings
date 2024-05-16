from datetime import timedelta
import random
import string

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
)
from flask_jwt_extended import create_access_token, get_jti


from app import app, config
from admin.forms import (
    AddRatingTypeForm,
    AddUserForm,
    DeleteUserForm,
    AddOrganizationForm,
    AddBlockedUserForm,
    AddAccessTokenForm,
    AddTagForm,
)
from auth.utils import requires_login_and_group
from models import (
    AccessTokenModel,
    AccessTokens,
    ChangeEventProfile,
    OrganizationProfile,
    Organizations,
    Users,
    UserProfile,
    Tags,
)

mod = Blueprint("admin", __name__, url_prefix="/admin")


def generate_id(length):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


@mod.route("/")
@requires_login_and_group("Admins")
def index():
    users = Users().users()
    tags = Tags().tags()

    return render_template(
        "admin/action.html",
        admins=[x for x in users if x.is_admin],
        antisocial_credits=[x for x in users if x.is_premium_user is False],
        manually_added_users=[x for x in users if x.create_method == "manual"],
        organization_count=Organizations().count(),
        blocked_users=[x for x in users if x.is_blocked is True],
        tags=tags,
        access_tokens=AccessTokens().ls(),
    )


# create and update a user
@mod.route("/user", methods=["GET", "POST"])
@mod.route("/user/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def edit_user(name: str = None):
    user = None
    if name:
        user = Users().get(name=name)
    if request.method == "GET" and user:
        form = AddUserForm(obj=user)
    else:
        form = AddUserForm()
    if form.validate_on_submit():
        new_user = False
        if user:
            user.name = form.username.data
        else:
            new_user = True
            user = UserProfile(name=form.username.data, uid=generate_id(80), create_method="manual")

        # update change events
        user.change_events.append(
            ChangeEventProfile(
                change="updated" if new_user is False else "admin created",
                change_date=str(arrow.utcnow()),
                changed_by_user=session["username"],
            )
        )
        Users().update(user)
        return redirect(url_for("main.profile", username=user.name))
    return render_template("admin/update.html", form=form, update_type="User")


# delete a user
@mod.route("/user/delete/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def delete_user(name: str):
    users = Users()
    user = users.get(name=name)
    if not user:
        return redirect(url_for("admin.search_users"))

    if user.create_method != "manual":
        return render_template("error.html", message="Cannot delete user created by SSO")

    form = DeleteUserForm()

    if form.validate_on_submit():
        if form.confirm.data == "DELETE":
            users.delete(user)
            return redirect(url_for("admin.index"))
    return render_template("admin/delete_user.html", form=form, user=user)


# create and update an organization
@mod.route("/organization", methods=["GET", "POST"])
@mod.route("/organization/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def edit_organization(name: str = None):
    organization = None
    orgs = Organizations()
    if name:
        organization = orgs().get(name=name)
    if request.method == "GET" and organization:
        form = AddOrganizationForm(obj=organization)
    else:
        form = AddOrganizationForm()
    print("organization", organization)
    if form.validate_on_submit():
        # check to see if organizaiton already exists with this name
        existing_organization = orgs.get(name=form.name.data)
        print("existing_organization", existing_organization, organization)
        if organization and existing_organization and existing_organization.name == organization.name:
            form.name.errors.append("Organization with this name already exists")

        if not form.name.errors:
            if organization:
                print("updating organization")
                organization.name = form.name.data
            else:
                print("creating organization")
                organization = OrganizationProfile(name=form.name.data)
            orgs.update(organization)
        return redirect(url_for("admin.index"))
    return render_template("admin/update.html", form=form, update_type="Organization")


# add a blocked user
@mod.route("/blockeduser", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def add_blocked_name():
    form = AddBlockedUserForm()
    if form.validate_on_submit():
        users = Users()
        # check to see if blocked user already exists with this name
        blocked_user = users.get(name=form.username.data)
        if not blocked_user:
            blocked_user = UserProfile(name=form.username.data, uid=generate_id(80), create_method="manual")
        blocked_user.is_blocked = True
        blocked_user.block_reason = form.reason.data
        blocked_user.change_events.append(
            ChangeEventProfile(
                change="blocked " + form.reason.data,
                change_date=str(arrow.utcnow()),
                changed_by_user=session["username"],
            )
        )
        users.update(blocked_user)
        return redirect(url_for("admin.index"))
    return render_template("admin/update.html", form=form, update_type="Blocked User")


# unblock a user
@mod.route("/user/unblock/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def unblock_user(name):
    users = Users()
    blocked_user = users.get(name=name)
    if not blocked_user:
        return redirect(url_for("admin.search_users"))
    if blocked_user.is_blocked is False:
        return redirect(url_for("admin.index"))
    blocked_user.is_blocked = False
    blocked_user.block_reason = None
    blocked_user.change_events.append(
        ChangeEventProfile(
            change="unblocked",
            change_date=str(arrow.utcnow()),
            changed_by_user=session["username"],
        )
    )
    users.update(blocked_user)
    return redirect(url_for("admin.index"))


# create route to add access token
@mod.route("/access_token", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def add_access_token():
    form = AddAccessTokenForm()
    if form.validate_on_submit():
        expiry_seconds = 60 * 60 * 24 * 365  # 1 year

        jwt = create_access_token(
            identity=session["username"],
            expires_delta=timedelta(seconds=expiry_seconds),
        )

        access_token = AccessTokenModel(
            token=jwt,
            jti=get_jti(encoded_token=jwt),
            created_by_user=session["username"],
            comment=form.comment.data,
            expiration_datetime=arrow.utcnow().shift(seconds=expiry_seconds).datetime,
            access_type=form.access_type.data,
        )
        AccessTokens().save(access_token)
        return redirect(url_for("admin.index"))
    return render_template("admin/update.html", form=form, update_type="Access Token")


# create route to delete access token
@mod.route("/access_token/delete/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def delete_access_token(name):
    access_token = AccessTokens().get(name=name)
    if not access_token:
        return redirect(url_for("admin.index"))
    AccessTokens().rm(access_token)
    return redirect(url_for("admin.index"))


# make a user an admin
@mod.route("/user/admin", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def make_admin(name: str = None):
    form = AddUserForm()
    if form.validate_on_submit():
        users = Users()
        user = users.get(name=form.username.data)
        if not user:
            return redirect(url_for("admin.search_users"))

        user.groups.append("Admins")
        user.change_events.append(
            ChangeEventProfile(
                change="made admin",
                change_date=str(arrow.utcnow()),
                changed_by_user=session["username"],
            )
        )
        users.update(user)
        return redirect(url_for("admin.index"))
    return render_template("admin/update.html", form=form, update_type="Admin")


# remove user from admin group
@mod.route("/user/admin/remove/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def remove_admin(name: str):
    users = Users()
    user = users.get(name=name)
    print("remove_admin", user)
    if not user:
        return redirect(url_for("admin.search_users"))
    user.groups.remove("Admins")
    user.change_events.append(
        ChangeEventProfile(
            change="removed from admin",
            change_date=str(arrow.utcnow()),
            changed_by_user=session["username"],
        )
    )
    users.update(user)
    return redirect(url_for("admin.index"))


# route to create a new tag
@mod.route("/tag", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def add_tag():
    form = AddTagForm()
    if form.validate_on_submit():
        tags = Tags()
        tag = tags.get(name=form.name.data)
        if not tag:
            name = form.name.data
            tag_data = dict(name=name, profiles=[], description=form.description.data)
            tags.update(name=name, data=tag_data)
        return redirect(url_for("admin.index"))
    return render_template("admin/update.html", form=form, update_type="Tag")


# route to delete a tag
@mod.route("/tag/delete/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def delete_tag(name):
    tags = Tags()
    tag = tags.get(name=name)
    if not tag:
        return redirect(url_for("admin.index"))
    if "profiles" in tag and tag["profiles"]:
        return render_template("error.html", message="Cannot delete tag with profiles")
    tags.rm(name=name)
    return redirect(url_for("admin.index"))


# route to delete a tag from all profiles
@mod.route("/tag/delete/all/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def delete_tag_all(name):
    tags = Tags()
    tag = tags.get(name=name)
    if not tag:
        return redirect(url_for("admin.index"))
    for user in Users().users():
        if tag in user.tags:
            user.tags.remove(tag)
            Users().update(user)

    return redirect(url_for("admin.index"))


# route to show users with a tag
@mod.route("/tag/<string:name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def show_tag(name):
    tag = Tags().get(name=name)
    if not tag:
        return redirect(url_for("admin.index"))

    users = Users()
    users_with_tag = []
    for profile_name in tag.get("profiles", []):
        profile = users.get(name=profile_name)
        if profile:
            users_with_tag.append(profile)
    users_with_tag.sort(key=lambda x: x.name)

    return render_template(
        "admin/show_profiles.html", tag=tag, profiles=users_with_tag, data_type="users", data_target="tag " + name
    )

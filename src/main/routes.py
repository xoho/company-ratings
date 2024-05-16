from csv import DictWriter
from datetime import date
from time import time
from urllib.parse import quote, unquote

import arrow
import boto3
from flask import (
    Blueprint,
    render_template,
    url_for,
    redirect,
    request,
    session,
)

from app import app, config
from main.forms import (
    UserProfileForm,
    ProfileSocialMediaForm,
    UserProfileEmailsForm,
    ProfileLocationsForm,
    ProfileRatingChangeForm,
    UserProfileAdminForm,
    OrganizationProfileForm,
    ProfileTelephoneNumbersForm,
    OrganizationContactUserForm,
    DeleteConfirmForm,
    ProfileTagsForm,
)
from models import (
    Users,
    Organizations,
    UserProfile,
    OrganizationProfile,
    SocialMediaAccountProfile,
    SocialMediaPlatforms,
    SocialMediaAccounts,
    EmailAddressProfile,
    PhysicalAddressProfile,
    TelephoneNumberProfile,
    ChangeEventProfile,
    Words,
    Tags,
)
from auth.utils import requires_login_and_group

mod = Blueprint("main", __name__, url_prefix="/")

s3 = boto3.client(
    "s3",
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_DEFAULT_REGION,
)


@mod.route("/")
@requires_login_and_group("Users")
def index():
    return redirect(url_for("main.profile"))


@mod.route("/search/<string:entity_type>")
@requires_login_and_group("Users")
def search_entity(entity_type: str):
    q = request.args.get("q", "")
    terms = [quote(x) for x in q.strip().split(" ")]
    # pagination
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", config.DEFAULT_PER_PAGE, type=int)
    start = time()
    pagination = None
    if entity_type == "user":
        pagination = search_users(terms=terms, page=page, per_page=per_page)
    elif entity_type == "organization":
        pagination = search_organizations(terms=terms, page=page, per_page=per_page)
    else:
        return render_template("error.html", message="Invalid entity type.")

    return render_template(
        "main/search_entities.html",
        pagination=pagination,
        q=q,
        entity_type=entity_type,
        page=page,
        per_page=per_page,
        duration=round(time() - start, 2),
    )


@mod.route("/search")
@requires_login_and_group("Users")
def search():
    q = request.args.get("q", "").replace("'", "").replace('"', "")
    terms = [quote(x) for x in q.strip().split(" ") if x]

    users = []
    organizations = []
    org_count = 0
    user_count = 0
    print("terms", terms)
    if terms:
        organizations = search_organizations(terms=terms)
        users = search_users(terms)
        org_count = organizations.total
        users = search_users(terms=terms)
        user_count = users.total
    else:
        org_count = Organizations().count()
        user_count = Users().count()
    return render_template(
        "main/search.html", users=users, organizations=organizations, q=q, org_count=org_count, user_count=user_count
    )


def search_users(terms: list[str], page: int = 1, per_page: int = config.DEFAULT_PER_PAGE):
    users = Users().ls()
    social_media_accounts = SocialMediaAccounts().ls()
    matches = {}
    for term in terms:
        if term not in matches:
            matches[term] = []
        for user in users:
            if term.lower() in user.lower():
                matches[term].append(user)
        for sma in social_media_accounts:
            if term.lower() in sma.lower():
                platform, handle = sma.split(":")
                profile = SocialMediaAccounts().get_profile(platform=platform, handle=handle, profile_type="user")
                matches[term].append(profile.name)

    # reduce the users by intersecting the sets of users for each term
    users = set()
    for _, usrs in matches.items():
        if not users:
            users = set(usrs)
        else:
            users = users.intersection(set(usrs))

    users = [UserProfile(name=x, uid="placeholder") for x in sorted(users, key=lambda x: x.lower())]
    return ListPagination(users, page, per_page, len(users))


class StubProfile:
    def __init__(self, name: str):
        self.name = name
        self.id = name


class ListPagination:
    def __init__(self, items: list, page: int, per_page: int, total: int):
        self.items = items[(page - 1) * per_page : page * per_page]
        self.page = page
        self.per_page = per_page
        self.total = total
        self.pages = int(total / per_page) + (1 if total % per_page > 0 else 0)
        self.has_prev = page > 1
        self.has_next = page < self.pages
        self.prev_num = max(1, page - 1)
        self.next_num = min(page + 1, self.pages)


def search_organizations(terms: list[str], page: int = 1, per_page: int = config.DEFAULT_PER_PAGE):
    concat_terms = "".join([x.lower() for x in sorted(terms)])
    organizations = []

    exclude_terms = ["llc", "inc"]
    words_group = Words()
    words = dict()
    for term in [x.lower() for x in terms if x and x not in exclude_terms]:
        word = words_group.get(term)
        if word and "organizations" in word:
            words[term] = word.get("organizations", [])

    # reduce the orgs by intersecting the sets of orgs for each word
    organizations = set()
    for _, orgs in words.items():
        if not organizations:
            organizations = set(orgs)
        else:
            organizations = organizations.intersection(set(orgs))
    organizations = list(organizations)
    organizations = [OrganizationProfile(name=x) for x in sorted(organizations, key=lambda x: x.lower())]
    pagination = ListPagination(organizations, page, per_page, len(organizations))
    return pagination


@mod.route("/profile")
@mod.route("/profile/username/<string:username>")
@requires_login_and_group("Users")
def profile(username=None):
    user = None
    _username = username if username else session["username"]
    if _username is None:
        return redirect(url_for("auth.login"))

    user = Users().get(name=_username)
    if not user:
        return render_template("error.html", message="User not found.")

    return render_template("main/profile.html", profile=user, system_tags=Tags().ls())


@mod.route("/profile/orgname/<string:name>", methods=["GET"])
@requires_login_and_group("Users")
def profile_organization(name: str):
    organization = Organizations().get(name)
    if name is None:
        return render_template("error.html", message="Organization not found.")
    print("organization", organization, type(organization))
    return render_template("main/profile.html", profile=organization)


@mod.route("/profile/personal/", methods=["GET", "POST"])
@mod.route("/profile/personal/<string:username>", methods=["GET", "POST"])
@requires_login_and_group("Users")
def profile_personal(username=None):
    user = None
    _username = username if username else session["username"]
    if not _username:
        return redirect(url_for("auth.login"))
    if "Admins" not in session.get("groups", []) and _username != session.get("username", None):
        # cannot edit if not an admin or the current logged in user
        return redirect(url_for("main.profile"))
    _username = quote(_username)

    user = Users().get(name=_username)
    if not user:
        return render_template("error.html", message="User not found.")

    form_class = UserProfileForm

    if "Admins" in session.get("groups", []) and username != session.get("username", None):
        # use admin form if admin and not editing own profile
        form_class = UserProfileAdminForm

    form = form_class()
    user.birthdate = arrow.get(user.birthdate or "1900-01-01").date()
    user.user_since = arrow.get(user.user_since or "1900-01-01").date()
    if request.method == "GET":
        form = form_class(obj=user)

    if form_class == UserProfileAdminForm:
        if user.name == session.get("username", None):
            # if current user is admin and editing own profile, do not allow editing of is_admin
            form.is_admin.render_kw = {"disabled": True}
        form.is_admin.data = "Admins" in user.groups

    if form.validate_on_submit():
        for k in [x for x in form.data.keys() if x not in ["user_since", "birthdate", "csrf_token", "is_admin"]]:
            v = form.data.get(k, None)
            if v is not None:
                setattr(user, k, v)

        birthdate = str(form.birthdate.data)
        if birthdate > "1900-01-01":
            user.birthdate = birthdate

        if form_class == UserProfileAdminForm:
            if user.name != session.get("username", None):
                # if current user is admin and not editing own profile, allow editing of is_admin
                if request.form.get("is_admin", None) == "y":
                    if "Admins" not in user.groups:
                        user.groups.append("Admins")
                else:
                    if "Admins" in user.groups:
                        user.groups = [x for x in user.groups if x != "Admins"]
            if form.user_since.data and str(form.user_since.data) > "1900-01-01":
                user.user_since = str(form.user_since.data)
            user.is_active = request.form.get("is_active", None) == "y"
            form.is_active.data = user.is_active
            form.is_admin.data = "Admins" in user.groups
        Users().update(user_profile=user)
        return redirect(url_for("main.profile", username=user.name))
    print("user", user)
    return render_template(
        "main/profile-update.html", profile=user, form=form, update_type="personal", update_verb="Update"
    )


def can_access(username: str = None):
    is_admin = "Admins" in session.get("groups", [])
    if is_admin:
        return True
    if username is None:
        return False
    return username == session.get("username", None)


# create route for profile_social_media
@mod.route("/profile/social_media/<string:profile_type>/<string:profile_name>", methods=["GET", "POST"])
@mod.route(
    "/profile/social_media/<string:profile_type>/<string:profile_name>/<string:platform>/<string:handle>",
    methods=["GET", "POST"],
)
@requires_login_and_group("Users")
def profile_social_media(profile_type: str, profile_name: str, platform: str = None, handle: str = None):
    if any([platform, handle]):
        if not platform or not handle:
            return render_template("error.html", message="Invalid social media account.")

    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)
    sma = None
    if handle:
        sma = [x for x in profile.social_media_accounts if x.platform == platform and x.handle == handle]
        if sma:
            sma = sma[0]

    form = ProfileSocialMediaForm()

    update_verb = "Add" if not sma else "Update"

    if request.method == "GET" and sma:
        form = ProfileSocialMediaForm(obj=sma)

    platforms = SocialMediaPlatforms().platforms()
    form.platform_id.choices = [(p, p) for p in platforms]

    if form.validate_on_submit():
        if sma and form.handle.data == sma.handle and form.platform_id.data == sma.platform:
            # nothing changed, redirect to profile
            return redirect(redirect_url)

        social_media_accounts = SocialMediaAccounts()

        # make sure platform is valid
        if form.platform_id.data not in platforms:
            form.platform_id.errors.append("Invalid platform.")
        # check to see if this handle on this platform is already present
        elif social_media_accounts.get(platform=form.platform_id.data, handle=form.handle.data):
            form.handle.errors.append("This handle is already in use on this platform.")

        if not form.handle.errors:
            # create the handle and add to the user
            if sma:
                # update the profile
                for account in profile.social_media_accounts:
                    if account.platform == sma.platform and account.handle == sma.handle:
                        account.platform = form.platform_id.data
                        account.handle = form.handle.data
                        break
            else:
                existing_sma = [
                    x
                    for x in profile.social_media_accounts
                    if x.platform == form.platform_id.data and x.handle == form.handle.data
                ]
                if not existing_sma:
                    profile.social_media_accounts.append(
                        SocialMediaAccountProfile(platform=form.platform_id.data, handle=form.handle.data)
                    )
            save_profile(profile)
            return redirect(redirect_url)
    return render_template(
        "main/profile-update.html", profile=profile, form=form, update_type="social media", update_verb=update_verb
    )


def save_profile(profile):
    if isinstance(profile, UserProfile):
        Users().update(user_profile=profile)
    elif isinstance(profile, OrganizationProfile):
        Organizations().update(profile)


# # create route to delete the social media account
@mod.route(
    "/profile/social_media/delete/<string:profile_type>/<string:profile_name>/<string:platform>/<string:handle>",
    methods=["GET", "POST"],
)
@requires_login_and_group("Users")
def profile_social_media_delete(profile_type: str, profile_name: str, platform: str, handle: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    sma = SocialMediaAccounts().get(platform=platform, handle=handle)
    if not sma:
        return render_template("error.html", message="Social media account not found.")

    if request.method == "POST":
        SocialMediaAccounts().rm(platform=platform, handle=handle)
        remove_idx = None
        for idx, account in enumerate(profile.social_media_accounts):
            if account.platform == platform and account.handle == handle:
                remove_idx = idx
                break
        if remove_idx is not None:
            del profile.social_media_accounts[remove_idx]
        save_profile(profile)
        return redirect(redirect_url)
    form = DeleteConfirmForm()
    return render_template(
        "confirm.html",
        form=form,
        action="Delete",
        message=f"Confirm deletion of social media account {handle} on {platform}.",
    )


# create route for profile_emails
@mod.route("/profile/emails/<string:profile_type>/<string:profile_name>", methods=["GET", "POST"])
@mod.route("/profile/emails/<string:profile_type>/<string:profile_name>/<string:email>", methods=["GET", "POST"])
@requires_login_and_group("Users")
def profile_emails(profile_type: str, profile_name: str, email: str = None):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    _email = None
    if email:
        _email = [x for x in profile.email_addresses if x.email == email]
        if _email:
            _email = _email[0]

    update_verb = "Add" if _email is None else "Update"
    form = UserProfileEmailsForm()
    if request.method == "GET" and email:
        form = UserProfileEmailsForm(obj=_email)

    if form.validate_on_submit():
        if _email:
            _email.email = form.email.data
        else:
            _email = EmailAddressProfile(email=form.email.data)
        _email.is_default = request.form.get("is_default", None) == "y"

        # update other emails to not be default if this one is default
        if _email.is_default:
            for e in profile.email_addresses:
                if e.email != _email.email:
                    e.is_default = False

        existing_email = [x for x in profile.email_addresses if x.email == _email.email]
        if not existing_email:
            profile.email_addresses.append(_email)

        save_profile(profile)
        return redirect(redirect_url)

    return render_template(
        "main/profile-update.html", profile=profile, form=form, update_type="emails", update_verb=update_verb
    )


# create route to delete the email address
@mod.route("/profile/emails/delete/<string:profile_type>/<string:profile_name>/<string:email>", methods=["GET", "POST"])
@requires_login_and_group("Users")
def profile_emails_delete(profile_type: str, profile_name: str, email: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    # check if email exists
    delete_idx = None
    for idx, _email in enumerate(profile.email_addresses):
        if _email.email.lower() == email.lower():
            delete_idx = idx
            break
    if delete_idx is None:
        return redirect(redirect_url)

    if request.method == "POST":
        del profile.email_addresses[delete_idx]
        save_profile(profile)
        return redirect(redirect_url)

    form = DeleteConfirmForm()
    return render_template(
        "confirm.html",
        form=form,
        action="Delete",
        message=f"Confirm deletion of email {email}.",
    )


# create route for profile_locations
@mod.route("/profile/locations/<string:profile_type>/<string:profile_name>", methods=["GET", "POST"])
@mod.route("/profile/locations/<string:profile_type>/<string:profile_name>/<string:hash>", methods=["GET", "POST"])
@requires_login_and_group("Users")
def profile_locations(profile_type: str, profile_name: str, hash: str = None):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    found_idx = None
    physical_addresses = None
    if hash:
        for idx, physical_addresses in enumerate(profile.physical_addresses):
            if physical_addresses.signature == hash:
                found_idx = idx
                break

    update_verb = "Add" if found_idx is None else "Update"

    form = ProfileLocationsForm()
    if request.method == "GET" and physical_addresses:
        form = ProfileLocationsForm(obj=physical_addresses)

    if form.validate_on_submit():
        if found_idx is None:
            kwargs = dict(
                street1=form.street1.data,
                street2=form.street2.data,
                city=form.city.data,
                state=form.state.data,
                postal_code=form.postal_code.data,
                country=form.country.data,
            )
            physical_addresses = PhysicalAddressProfile(**kwargs)
        else:
            physical_addresses.street1 = form.street1.data
            physical_addresses.street2 = form.street2.data
            physical_addresses.city = form.city.data
            physical_addresses.state = form.state.data
            physical_addresses.postal_code = form.postal_code.data
            physical_addresses.country = form.country.data

        physical_addresses.is_default = request.form.get("is_default", None) == "y"
        # set all other locations to not default
        if physical_addresses.is_default:
            for l in profile.physical_addresses:
                if l.signature != physical_addresses.signature:
                    l.is_default = False
        if found_idx is None:
            profile.physical_addresses.append(physical_addresses)
        else:
            profile.physical_addresses[found_idx] = physical_addresses
        save_profile(profile)
        return redirect(redirect_url)

    return render_template(
        "main/profile-update.html", profile=profile, form=form, update_type="locations", update_verb=update_verb
    )


# create route to delete the location
@mod.route(
    "/profile/locations/delete/<string:profile_type>/<string:profile_name>/<string:hash>", methods=["GET", "POST"]
)
@requires_login_and_group("Users")
def profile_locations_delete(profile_type: str, profile_name: str, hash: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    delete_idx = None
    location = None
    for idx, location in enumerate(profile.physical_addresses):
        if location.signature == hash:
            delete_idx = idx
            break
    if delete_idx is None:
        return render_template("error.html", message="Location not found.")

    if request.method == "POST":
        del profile.physical_addresses[delete_idx]
        save_profile(profile)
        return redirect(redirect_url)

    form = DeleteConfirmForm()
    return render_template(
        "confirm.html",
        form=form,
        action="Delete",
        message=f"Confirm deletion of location.",
    )


def get_profile_and_redirect_url(profile_type: str, profile_name: str):
    profile = None
    redirect_url = url_for("main.profile")
    profile_name = unquote(profile_name)
    obj = None
    if profile_type == "user":
        obj = Users()
        redirect_url = url_for("main.profile", username=profile_name)
    elif profile_type == "organization":
        obj = Organizations()
        redirect_url = url_for("main.profile_organization", name=profile_name)
    else:
        raise Exception(f'Invalid profile type "{profile_type}"')
    profile = obj.get(name=profile_name)
    if profile is None or can_access(profile.name) is False:
        return None, redirect_url

    return profile, redirect_url


# create route for editing telephone_numbers
@mod.route("/profile/telephone_numbers/<string:profile_type>/<string:profile_name>", methods=["GET", "POST"])
@mod.route(
    "/profile/telephone_numbers/<string:profile_type>/<string:profile_name>/<string:hash>", methods=["GET", "POST"]
)
@requires_login_and_group("Users")
def profile_telephone_numbers(profile_name: str, profile_type: str, hash: str = None):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    found_idx = None
    telephone_number = None
    if hash:
        for idx, telephone_number in enumerate(profile.telephone_numbers):
            if telephone_number.signature == hash:
                found_idx = idx
                break

    update_verb = "Add" if found_idx is None else "Update"

    form = ProfileTelephoneNumbersForm()
    if request.method == "GET" and telephone_number:
        form = ProfileTelephoneNumbersForm(obj=telephone_number)

    if form.validate_on_submit():
        if found_idx is None:
            telephone_number = TelephoneNumberProfile(phone=form.phone.data)
        else:
            telephone_number.phone = form.phone.data
        telephone_number.is_default = request.form.get("is_default", None) == "y"
        # set all other telephone_numbers to not default
        if telephone_number.is_default:
            for t in profile.telephone_numbers:
                if t.signature != telephone_number.signature:
                    t.is_default = False
        if found_idx is not None:
            profile.telephone_numbers[idx] = telephone_number
        else:
            profile.telephone_numbers.append(telephone_number)
        save_profile(profile)
        return redirect(redirect_url)

    return render_template(
        "main/profile-update.html", profile=profile, form=form, update_type="telephone_numbers", update_verb=update_verb
    )


# delete telephone_numbers endpoint
@mod.route(
    "/profile/telephone_numbers/delete/<string:profile_type>/<string:profile_name>/<string:hash>",
    methods=["GET", "POST"],
)
@requires_login_and_group("Users")
def profile_telephone_numbers_delete(profile_type: str, profile_name: str, hash: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    delete_idx = None
    telephone_number = None
    for idx, telephone_number in enumerate(profile.telephone_numbers):
        if telephone_number.signature == hash:
            delete_idx = idx
            break

    if delete_idx is None:
        return render_template("error.html", message="Telephone number not found.")

    if request.method == "POST":
        del profile.telephone_numbers[delete_idx]
        save_profile(profile)
        return redirect(redirect_url)
    form = DeleteConfirmForm()
    return render_template(
        "confirm.html",
        form=form,
        action="Delete",
        message=f"Confirm deletion of telephone {telephone_number.phone}.",
    )


# create route for profile_history
@mod.route("/profile/ratingchange/<string:profile_type>/<string:profile_name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def profile_rating_change(profile_type: str, profile_name: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    if profile.name == session["username"]:
        return render_template("error.html", message="You cannot change your own rating.")

    form = ProfileRatingChangeForm()
    if request.method == "GET":
        form = ProfileRatingChangeForm(obj=profile)

    if form.validate_on_submit():
        social_rating_change = max(0, form.social_rating_change.data)
        antisocial_rating_change = max(0, form.antisocial_rating_change.data)
        rating_change = ChangeEventProfile(
            change=form.change.data,
            social_rating_change=social_rating_change,
            antisocial_rating_change=antisocial_rating_change,
            changed_by_user=session["username"],
            change_date=str(arrow.utcnow()),
        )
        profile.change_events.append(rating_change)
        profile.social_rating += social_rating_change
        profile.antisocial_rating += antisocial_rating_change
        save_profile(profile)
        return redirect(redirect_url)

    return render_template(
        "main/profile-update.html", profile=profile, form=form, update_type="history", update_verb="Change"
    )


# route to add a tag to a profile
@mod.route("/profile/tags/<string:profile_type>/<string:profile_name>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def profile_tags(profile_type: str, profile_name: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    form = ProfileTagsForm()
    form.tag.choices = [(t, t) for t in Tags().ls()]

    if form.validate_on_submit():
        tag_name = form.tag.data
        if tag_name not in profile.tags:
            profile.tags.append(tag_name)
            save_profile(profile)
        return redirect(redirect_url)

    return render_template(
        "main/profile-update.html", profile=profile, form=form, update_type="tags", update_verb="Add"
    )


# route to remove a tag from a profile
@mod.route("/profile/tags/delete/<string:profile_type>/<string:profile_name>/<string:tag>", methods=["GET", "POST"])
@requires_login_and_group("Admins")
def profile_tags_delete(profile_type: str, profile_name: str, tag: str):
    profile, redirect_url = get_profile_and_redirect_url(profile_type, profile_name)
    if not profile:
        return redirect(redirect_url)

    if request.method == "POST":
        if tag in profile.tags:
            profile.tags = [x for x in profile.tags if x != tag]
            save_profile(profile)
        return redirect(redirect_url)

    form = DeleteConfirmForm()
    return render_template(
        "confirm.html",
        form=form,
        action="Delete",
        message=f"Confirm deletion of tag {tag} for {profile.name}.",
    )

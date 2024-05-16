import json
from flask_wtf import FlaskForm
from wtforms import (
    DecimalField,
    EmailField,
    StringField,
    SelectField,
    TextAreaField,
    SubmitField,
    BooleanField,
    HiddenField,
    RadioField,
    IntegerField,
    FieldList,
    FormField,
    DateField,
)
from wtforms.form import BaseForm
from wtforms.validators import DataRequired, Length, NumberRange, Email


class UserProfileForm(FlaskForm):
    given_name = StringField("Given Name", validators=[Length(min=0, max=80)])
    family_name = StringField("Family Name", validators=[Length(min=0, max=80)])
    middle_name = StringField("Middle Name", validators=[Length(min=0, max=80)])
    nickname = StringField("Nickname", validators=[Length(min=0, max=80)])
    birthdate = DateField("Birthdate")
    pronouns = StringField("Pronouns", validators=[Length(min=0, max=80)])


class ProfileSocialMediaForm(FlaskForm):
    handle = StringField("Handle", validators=[Length(min=0, max=80)])
    platform_id = SelectField("Platform", coerce=str)


class ProfileTelephoneNumbersForm(FlaskForm):
    phone = StringField("Phone", validators=[Length(min=0, max=80)])
    is_default = BooleanField("Default")


class UserProfileEmailsForm(FlaskForm):
    email = EmailField("Email", validators=[DataRequired(), Length(min=1, max=80)])
    is_default = BooleanField("Default")


class ProfileLocationsForm(FlaskForm):
    street1 = StringField("Street 1", validators=[Length(min=0, max=80)])
    street2 = StringField("Street 2", validators=[Length(min=0, max=80)])
    city = StringField("City", validators=[Length(min=0, max=80)])
    state = StringField("State", validators=[Length(min=0, max=80)])
    postal_code = StringField("Postal Code", validators=[Length(min=0, max=80)])
    country = StringField("Country", validators=[Length(min=0, max=80)])
    is_default = BooleanField("Default")


class ProfileRatingChangeForm(FlaskForm):
    change = StringField("Change", validators=[Length(min=0, max=10240)])
    social_rating_change = IntegerField("Social Credit Score Change", validators=[NumberRange(min=0)], default=0)
    antisocial_rating_change = IntegerField("Anti-social credit Score Change", validators=[NumberRange(min=0)], default=0)


class UserProfileAdminForm(UserProfileForm):
    is_active = BooleanField("Active")
    user_since = DateField("User Since")
    is_admin = BooleanField("Admin")


class OrganizationProfileForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired(), Length(min=1, max=80)])
    dba = StringField("DBA", validators=[Length(min=0, max=2048)])
    is_active = BooleanField("Active")


class OrganizationContactUserForm(FlaskForm):
    given_name = StringField("Given Name", validators=[Length(min=0, max=80)])
    family_name = StringField("Family Name", validators=[Length(min=0, max=80)])
    middle_name = StringField("Middle Name", validators=[Length(min=0, max=80)])
    nickname = StringField("Nickname", validators=[Length(min=0, max=80)])


class DeleteConfirmForm(FlaskForm):
    confirm = SubmitField("Confirm Delete")


class ProfileTagsForm(FlaskForm):
    tag = SelectField("Tag", coerce=str, validators=[DataRequired()])

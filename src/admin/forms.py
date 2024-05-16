import json
from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    SelectField,
    IntegerField,
    DateTimeField,
)
from wtforms.form import BaseForm
from wtforms.validators import DataRequired, Length, NumberRange, Email


class AddRatingTypeForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired(), Length(min=1, max=80)])
    description = StringField("Description", validators=[Length(min=0, max=80)])
    start_score = IntegerField("Start Score")
    end_score = IntegerField("End Score")


class AddUserForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired(), Length(min=1, max=80)])


class DeleteUserForm(FlaskForm):
    confirm = StringField("Confirm", validators=[DataRequired(), Length(min=6, max=6)])


class AddOrganizationForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired(), Length(min=1, max=80)])


class AddBlockedUserForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired(), Length(min=1, max=80)])
    reason = StringField("Reason", validators=[Length(min=0, max=2048)])


class AddAccessTokenForm(FlaskForm):
    comment = StringField("Comment", validators=[Length(min=0, max=2048)])
    access_type = SelectField("Access Type", choices=[("read", "Read"), ("write", "Write")])


class AddTagForm(FlaskForm):
    name = StringField("Name", validators=[DataRequired(), Length(min=1, max=80)])
    description = StringField("Description", validators=[Length(min=0, max=80)])

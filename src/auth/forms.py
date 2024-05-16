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
    FormField
)
from wtforms.form import BaseForm
from wtforms.validators import DataRequired, Length, NumberRange, Email

class AForm(FlaskForm):
    pass
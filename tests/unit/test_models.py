# tests for src/models.py using pytest
import json
import pytest
from models import *
from auth.utils import requires_login_and_group
from flask import Blueprint, render_template, redirect, url_for, request, session
from urllib.parse import unquote
from main.forms import *
from app import app, config

mod = Blueprint("main", __name__, url_prefix="/")


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.app_context():
        yield app.test_client()

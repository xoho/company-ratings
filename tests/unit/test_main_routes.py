# tests for src/main/routes.py using pytest
import json
import os
import pytest
from models import *
from auth.utils import requires_login_and_group
from flask import Blueprint, render_template, redirect, url_for, request, session
from urllib.parse import unquote
from main.forms import *
from app import app, config

mod = Blueprint("main", __name__, url_prefix="/")


# tests for src/main/routes.py using pytest
@pytest.fixture
def client():
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = "secret"
    app.config["JWT_SECRET_KEY"] = "secret"
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = False
    app.config["JWT_COOKIE_SECURE"] = False
    app.config["JWT_TOKEN_LOCATION"] = ["cookies"]
    app.config["JWT_COOKIE_CSRF_PROTECT"] = False
    app.config["JWT_CSRF_CHECK_FORM"] = False
    app.config["JWT_CSRF_IN_COOKIES"] = False
    with app.app_context():
        yield app.test_client()


@pytest.fixture
def user(client):
    u = UserProfile(name="test", uid="foobarbaztest")
    u.groups.append("Users")
    return u


@pytest.fixture
def loggedin_client(client, user):
    with client.session_transaction() as sess:
        sess["username"] = user.name
        sess["groups"] = user.groups
    return client


def test_not_logged_in(client):
    response = client.get("/profile")
    assert response.status_code == 302
    assert response.headers["Location"] == "/auth/login"


def test_index(loggedin_client):
    response = loggedin_client.get("/")
    print(response.data)
    assert response.status_code == 302
    assert b"Redirecting" in response.data

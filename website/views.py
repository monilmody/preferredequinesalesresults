from flask import Blueprint, render_template

views = Blueprint(__name__, "views")

@views.route("/")
def home():
    return render_template("homepage.html")

@views.route("/index.html")
def index():
    return render_template("index.html")

@views.route("/keenland")
def run():
    return render_template("index.html")
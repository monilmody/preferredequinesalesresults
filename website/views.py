from flask import Blueprint, render_template

views = Blueprint(__name__, "views")

@views.route("/")
def home():
    return render_template("homepage.html")

@views.route("/keenland_redirect")
def keenlandRedirect():
    return render_template("keenland.html")

@views.route("/keenland")
def keenland():
    return render_template("keenland.html")

@views.route("/fasig_tipton_redirect")
def fasigTiptonRedirect():
    return render_template("fasigtipton.html")

@views.route("/fasigtipton")
def fasigTipton():
    return render_template("fasigtipton.html")

@views.route("/goffs_redirect")
def goffsRedirect():
    return render_template("goffs.html")

@views.route("/goffs")
def goffs():
    return render_template("goffs.html")

@views.route("/obs_redirect")
def obsRedirect():
    return render_template("obs.html")

@views.route("/obs")
def obs():
    return render_template("obs.html")

@views.route("/tattersalls_redirect")
def tattersallsRedirect():
    return render_template("tattersalls.html")

@views.route("/tattersalls")
def tattersalls():
    return render_template("tattersalls.html")

@views.route("/arquana_redirect")
def arquanaRedirect():
    return render_template("arquana.html")

@views.route("/arquana")
def arquana():
    return render_template("arquana.html")
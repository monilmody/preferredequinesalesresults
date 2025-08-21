from flask import Blueprint, render_template, send_file, current_app
import boto3
from io import BytesIO

# Initialize S3 client (IAM role for EC2 assumed)
s3_client = boto3.client('s3')
S3_BUCKET = 'horse-list-photos-and-details'

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

@views.route("/obs_redirect_old")
def obsRedirectOld():
    return render_template("obs-old.html")

@views.route("/obs-old")
def obs_old():
    return render_template("obs-old.html")

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

@views.route("/uploads/<filename>")
def download_file(filename):
    print(f"Received request for file: {filename}")  # Log request for debugging
    
    try:
        # Define the S3 file path
        s3_file_path = f"horse_data/{filename}"
        
        # Attempt to fetch the file from S3
        file_obj = BytesIO()
        s3_client.download_fileobj(S3_BUCKET, s3_file_path, file_obj)
        
        # Reset the pointer of the BytesIO object before returning
        file_obj.seek(0)
        
        print(f"File {filename} found in S3. Sending file.")  # Log when file is found
        
        # Send the file directly from the in-memory BytesIO object
        return send_file(file_obj, as_attachment=True, download_name=filename)

    except Exception as e:
        print(f"File {filename} not found in S3. Error: {str(e)}")  # Log when file is not found
        return f"File {filename} not found", 404
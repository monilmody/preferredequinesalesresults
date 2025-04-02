from flask import Flask
from flask import render_template, request
import numpy as np
from views import views
import pandas as pd
from datetime import datetime
from datetime import date
from sqlalchemy import text, Column, String, Date, Float, ForeignKey, Integer, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import time
from mysql.connector import Error as MySQLError
import sys
import re
import os
from werkzeug.utils import secure_filename

print(sys.executable)

app = Flask(__name__)
app.register_blueprint(views, url_prefix="/views")

Base = declarative_base()

# Define the table schema for tsales
class main_Tsales(Base):
    __tablename__ = 'tsales'
    __table_args__ = {'extend_existing': True}
    SALE_ID = Column(Integer, primary_key=True, autoincrement=True)
    SALEYEAR = Column(Integer)
    SALETYPE = Column(String(1))
    SALECODE = Column(String(20))
    SALEDATE = Column(Date)
    BOOK = Column(String(2))
    DAY = Column(Integer)
    HIP = Column(String(10))
    HIPNUM = Column(String(10))
    HORSE = Column(String(35))
    CHORSE = Column(String(35))
    RATING = Column(String(5))
    TATTOO = Column(String(6))
    DATEFOAL = Column(Date)
    AGE = Column(Integer)
    COLOR = Column(String(5))
    SEX = Column(String(3))
    GAIT = Column(String(3))
    TYPE = Column(String(3))
    RECORD = Column(String(25))
    ET = Column(String(1))
    ELIG = Column(String(3))
    BREDTO = Column(String(20))
    LASTBRED = Column(Date)
    CONSLNAME = Column(String(150))
    CONSNO = Column(String(20))
    PEMCODE = Column(String(15))
    PURFNAME = Column(String(30))
    PURLNAME = Column(String(70))
    SBCITY = Column(String(25))
    SBSTATE = Column(String(10))
    SBCOUNTRY = Column(String(15))
    PRICE = Column(Float)
    CURRENCY = Column(String(3))
    URL = Column(String(150))
    NFFM = Column(String(2))
    PRIVATESALE = Column(String(2))
    BREED = Column(String(2))
    YEARFOAL = Column(Integer)
    UTT = Column(String(255))
    STATUS = Column(String(255))
    TDAM = Column(String(255))
    tSire = Column(String(255))
    tSireofdam = Column(String(255))
    DAMSIRE_ID = Column(Integer, ForeignKey('tdamsire.DAMSIRE_ID'))
    tdamsire = relationship("main_Tdamsire", back_populates="tsales")

# Define the table schema for tdamsire
class main_Tdamsire(Base):
    __tablename__ = 'tdamsire'
    __table_args__ = {'extend_existing': True}
    DAMSIRE_ID = Column(Integer, primary_key=True, autoincrement=True)
    SIRE = Column(String(50))
    CSIRE = Column(String(50))
    DAM = Column(String(50))
    CDAM = Column(String(50))
    SIREOFDAM = Column(String(50))
    CSIREOFDAM = Column(String(50))
    DAMOFDAM = Column(String(50))
    CDAMOFDAM = Column(String(50))
    DAMTATT = Column(String(6))
    DAMYOF = Column(Integer, nullable=True, default=0)
    DDAMTATT = Column(String(6))
    tsales = relationship("main_Tsales", back_populates="tdamsire")

csv_data = pd.DataFrame({})

UPLOAD_FOLDER = 'uploads/'  # Path to the upload directory
# Set the UPLOAD_FOLDER in the app's configuration
app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')  # Or any path you prefer
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}  # Allowed file types: CSV and Excel

def allowed_file(filename):
    """Check if the uploaded file is either a CSV or Excel file."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def handle_file_upload(request):
    try:
        if not os.path.exists(UPLOAD_FOLDER):
            os.makedirs(UPLOAD_FOLDER)
        
        print(request.files)

        file = request.files['file']
        
        print("Filename:", file.filename)

        if file.filename == '':
            raise ValueError("No selected file")

        # Ensure the file has an allowed extension (CSV or Excel)
        if file and allowed_file(file.filename):
            # Use SALECODE as the filename (add .csv or .xlsx extension based on file type)
            filename = file.filename  # Use the original filename for now
            file_path = os.path.join(UPLOAD_FOLDER, filename)
             # Check if a file with the same SALECODE already exists
            if os.path.exists(file_path):
                os.remove(file_path)  # Remove the old file before saving the new one
            try:
                file.save(file_path)
                print(f"File successfully saved to: {file_path}")
            except Exception as e:
                print(f"Error saving file: {e}")

            # After saving, process or format the file (this can be any format operation you want)
          # Format the file (e.g., drop columns, modify data)
            return file_path
        else:
            raise ValueError("Invalid file type")

    except Exception as e:
        print(f"Error during file upload: {e}")
        raise e
    
def upload_data_to_mysql(df):
    global csv_data
    db_host = "preferredequinesalesresultsdatabase.cdq66kiey6co.us-east-1.rds.amazonaws.com"
    db_name = "horse"
    db_user = "preferredequine"
    db_pass = "914MoniMaker77$$"

    try:
        print("1")
        # Create a MySQL engine
        engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}?charset=utf8mb4", connect_args={"collation": "utf8mb4_general_ci"})

        # Create a session factory
        Session = sessionmaker(bind=engine)

        # Create a new session
        session = Session()

        file = request.files['file']

        # Extract the filename from the FileStorage object
        filename = file.filename

        # Get the base filename without extension (remove ".csv")
        filename_without_extension = os.path.splitext(filename)[0]
        print("2")
        # Check if the file (based on SALECODE) already exists in the database
        check_existing_file_sql = text("SELECT file_name FROM documents WHERE file_name = :file_name")
        result_existing_file = session.execute(check_existing_file_sql, {'file_name': filename_without_extension}).fetchone()

        if result_existing_file:
            # File exists, delete previous records and the file from the system
            delete_previous_records_sql = text("DELETE FROM documents WHERE file_name = :file_name")
            session.execute(delete_previous_records_sql, {'file_name': filename_without_extension})
            session.commit()

            # Delete the previous file from the file system
            for ext in ['.csv', '.xls', '.xlsx']:
                previous_file_path = os.path.join(UPLOAD_FOLDER, result_existing_file[0] + ext)
                if os.path.exists(previous_file_path):
                    os.remove(previous_file_path)

        # # Save the new file using the SALECODE as the filename (just a placeholder)
        # with open(file_path, 'w') as f:
        #     f.write("This is a placeholder file for SALECODE: " + sale_code)  # Example content

        # Insert file details into the database (with the current timestamp)
        insert_file_sql = text("""
            INSERT INTO documents (file_name, upload_date)
            VALUES (:file_name, NOW())
        """)
        session.execute(insert_file_sql, {'file_name': filename_without_extension})
        session.commit()
        print("3")

        # Define tables
        Base.metadata.create_all(engine)

        # Define the relationship after both classes have been defined
        # main_Tsales.tdamsire = relationship("main_Tdamsire", back_populates="tsales")

        # Define the columns you want to insert into each table
        columns_for_tsales = ["SALEYEAR", "SALETYPE", "SALECODE", "SALEDATE", "BOOK", "DAY", "HIP", "HIPNUM", "HORSE", "CHORSE", "RATING", "TATTOO", "DATEFOAL", "AGE", "COLOR", "SEX", "GAIT", "TYPE", "RECORD", "ET", "ELIG", "BREDTO", "LASTBRED", "CONSLNAME", "CONSNO", "PEMCODE", "PURFNAME", "PURLNAME", "SBCITY", "SBSTATE", "SBCOUNTRY", "PRICE", "CURRENCY", "URL", "NFFM", "PRIVATESALE", "BREED", "YEARFOAL", "UTT", "STATUS", "TDAM", "tSire", "tSireofdam"]
        columns_for_tdamsire = ["SIRE", "CSIRE", "DAM", "CDAM", "SIREOFDAM", "CSIREOFDAM", "DAMOFDAM", "CDAMOFDAM", "DAMTATT", "DAMYOF", "DDAMTATT"]

        for _, row in df.iterrows():
                    try:
                        # Insert into tdamsire first
                        tdamsire_data = {col: row[col] for col in columns_for_tdamsire}
                        tdamsire = main_Tdamsire(**tdamsire_data)
                        session.add(tdamsire)

                        # Use the generated DAMSIRE_ID in tsales
                        tsales_data = {col: row[col] for col in columns_for_tsales}
                        #tsales_data['DAMSIRE_ID'] = tdamsire.DAMSIRE_ID
                        tsales = main_Tsales(**tsales_data)
                        tsales.tdamsire = tdamsire
                        session.add(tsales)

                    except (MySQLError, Exception) as e:
                        session.rollback()
                        print(f"Error: {str(e)}")
                        time.sleep(2)  # Wait for a few seconds before retrying
                        continue

        session.commit()  # Commit all changes
        print("Data inserted successfully into both tsales and tdamsire tables.")

    except Exception as e:
        print(f"Error: {str(e)}")
        session.rollback()
    finally:
        session.close()
    return render_template("keenland.html", message=f'Data has been uploaded to the database successfully', data=df.to_html())

@app.route("/")
def home():
    return render_template("homepage.html")

@app.route('/keenland_redirect')
def keenlandRedirect():
    return render_template('keenland.html')

@app.route('/fasig_tipton_redirect')
def fasigTiptonRedirect():
    return render_template('fasigtipton.html')

@app.route('/goffs_redirect')
def goffsRedirect():
    return render_template('goffs.html')

@app.route('/obs_redirect')
def obsRedirect():
    return render_template('obs.html')

@app.route("/obs_redirect_old")
def obsRedirectOld():
    return render_template("obs-old.html")

@app.route('/tattersalls_redirect')
def tattersallsRedirect():
    return render_template('tattersalls.html')

@app.route('/arquana_redirect')
def arquanaRedirect():
    return render_template('arquana.html')

@app.route('/keenland', methods=['POST'])

def keenland():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('keenland.html', message='No file path')
        file = request.files['file']

        if file.filename == '':
            return render_template('keenland.html', message='No selected file')

        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path

        # Read the selected Excel file into a DataFrame
        df = pd.read_csv(file_path)

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        saleyear = request.form['saleyear']

        # Check if the user provided a salecode or canceled the dialog
        if saleyear is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            saleyear

        else:
            print("saleyear input canceled.")

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        saletype = 'Y'
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        if 'Book' in df.columns:
            df['BOOK'] = df['Book']
        else:
            df['BOOK'] = 1
        
        # # Adding a new column DAY
        # if 'SALEDATE' in df.columns:
        df['DAY'] = df['Session']
        
        # Function to update sale dates based on user input
        def update_sale_dates(df, sale_dates_input):
            sale_dates = [date.strip() for date in sale_dates_input.split(',')]
            # Convert the sale dates to datetime objects
            sale_date_objects = [datetime.strptime(date, '%Y-%m-%d') for date in sale_dates]
            for i, sale_date_obj in enumerate(sale_date_objects):
                day_increment = i + 1  # Increment the day by the index (starting from 1)
                for j, day in enumerate(df['DAY']):
                    if not pd.isnull(day):
                        if day == day_increment:
                            df.at[j, 'SALEDATE'] = sale_date_obj

        # Get sale dates from user input
        sale_dates_input = request.form['sale_dates']

        # Update sale dates
        update_sale_dates(df, sale_dates_input)
        print(df[['DAY', 'SALEDATE']])

        # Adding a new column HIP
        df['HIP'] = df['Hip']

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['Hip']

        # Dropping a column HIP1
        if 'HIP1' in df.columns:
            df.drop(columns=['HIP1'], inplace=True)

        default_horse = "No Horse"
        df['HORSE'] = df['Horse Name'].fillna(default_horse)

        default_horse = "No Horse"
        df['CHORSE'] = df['Horse Name'].fillna(default_horse)

        # Check if 'NAME' is a column in the DataFrame
        if 'Horse Name' in df.columns:
                    # Dropping a column NAME
                    df.drop(columns=['Horse Name'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        default_date = pd.to_datetime('1900-01-01')  # Choose your default date
        df['DATEFOAL'] = pd.to_datetime(df['DOB'], errors='coerce').fillna(default_date)

        # Function to calculate the age from DATEFOAL
        def calculate_age(datefoal):
            today = date.today()
            born = pd.to_datetime(datefoal, errors='coerce')  # Convert to datetime, handle invalid dates
            age = today.year - born.dt.year - ((today.month * 100 + today.day) < (born.dt.month * 100 + born.dt.day))
            return age

        # Calling the calculate_age() function
        age = calculate_age(df['DATEFOAL'])

        # Adding a new column AGE
        df['AGE'] = age

        color_mapping = {
            'BAY': 'B',
            'CHESTNUT': 'CH',
            'Dark Bay/Brown': 'BR',
            'Gray/Roan': 'GR',
            'DARK BAY/BROWN': 'BR',
            'GRAY/ROAN': 'GR',
            'Bay': 'B',
            'Chestnut': 'CH',
            'Black': 'BLK',
            'BLACK': 'BLK'
        }

        # Adding a new column COLOR
        df['COLOR'] = df['Color'].replace(color_mapping)

        sex_mapping = {
            'Colt': 'C',
            'Filly': 'F',
            'Mare': 'M',
            'Gelding': 'G',
            'Horse': 'H',
            'Ridgling': 'R'
        }
        # Adding a new column SEX
        if 'Sex' in df.columns:
            df['SEX'] = df['Sex'].replace(sex_mapping)
        else:
            df['SEX'] = ''

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        type_mapping = {
            'Racing or Broodmare Prospect': 'BRP',
            'Broodmare': 'B',
            'Weanling': 'W',
            'Broodmare Prospect': 'BP',
            'Stallion Prospect': 'SP',
            'Stallion': 'T',
            'YR': 'Y',
            'Yearling': 'Y',
            'Racing Prospect': 'RP',
            'Racing or Stallion Prospect': 'SRP'
        }
        # Adding a new column TYPE
        if 'SoldAs' in df.columns:
            df['TYPE'] = df['SoldAs'].replace(type_mapping).fillna('Y')
        elif 'Sold As' in df.columns:
            df['TYPE'] = df['Sold As'].replace(type_mapping).fillna('Y')
        elif 'Type' in df.columns:
            df['TYPE'] = df['Type'].replace(type_mapping).fillna('Y')
        else:
            df['TYPE'] = 'Y'

        if 'Type' in df.columns:
            df.drop(columns=['Type'], inplace=True)

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = df['Elig'].fillna("")

        df.drop(columns=['Elig'], inplace=True)

        # Adding a new column SIRE
        df['SIRE'] =  df['Sire']

        # Adding a new column CSIRE
        df['CSIRE'] = df['Sire']

        # Adding a new column DAM
        df['DAM'] = df['Dam']

        # Adding a new column CDAM
        df['CDAM'] = df['Dam']

        # Adding a new column SIREOFDAM
        if 'Broodmare Sire' in df.columns:
            df['SIREOFDAM'] = df['Broodmare Sire'].fillna("")
        elif 'Sire Of Dam' in df.columns:
            df['SIREOFDAM'] = df['Sire Of Dam'].fillna("")

        # Adding a new column CSIREOFDAM
        if 'Broodmare Sire' in df.columns:
            df['CSIREOFDAM'] = df['Broodmare Sire'].fillna("")
        elif 'Sire Of Dam' in df.columns:
            df['CSIREOFDAM'] = df['Sire Of Dam'].fillna("")

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        bredto = df['CoveringSire']
        df['BREDTO'] = bredto.fillna("")
            
        # Adding a new column LASTBRED
        if 'LastService' in df.columns:
            df['LASTBRED'] = pd.to_datetime(df['LastService']).fillna(pd.to_datetime("1901-01-01"))
            
        # Adding a new column CONLNAME
        if 'Consignor' in df.columns:
            df['CONSLNAME'] = df['Consignor']
        elif 'PropertyLine1' in df.columns:
            df['CONSLNAME'] = df['PropertyLine1']
        else:
            df['CONSLNAME'] = ''

        # Dropping a column PROPERTY LINE
        if 'PropertyLine1' in df.columns:
            df.drop(columns=['PropertyLine1'], inplace=True)
        elif 'Consignor' in df.columns:
            df.drop(columns=['Consignor'], inplace=True)
        elif 'Farm Name' in df.columns:
            df.drop(columns=['Farm Name'], inplace=True)

        # Adding a new column CONSNO
        consno = ''
        df['CONSNO'] = consno

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        # Adding a new column PURLNAME
        if 'Purchaser' in df.columns:
            df['PURLNAME'] = df['Purchaser']
        else: 
            df['PURLNAME'] = ''

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Ensure df is a valid DataFrame
        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("Input is not a valid DataFrame.")
        
        # Print initial DataFrame for debugging
        print("Initial DataFrame:")
        print(df.head())

        # Define the mapping for R.N.A.
        price_mapping = {
            '---': np.nan
        }
        
        df['PRICE'] = pd.to_numeric(df['Price'], errors='coerce')

        # Process each row
        for i, row in df.iterrows():
            if pd.isna(row['PRICE']) and 'R.N.A.' in row['Purchaser']:
                # Extract the price from R.N.A. entry
                match = re.search(r'\(([\d,]+)\)', row['Purchaser'])
                if match:
                    try:
                        rna_price = float(match.group(1).replace(',', ''))
                        # Set the PRICE for the current row to the extracted rna_price
                        df.at[i, 'PRICE'] = rna_price
                    except ValueError:
                        print(f"Failed to convert R.N.A. price to float: {match.group(1)} from row {i}")
                        rna_price = None
                else:
                    print(f"Failed to extract R.N.A. price from: {row['Purchaser']} in row {i}")

            elif pd.isna(row['PRICE']):
                # Populate 'PRICE' for missing entries based on the last valid R.N.A. price
                if rna_price is not None:
                    df.at[i, 'PRICE'] = rna_price

        if 'Price' in df.columns:
            df.drop(columns=['Price'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = '' 
        df['URL'] = url

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = ''
        df['PRIVATESALE'] = privatesale

        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Assuming df is your DataFrame
        if 'DOB' in df.columns:
            # Extracting year from DATEFOAL
            df['YEARFOAL'] = df['DATEFOAL'].dt.year
            # If 'yearfoal' input from HTML has values, prioritize it over DATEFOAL
            if 'yearfoal' in request.form and request.form['yearfoal']:
                yearfoal_value = request.form['yearfoal']
                df['YEARFOAL'] = yearfoal_value
        else:
            # If 'DOB' is not in df, fill YEARFOAL with '0000'
            df['YEARFOAL'] = df.fillna(0000)

        df['UTT'] = df['utt'].fillna(0.0)

        pragnancy_mapping = {
            'PRAGNANCY' : 'P',
            'NOT PRAGNANT' : 'B',
            'NOT MATED' : 'NM'
        }

        df['STATUS'] = df['Pregnancy'].replace(pragnancy_mapping).fillna("")

        # Adding a new column DAM
        df['TDAM'] = df['Dam']

        # Adding a new column SIRE
        df['tSire'] = df['Sire']

        # Adding a new column SIREOFDAM
        df['tSireofdam'] = df['Sire Of Dam'].fillna("")

        
        # Dropping columns Broodmare Sire and Sire Of Dam
        if 'Broodmare Sire' in df.columns:
            df.drop(columns=['Broodmare Sire'], inplace=True)
        elif 'Sire Of Dam' in df.columns:
            df.drop(columns=['Sire Of Dam'], inplace=True)

        # Dropping a column PURCHASER
        if 'Purchaser' in df.columns:
            df.drop(columns=['Purchaser'], inplace=True)

        df.drop(columns=['utt'], inplace=True)

        # Dropping a column SOLD AS CODE
        if 'SOLD AS CODE' in df.columns:
            df.drop(columns=['SOLD AS CODE'], inplace=True)

        if 'Session' in df.columns:
            df.drop(columns=['Session'], inplace=True)

        if 'Book' in df.columns:
            df.drop(columns=['Book'], inplace=True)    

        if 'Hip' in df.columns:
            df.drop(columns=['Hip'], inplace=True)

        if 'PropertyLine2' in df.columns:
            df.drop(columns=['PropertyLine2'], inplace=True)

        if 'Description' in df.columns:
            df.drop(columns=['Description'], inplace=True)

        if 'Farm Name' in df.columns:
            df.drop(columns=['Farm Name'], inplace=True)

        df.drop(columns=['DOB'], inplace=True)

        df.drop(columns=['Color'], inplace=True)

        df.drop(columns=['Sex'], inplace=True)

        df.drop(columns=['Sire'], inplace=True)

        df.drop(columns=['Dam'], inplace=True)

        if 'CoveringSire' in df.columns:
            df.drop(columns=['CoveringSire'], inplace=True)
        elif 'Covering Sire' in df.columns:
            df.drop(columns=['Covering Sire'], inplace=True)

        if 'Breeding Status' in df.columns:
            df.drop(columns=['Breeding Status'], inplace=True)
            
        if 'Out' in df.columns:
            df.drop(columns=['Out'], inplace=True)

        if 'Location' in df.columns:
            df.drop(columns=['Location'], inplace=True)

        if 'LastService' in df.columns:
            df.drop(columns=['LastService'], inplace=True)

        if 'Pregnancy' in df.columns:
            df.drop(columns=['Pregnancy'], inplace=True)

        if 'SoldAs' in df.columns:
            df.drop(columns=['SoldAs'], inplace=True)
        elif 'Sold As' in df.columns:
            df.drop(columns=['Sold As'], inplace=True)

        if 'Price' in df.columns:
            df.drop(columns=['Price'], inplace=True)

        if 'Breeders Cup Eligible' in df.columns:
            df.drop(columns=['Breeders Cup Eligible'], inplace=True)
            
        print("reached here 1")
        # Save the formatted file back to the server
        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_csv(formatted_file_path, index=False)  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)
        # Engine.execute("COMMIT;") 
        print("reached here 2")
        return render_template("keenland.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("keenland.html", message=f'Error: {str(e)}', data=None)

@app.route('/fasigtipton', methods=['POST'])
# Check if the user selected a file or canceled the dialog
def fasigTipton():

    global csv_data
    try:

        if 'file' not in request.files:
            return render_template('fasigtipton.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('fasigtipton.html', message='No selected file')
        
        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path
        
        # Read the selected Excel file into a DataFrame
        df = pd.read_csv(file_path)

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        # Renaming HIP to HIP1
        df.rename(columns={'HIP': 'HIP1'}, inplace=True)

        # Renaming PRICE to PRICE1
        df.rename(columns={'PRICE': 'PRICE1'}, inplace=True)

        # Renaming COLOR to COLOR1
        df.rename(columns={'COLOR': 'COLOR1'}, inplace=True)

        # Renaming SEX to SEX1
        df.rename(columns={'SEX': 'SEX1'}, inplace=True)

        # Renaming SIRE to SIRE1
        df.rename(columns={'SIRE': 'SIRE1'}, inplace=True)

        # Renaming DAM to DAM1
        df.rename(columns={'DAM': 'DAM1'}, inplace=True)

        # Adding a new column SALEYEAR
        df['SALEYEAR'] = request.form['saleyear']

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        if 'SESSION' in df.columns:
            # Convert 'SESSION' to datetime, allowing time or just the date
            df['SALEDATE'] = pd.to_datetime(df['SESSION'], format='%m/%d/%Y %H:%M', errors='coerce')  # with time format

            # If the time format does not match, try to convert it just to date
            df['SALEDATE'] = pd.to_datetime(df['SESSION'], format='%m/%d/%Y', errors='coerce').fillna(df['SALEDATE'])

            # Extract only the date part (ignoring time) if time exists
            df['SALEDATE'] = df['SALEDATE'].dt.date

            # Format 'SALEDATE' to 'YYYY-MM-DD' for MySQL compatibility
            df['SALEDATE'] = df['SALEDATE'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Initialize a counter
        counter = 0

        # Initialize a list to store the counter values
        counter_values = []

        # Iterate through the list of dates
        prev_date = None  # Initialize the previous date variable
        for i, date_str in enumerate(df['SALEDATE']):
            try:
                # Use the correct date format here (now the column should be in 'YYYY-MM-DD' format)
                date1 = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Convert the datetime object back to string for consistency
                formatted_date_str = date1.strftime('%Y-%m-%d')
                
                # Check if this is the first date or if the date has changed from the previous one
                if i == 0 or formatted_date_str != prev_date:
                    counter += 1  # Increment the counter when the date changes
                    prev_date = formatted_date_str  # Update the previous date
                
                counter_values.append(counter)
            except ValueError:
                print(f"Invalid date format or value at index {i}: {date_str}")


        # Adding a new column DAY
        df['DAY'] = counter_values

        # Dropping a column SESSION
        if 'SESSION' in df.columns:
            df.drop(columns=['SESSION'], inplace=True)

        # Adding a new column HIP
        if 'HIP1' in df.columns:
            df['HIP'] = df['HIP1']

        # Adding a new column HIPNUM
        if 'HIP1' in df.columns:
            df['HIPNUM'] = df['HIP1']

        # Dropping a column HIP1
        if 'HIP1' in df.columns:
            df.drop(columns=['HIP1'], inplace=True)

        # Fill NaN and empty strings in 'NAME' and create 'HORSE' and 'CHORSE'
        df['HORSE'] = df['NAME'].fillna("No Horse").replace('', 'No Horse')
        df['CHORSE'] = df['NAME'].fillna("No Horse").replace('', 'No Horse')
            
        # Drop the 'NAME' column after processing
        if 'NAME' in df.columns:
            df.drop(columns=['NAME'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        # Adding a new column DATEFOAL
        if 'YEAR OF BIRTH' in df.columns:
            def process_year_of_birth(year_of_birth):
                # Convert the value to string to ensure consistency
                year_of_birth_str = str(year_of_birth)
                
                # If the value is just a year (length 4), convert to int and set default date
                if len(year_of_birth_str) == 4 and year_of_birth_str.isdigit():
                    year = int(year_of_birth_str)  # Convert to int
                    datefoal = pd.to_datetime('1900-01-01')  # Default date for year-only entries
                else:
                    # Otherwise, it's a full date, so convert it to datetime
                    year = pd.to_datetime(year_of_birth_str, errors='coerce').year
                    datefoal = pd.to_datetime(year_of_birth_str, errors='coerce')  # Full date
                
                return year, datefoal

            # Apply the processing function
            df[['YEARFOAL', 'DATEFOAL']] = df['YEAR OF BIRTH'].apply(lambda x: pd.Series(process_year_of_birth(x)))

        # Dropping a column YEAR OF BIRTH
        if 'YEAR OF BIRTH' in df.columns:
            df.drop(columns=['YEAR OF BIRTH'], inplace=True)

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        # df['YEARFOAL'] = df['DATEFOAL'].dt.year
        
        # Function to calculate the age from yearfoal and datefoal
        def calculate_age(yearfoal, saleyear):
            born_year = pd.to_numeric(yearfoal, errors='coerce')  # Convert to numeric, handle invalid values
            sale_year = pd.to_numeric(saleyear, errors='coerce')  # Convert to numeric, handle invalid values
            age = sale_year - born_year
            return age
        
        # Calling the calculate_age() function with yearfoal and datefoal
        age = calculate_age(df['YEARFOAL'], df['SALEYEAR'])
        
        # Adding a new column AGE
        df['AGE'] = age

        # Adding a new column COLOR
        if 'COLOR1' in df.columns:
            df['COLOR'] = df['COLOR1'].fillna("")

        # Dropping a column COLOR1
        if 'COLOR1' in df.columns:
            df.drop(columns=['COLOR1'], inplace=True)

        # Adding a new column SEX
        if 'SEX1' in df.columns:
            df['SEX'] = df['SEX1']

        # Dropping a column SEX1
        if 'SEX1' in df.columns:
            df.drop(columns=['SEX1'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        # Adding a new column TYPE
        if 'SOLD AS CODE' in df.columns:
            df['TYPE'] = df['SOLD AS CODE']
        else:
            # Check if the 'AGE' column exists and apply the condition for AGE == 2
            if 'AGE' in df.columns:
                df['TYPE'] = df['AGE'].apply(lambda x: 'R' if x == 2 else 'Y')
            else:
                df['TYPE'] = "Y"  # If 'AGE' column doesn't exist, set TYPE to empty string

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Adding sate_mapping key: value pair to replace the value
        state_mapping = {
            'KENTUCKY': 'KY',
            'NEW YORK': 'NY',
            'PENNSYLVANIA': 'PA',
            'FLORIDA': 'FL',
            'ONTARIO': 'ON',
            'VIRGINIA': 'VA',
            'LOUISIANA': 'LA',
            'MARYLAND': 'MD',
            'ARKANSAS': 'AR',
            'INDIANA': 'IN',
            'OHIO': 'OH',
            'CALIFORNIA': 'CA',
            'TEXAS': 'TX',
            'IOWA': 'IA',
            'NEW MEXICO': 'NM',
            'MINNESOTA': 'MN',
            'OKLAHOMA': 'OK',
            'ILLINOIS': 'IL',
            'NEW JERSEY': 'NJ',
            'ALABAMA': 'AL',
            'COLORADO': 'CO',
            'GEORGIA': 'GA',
            'MISSOURI': 'MO',
            'WEST VIRGINIA': 'WV',
            'MISSISSIPPI': 'MS',
            'TENNESSEE': 'TN',
            'MASSACHUSETTS': 'MA',
            'KANSAS': 'KS',
            'CONNECTICUT': 'CT',
            'NORTH CAROLINA': 'NC',
            'SOUTH CAROLINA': 'SC',
            'DELAWARE': 'DE',
            'MICHIGAN': 'MI',
            'WISCONSIN': 'WI',
            'NEVADA': 'NV',
            'NEBRASKA': 'NE',
            'UTAH': 'UT',
            'IDAHO': 'ID',
            'BRITISH COLUMBI': 'BC',
            'AUS': 'AU'
        }

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = df['FOALED'].replace(state_mapping)

        # Adding a new column SIRE
        if 'SIRE1' in df.columns:
            df['SIRE'] =  df['SIRE1']

        # Adding a new column CSIRE
        if 'SIRE1' in df.columns:
            df['CSIRE'] = df['SIRE1']

        # Adding a new column DAM
        if 'DAM1' in df.columns:
            df['DAM'] = df['DAM1']

        # Adding a new column CDAM
        if 'DAM1' in df.columns:
            df['CDAM'] = df['DAM1']

        # Adding a new column SIREOFDAM
        if 'SIRE OF DAM' in df.columns:
            df['SIREOFDAM'] = df['SIRE OF DAM']

        # Adding a new column CSIREOFDAM
        if 'SIRE OF DAM' in df.columns:
            df['CSIREOFDAM'] = df['SIRE OF DAM']

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'COVERING SIRE' in df.columns:
            df['BREDTO'] = df['COVERING SIRE'].fillna("")
        else:
            df['BREDTO'] = ""

        # Dropping a column CONSIGNOR NAME
        if 'COVERING SIRE' in df.columns:
            df.drop(columns=['COVERING SIRE'], inplace=True)

        if 'COVER DATE' in df.columns:
            df['LASTBRED'] = pd.to_datetime(df['COVER DATE']).fillna(pd.to_datetime("1901-01-01"))
        
        # Adding a new column CONLNAME
        conlname = df['PROPERTY LINE']
        df['CONSLNAME'] = conlname

        # Dropping a column PROPERTY LINE
        df.drop(columns=['PROPERTY LINE'], inplace=True)

        # Adding a new column CONSNO
        consno = ''
        df['CONSNO'] = consno

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        # Adding a new column PURLNAME
        purlname = df['PURCHASER']
        df['PURLNAME'] = purlname

        # Dropping a column PURCHASER
        df.drop(columns=['PURCHASER'], inplace=True)

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Adding a new column PRICE
        price = df['PRICE1']
        df['PRICE'] = price

        # Adding a new column PRICE1
        df.drop(columns=['PRICE1'], inplace=True)

        # Adding a new column SALE TITLE
        if "SALE TITLE" in df.columns:
            df.drop(columns=['SALE TITLE'], inplace=True)

        if "SALE_TITLE" in df.columns:
            df.drop(columns=['SALE_TITLE'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        if "URL" in df.columns:
            df.drop(columns=['URL'], inplace=True)

        # Adding a new column URL
        url = df['VIRTUAL INSPECTION'] 
        df['URL'] = url.fillna('')

        # Dropping a column VIRTUAL INSPECTION
        df.drop(columns=['VIRTUAL INSPECTION'], inplace=True)

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = df['PRIVATE SALE']
        df['PRIVATESALE'] = privatesale.fillna('')
            
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        df['UTT'] = df['utt'].fillna(0.0)

        df.drop(columns=['utt'], inplace=True)

        df['STATUS'] = ""

        df['TDAM'] = df['DAM1']
        
        df['tSire'] = df['SIRE1']

        df['tSireofdam'] = df['SIRE OF DAM']
        
        if 'SIRE1' in df.columns:
            df.drop(columns=['SIRE1'], inplace=True)

        # Dropping a column DAM1
        if 'DAM1' in df.columns:
            df.drop(columns=['DAM1'], inplace=True)

        # Dropping a column SIRE1
        if 'SIRE1' in df.columns:
            df.drop(columns=['SIRE1'], inplace=True)

        # Dropping a column SIRE OF DAM
        if 'SIRE OF DAM' in df.columns:
            df.drop(columns=['SIRE OF DAM'], inplace=True)

        df['url-ut'] = df['url ut'].fillna('')

        df.drop(columns=['url ut'], inplace=True)

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Dropping a column BARN
        if 'BARN' in df.columns:
            df.drop(columns=['BARN'], inplace=True)

        # Dropping a column COVER DATE
        if 'COVER DATE' in df.columns:
            df.drop(columns=['COVER DATE'], inplace=True)

        # Dropping a column SOLD AS CODE
        if 'SOLD AS CODE' in df.columns:
            df.drop(columns=['SOLD AS CODE'], inplace=True)

        if 'COVERING SIRE' in df.columns:
            df.drop(columns=['COVERING SIRE'], inplace=True)

        if 'CONSIGNOR NAME' in df.columns:
            df.drop(columns=['CONSIGNOR NAME'], inplace=True)

        # Dropping a column SOLD AS DESCRIPTION
        if 'SOLD AS DESCRIPTION' in df.columns:
            df.drop(columns=['SOLD AS DESCRIPTION'], inplace=True)

        # Dropping a column FOALED
        if 'FOALED' in df.columns:
            df.drop(columns=['FOALED'], inplace=True)

        # Dropping a column PRIVATE SALE
        if 'PRIVATE SALE' in df.columns:
            df.drop(columns=['PRIVATE SALE'], inplace=True)

        # Save the formatted file back to the server
        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_csv(formatted_file_path, index=False)  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)

        return render_template("fasigtipton.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("fasigtipton.html", message=f'Error: {str(e)}', data=None)
    
@app.route('/goffs', methods=['POST'])

def goffs():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('goffs.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('goffs.html', message='No selected file')
        
        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path
        
        # Read the selected Excel file into a DataFrame
        df = pd.read_excel(file_path)

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        # Adding a new column SALEYEAR
        saleyear = int(request.form['saleyear'])  # Convert saleyear to an integer
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        df['SALEDATE'] = request.form['sale_dates']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Adding a new column DAY
        day = 1
        df['DAY'] = day

        # Dropping a column SESSION
        if 'SESSION' in df.columns:
            df.drop(columns=['SESSION'], inplace=True)

        # Adding a new column HIP
        df['HIP'] = df['Lot'].fillna('')

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['Lot'].fillna('')

        # Dropping a column HIP1
        if 'Lot' in df.columns:
            df.drop(columns=['Lot'], inplace=True)

        # Create a new 'HORSE' column and populate it with 'NAME'
        if 'Name' in df.columns:
            df['HORSE'] = df['Name']

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
            # Create a new 'HORSE' column and populate it with 'NAME'
            df['CHORSE'] = df['Name']

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
            # Dropping a column NAME
            df.drop(columns=['Name'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        # Adding a new column LASTBRED
        datefoal = '1901-01-01'
        df['DATEFOAL'] = pd.to_datetime(datefoal)

        # Adding a new column AGE
        age = 0
        df['AGE'] = age

        # Adding a new column COLOR
        color = ''
        df['COLOR'] = color

        # Adding a new column SEX
        if 'Sex' in df.columns:
            df['SEX'] = df['Sex']

        # Dropping a column SEX1
        if 'Sex' in df.columns:
            df.drop(columns=['Sex'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        condition_covered_by = df['Covering Sire'].notna()
        # condition_foal = df['Produit'] == 'foal'
        condition_weanling = df['Year'] == saleyear
        condition_datefoal = df['Year'] == (saleyear - 1)
        # Define choices
        choices = np.select(
            [condition_covered_by, condition_weanling, condition_datefoal],
            ['B', 'W', 'Y'],
            default=''
        )

        # Assign the result to the 'TYPE' column
        df['TYPE'] = choices

        # Condition 1: If 'Covering Sire' is not null, set 'B'
        df.loc[df['Covering Sire'].notnull(), 'TYPE'] = 'B'

        # Condition 2: If 'Year' is 2022 or more, set 'Y'
        df.loc[df['Year'] >= 2022, 'TYPE'] = 'Y'

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        elig = ''
        df['ELIG'] = elig

        # Adding a new column SIRE
        if 'Sire' in df.columns:
            df['SIRE'] =  df['Sire']

        # Adding a new column CSIRE
        if 'Sire' in df.columns:
            df['CSIRE'] = df['Sire']

        # Adding a new column DAM
        if 'Dam' in df.columns:
            df['DAM'] = df['Dam']

        # Adding a new column CDAM
        if 'Dam' in df.columns:
            df['CDAM'] = df['Dam']

        # Adding a new column SIREOFDAM
        if 'SIRE OF DAM' in df.columns:
            df['SIREOFDAM'] = df['SIRE OF DAM']
        else:
            df['SIREOFDAM'] = ''

        # Adding a new column CSIREOFDAM
        if 'SIRE OF DAM' in df.columns:
            df['CSIREOFDAM'] = df['SIRE OF DAM']
        else:
            df['CSIREOFDAM'] = ''

        # Dropping a column SIRE OF DAM
        if 'SIRE OF DAM' in df.columns:
            df.drop(columns=['SIRE OF DAM'], inplace=True)

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'Covering Sire' in df.columns:
            df['BREDTO'] = df['Covering Sire'].fillna("")
        else:
            df['BREDTO'] = ""

        # Adding a new column LASTBRED
        lastbred = '1901-01-01'
        df['LASTBRED'] = pd.to_datetime(lastbred)

        # Adding a new column CONLNAME
        if 'Consignor' in df.columns:
            df['CONSLNAME'] = df['Consignor']

        # Dropping a column CONSIGNOR NAME
        if 'Consignor' in df.columns:
            df.drop(columns=['Consignor'], inplace=True)

        # Adding a new column CONSNO
        consno = ''
        df['CONSNO'] = consno

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        # Adding a new column PURLNAME
        purlname = df['Purchaser']
        df['PURLNAME'] = purlname.fillna('')

        # Dropping a column PURCHASER
        df.drop(columns=['Purchaser'], inplace=True)

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Adding a new column PRICE
        price = df['Price']
        df['PRICE'] = price.fillna(0.0)

        # Adding a new column PRICE1
        df.drop(columns=['Price'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = ''
        df['URL'] = url

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = ''
        df['PRIVATESALE'] = privatesale

        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['Year']

        df['UTT'] = 0.0
        df['STATUS'] = ""
        # Dropping a column BARN
        df.drop(columns=['Year'], inplace=True)

        if 'Dam' in df.columns:
            df['TDAM'] = df['Dam']

        # Dropping a column DAM1
        if 'Dam' in df.columns:
            df.drop(columns=['Dam'], inplace=True)
        
        if 'Sire' in df.columns:
            df['tSire'] = df['Sire']

        # Dropping a column DAM1
        if 'Sire' in df.columns:
            df.drop(columns=['Sire'], inplace=True)

        df['tSireofdam'] = ''

        # Dropping a column SOLD AS CODE
        if 'Stabling' in df.columns:
            df.drop(columns=['Stabling'], inplace=True)

        if 'Status' in df.columns:
            df.drop(columns=['Status'], inplace=True)

        df.drop(columns=['Covering Sire'], inplace=True)

        # Save the formatted file back to the server
        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_excel(formatted_file_path, index=False)  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)

        return render_template("goffs.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("goffs.html", message=f'Error: {str(e)}', data=None)
    
@app.route('/obs', methods=['POST'])

def obs():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('obs.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('obs.html', message='No selected file')
        
        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path
        
        # Read the selected Excel file into a DataFrame
        df = pd.read_csv(file_path)

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        # Adding a new column SALEYEAR
        df['SALEYEAR'] = request.form['saleyear']

        df['SALEYEAR'] = df['SALEYEAR'].astype(int)

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Adding a new column HIP
        df['HIP'] = df['hip_number']

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['hip_number']

        # Function to update sale dates and corresponding day column based on user input
        def update_sale_dates(df, sale_dates_input, hip_ranges_input):
            sale_dates = [date.strip() for date in sale_dates_input.split(',')]
            hip_ranges = [range.strip() for range in hip_ranges_input.split(',')]
            
            # Convert the sale dates to datetime objects
            sale_date_objects = [datetime.strptime(date, '%Y-%m-%d').date() for date in sale_dates]
            
            # Initialize a dictionary to store hip ranges and corresponding sale dates and days
            hip_range_data = {}
            for i, hip_range in enumerate(hip_ranges):
                start, end = map(int, hip_range.split('-'))
                for hip_number in range(start, end + 1):
                    hip_range_data[hip_number] = {'sale_date': sale_date_objects[i], 'day': i + 1}
            
            # Initialize DAY column to 0
            df['DAY'] = 0
            
            # Update SALEDATE and DAY columns
            for hip_number, data in hip_range_data.items():
                df.loc[df['hip_number'] == hip_number, 'SALEDATE'] = data['sale_date']
                df.loc[df['hip_number'] == hip_number, 'DAY'] = data['day']

        # Get sale dates from user input
        sale_dates_input = request.form['sale_dates']
        hip_ranges_input = request.form['hip_ranges']

        # Update sale dates and corresponding day column
        update_sale_dates(df, sale_dates_input, hip_ranges_input)

        # Dropping a column hip_number
        df.drop(columns=['hip_number'], inplace=True)

        # Dropping a column in_out_status
        df.drop(columns=['in_out_status'], inplace=True)

        # Check if 'NAME' is a column in the DataFrame
        if 'horse_name' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['HORSE'] = df['horse_name'].fillna("")
        else:
            df['HORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'horse_name' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['CHORSE'] = df['horse_name'].fillna("")
        else:
            df['CHORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'horse_name' in df.columns:
                    # Dropping a column NAME
                    df.drop(columns=['horse_name'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        # Adding a new column DATEFOAL
        df['DATEFOAL'] = pd.to_datetime(df['foaling_date'])

        # Dropping a column Foaling Date
        df.drop(columns=['foaling_date'], inplace=True)

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = datefoal_series.dt.year.fillna(1901)

        def calculate_age(yearfoal, saleyear):
            # Calculate age as the difference between sale year and foaling year, plus 1
            age = saleyear - yearfoal
            return age

        # Calling the calculate_age() function
        age = calculate_age(df['YEARFOAL'], df['SALEYEAR'])

        # Adding a new column AGE
        df['AGE'] = age.fillna(0)

        # Adding a new column COLOR
        if 'color' in df.columns:
            df['COLOR'] = df['color'].fillna("")

        # Dropping a column COLOR1
        if 'color' in df.columns:
            df.drop(columns=['color'], inplace=True)

        # Adding a new column SEX
        if 'sex' in df.columns:
            df['SEX'] = df['sex'].fillna("")

        # Dropping a column SEX1
        if 'sex' in df.columns:
            df.drop(columns=['sex'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        # Adding a new column TYPE
        df['TYPE'] = df['horsetype'].fillna("R")

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = df['foaling_area'].fillna("")

        df.drop(columns=['foaling_area'], inplace=True)

        # Adding a new column SIRE
        if 'sire_name' in df.columns:
            df['SIRE'] =  df['sire_name'].fillna("")

        # Adding a new column CSIRE
        if 'sire_name' in df.columns:
            df['CSIRE'] = df['sire_name'].fillna("")

        # Adding a new column DAM
        if 'dam_name' in df.columns:
            df['DAM'] = df['dam_name'].fillna("")

        # Adding a new column CDAM
        if 'dam_name' in df.columns:
            df['CDAM'] = df['dam_name'].fillna("")

        # Adding a new column SIREOFDAM
        if 'dam_sire' in df.columns:
            df['SIREOFDAM'] = df['dam_sire'].fillna("")

        # Adding a new column CSIREOFDAM
        if 'dam_sire' in df.columns:
            df['CSIREOFDAM'] = df['dam_sire'].fillna("")

        df.drop(columns=['sort_dam'], inplace=True)
        df.drop(columns=['property_line_2'], inplace=True)
        df.drop(columns=['consignor_sort'], inplace=True)
        df.drop(columns=['outdate'], inplace=True)

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        bredto = df['bredto']
        df['BREDTO'] = bredto.fillna("")

        # Dropping a column PROPERTY LINE
        df.drop(columns=['bredto'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = df['lastbred'] 
        df['LASTBRED'] = lastbred.fillna(pd.to_datetime('1901-01-01'))

        # Adding a new column CONLNAME
        conlname = df['property_line_1']
        df['CONSLNAME'] = conlname.fillna("")

        # Dropping a column PROPERTY LINE
        df.drop(columns=['property_line_1'], inplace=True)

        # Adding a new column CONSNO
        consno = ''
        df['CONSNO'] = consno

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        df.drop(columns=['barn_number'], inplace=True)

        # Adding a new column PURLNAME
        purlname = df['buyer_name']
        df['PURLNAME'] = purlname

        # Dropping a column PURCHASER
        df.drop(columns=['buyer_name'], inplace=True)

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Adding a new column PRICE
        # Fill NaN values with an empty int 0.0
        df['PRICE'] = df['hammer_price'].fillna(0.0)

        # Adding a new column PRICE1
        df.drop(columns=['hammer_price'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        
        url = df['pp_pdf_link']
        df['URL'] = url.fillna("")

        df.drop(columns=['pp_pdf_link'], inplace=True)

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = df['post_sale_indicator']
        df['PRIVATESALE'] = privatesale.fillna("")

        df.drop(columns=['post_sale_indicator'], inplace=True)
      
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        df.drop(columns=['foaling_year'], inplace=True)

        utt_mapping = {
            'G': 0.0
        }

        df['UTT'] = df['ut_time'].replace(utt_mapping).fillna(0.0)
        
        df.drop(columns=['ut_time'], inplace=True)

        df['STATUS'] = ""

        df['tSire'] =  df['sire_name'].fillna("")
        df['TDAM'] = df['dam_name'].fillna("")
        df['tSireofdam'] = df['dam_sire'].fillna("")

        # Dropping a column SIRE1
        if 'sire_name' in df.columns:
            df.drop(columns=['sire_name'], inplace=True)

        # Dropping a column DAM1
        if 'dam_name' in df.columns:
            df.drop(columns=['dam_name'], inplace=True)

        # Dropping a column DAM SIRE
        if 'dam_sire' in df.columns:
            df.drop(columns=['dam_sire'], inplace=True)

        df.drop(columns=['ut_distance'], inplace=True)
        df.drop(columns=['ut_actual_date'], inplace=True)
        df.drop(columns=['ut_group'], inplace=True)
        df.drop(columns=['ut_set'], inplace=True)
        df.drop(columns=['lastbred'], inplace=True)
        df.drop(columns=['horsetype'], inplace=True)

        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_csv(formatted_file_path, index=False)  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)

        return render_template("obs.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("obs.html", message=f'Error: {str(e)}', data=None)

@app.route('/obs-old', methods=['POST'])

def obs_old():
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('obs-old.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('obs-old.html', message='No selected file')
        
        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path
        
        file_extension = os.path.splitext(file_path)[1].lower()

        # Read the selected Excel file into a DataFrame
        if file_extension in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        # Adding a new column SALEYEAR
        df['SALEYEAR'] = request.form['saleyear']

        df['SALEYEAR'] = df['SALEYEAR'].astype(int)

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Adding a new column HIP
        df['HIP'] = df['Hip']

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['Hip']

        # Function to update sale dates and corresponding day column based on user input
        def update_sale_dates(df, sale_dates_input, hip_ranges_input):
            sale_dates = [date.strip() for date in sale_dates_input.split(',')]
            hip_ranges = [range.strip() for range in hip_ranges_input.split(',')]
            
            # Convert the sale dates to datetime objects
            sale_date_objects = [datetime.strptime(date, '%Y-%m-%d').date() for date in sale_dates]
            
            # Initialize a dictionary to store hip ranges and corresponding sale dates and days
            hip_range_data = {}
            for i, hip_range in enumerate(hip_ranges):
                start, end = map(int, hip_range.split('-'))
                for hip_number in range(start, end + 1):
                    hip_range_data[hip_number] = {'sale_date': sale_date_objects[i], 'day': i + 1}
            
            # Initialize DAY column to 0
            df['DAY'] = 0
            
            # Update SALEDATE and DAY columns
            for hip_number, data in hip_range_data.items():
                df.loc[df['Hip'] == hip_number, 'SALEDATE'] = data['sale_date']
                df.loc[df['Hip'] == hip_number, 'DAY'] = data['day']

        # Get sale dates from user input
        sale_dates_input = request.form['sale_dates']
        hip_ranges_input = request.form['hip_ranges']

        # Update sale dates and corresponding day column
        update_sale_dates(df, sale_dates_input, hip_ranges_input)

        # Dropping a column hip_number
        df.drop(columns=['Hip'], inplace=True)

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['HORSE'] = df['Name'].fillna("")
        else:
            df['HORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['CHORSE'] = df['Name'].fillna("")
        else:
            df['CHORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
                    # Dropping a column NAME
                    df.drop(columns=['Name'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        if 'Foaling Date' in df.columns:
            df['DATEFOAL'] = pd.to_datetime(df['Foaling Date'])
            df.drop(columns=['Foaling Date'], inplace=True)
        elif 'Foal Date' in df.columns:
            df['DATEFOAL'] = pd.to_datetime(df['Foal Date'])
            df.drop(columns=['Foal Date'], inplace=True)
        else:
            # Assign a default date if no date column is found
            default_date = pd.to_datetime('1900-01-01')  # Your chosen default date
            df['DATEFOAL'] = default_date

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = datefoal_series.dt.year.fillna(1901)

        def calculate_age(yearfoal, saleyear):
            # Calculate age as the difference between sale year and foaling year, plus 1
            age = saleyear - yearfoal
            return age

        # Calling the calculate_age() function
        age = calculate_age(df['YEARFOAL'], df['SALEYEAR'])

        # Adding a new column AGE
        df['AGE'] = age.fillna(0)

        # Adding a new column COLOR
        if 'Color' in df.columns:
            df['COLOR'] = df['Color'].fillna("")
        else:
            df['COLOR'] = ""

        # Dropping a column COLOR1
        if 'Color' in df.columns:
            df.drop(columns=['Color'], inplace=True)

        # Adding a new column SEX
        if 'Sex' in df.columns:
            df['SEX'] = df['Sex'].fillna("")
        else:
            df['SEX'] = ""

        # Dropping a column SEX1
        if 'Sex' in df.columns:
            df.drop(columns=['Sex'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        # Adding a new column TYPE
        df['TYPE'] = df['horsetype'].fillna("R")

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = df['State'].fillna("")

        df.drop(columns=['State'], inplace=True)

        # Adding a new column SIRE
        if 'Sire' in df.columns:
            df['SIRE'] =  df['Sire'].fillna("")
        else:
            df['SIRE'] = ""

        # Adding a new column CSIRE
        if 'Sire' in df.columns:
            df['CSIRE'] = df['Sire'].fillna("")
        else:
            df['CSIRE'] = "" 

        # Adding a new column DAM
        if 'Dam' in df.columns:
            df['DAM'] = df['Dam'].fillna("")
        else:
            df['DAM'] = ""

        # Adding a new column CDAM
        if 'Dam' in df.columns:
            df['CDAM'] = df['Dam'].fillna("")
        else:
            df['CDAM'] = ""

        # Adding a new column SIREOFDAM
        if 'Damsire' in df.columns:
            df['SIREOFDAM'] = df['Damsire'].fillna("")
        else:
            df['SIREOFDAM'] = ""

        # Adding a new column CSIREOFDAM
        if 'Damsire' in df.columns:
            df['CSIREOFDAM'] = df['Damsire'].fillna("")
        else:
            df['CSIREOFDAM'] = ""

        if 'Sort by Dam' in df.columns:
            df.drop(columns=['Sort by Dam'], inplace=True)
        
        if 'Out date' in df.columns:
            df.drop(columns=['Out date'], inplace=True)

        if 'Out Date' in df.columns:
            df.drop(columns=['Out Date'], inplace=True)

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        bredto = df['bredto']
        df['BREDTO'] = bredto.fillna("")

        # Dropping a column PROPERTY LINE
        df.drop(columns=['bredto'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = df['lastbred'] 
        df['LASTBRED'] = lastbred.fillna(pd.to_datetime('1901-01-01'))

        # Check if 'Alpha Sort' or 'Alphabetic Consignor Sort' exists
        if 'Alpha Sort' in df.columns:
            conlname = df['Alpha Sort']
        elif 'Alphabetic Consignor Sort' in df.columns:
            conlname = df['Alphabetic Consignor Sort']
        else:
            conlname = ""  # If neither column is found, create an empty series

        # Adding a new column CONSLNAME
        df['CONSLNAME'] = conlname

        # Dropping the column(s) if they exist
        df.drop(columns=['Alpha Sort', 'Alphabetic Consignor Sort'], inplace=True, errors='ignore')

        # Adding a new column CONSNO
        consno = df['Consignor']
        df['CONSNO'] = consno.fillna("")

        df.drop(columns=['Consignor'], inplace=True)

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        if 'Barn' in df.columns:
            df.drop(columns=['Barn'], inplace=True)

        if 'Set' in df.columns:
            df.drop(columns=['Set'], inplace=True)

        if 'Day' in df.columns:
            df.drop(columns=['Day'], inplace=True)

        if 'Out' in df.columns:
            df.drop(columns=['Out'], inplace=True)

        # Adding a new column PURLNAME
        purlname = df['Buyer']
        df['PURLNAME'] = purlname

        # Dropping a column PURCHASER
        df.drop(columns=['Buyer'], inplace=True)

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Adding a new column PRICE
        price_mapping = {
            'Not Sold': 0,
            'Out': 0
        }

        if 'Price' in df.columns:
            df['PRICE'] = df['Price'].replace(price_mapping).fillna(0)
        else:
            df['PRICE'] = 0

        if 'Price' in df.columns:
            # Adding a new column PRICE1
            df.drop(columns=['Price'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = ''
        df['URL'] = url

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Check if 'PS' or 'Post Sale' exists
        if 'PS' in df.columns:
            privatesale = df['PS']
        elif 'Post Sale' in df.columns:
            privatesale = df['Post Sale']
        else:
            privatesale = ""  # If neither column is found, create an empty string

        # Adding a new column PRIVATESALE
        df['PRIVATESALE'] = privatesale.fillna("")

        # Dropping the column(s) if they exist
        df.drop(columns=['PS', 'Post Sale'], inplace=True, errors='ignore')
      
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Check if 'UT Time' exists in the DataFrame
        if 'UT Time' in df.columns:
            # If 'UT Time' exists, apply the mapping and fill missing values with 0.0
            utt_mapping = {'out': 0.0}
            df['UTT'] = df['UT Time'].replace(utt_mapping).fillna(0.0)
        else:
            # If 'UT Time' doesn't exist, directly assign 0.0 to 'UTT'
            df['UTT'] = 0.0

        if 'UT Time' in df.columns:
            df.drop(columns=['UT Time'], inplace=True)

        df['STATUS'] = ""

        if 'Sire' in df.columns:
            df['tSire'] =  df['Sire'].fillna("")
        else:
            df['tSire'] = ""

        if 'Dam' in df.columns:
            df['TDAM'] = df['Dam'].fillna("")
        else:
            df['TDAM'] = ""

        if 'Damsire' in df.columns:
            df['tSireofdam'] = df['Damsire'].fillna("")
        else:
            df['tSireofdam'] = ""

        # Dropping a column SIRE1
        if 'Sire' in df.columns:
            df.drop(columns=['Sire'], inplace=True)

        # Dropping a column DAM1
        if 'Dam' in df.columns:
            df.drop(columns=['Dam'], inplace=True)

        # Dropping a column DAM SIRE
        if 'Damsire' in df.columns:
            df.drop(columns=['Damsire'], inplace=True)

        if 'Status' in df.columns:
            df.drop(columns=['Status'], inplace=True)

        df.drop(columns=['lastbred'], inplace=True)
        df.drop(columns=['horsetype'], inplace=True)

        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_excel(formatted_file_path, index=False, engine='openpyxl')  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)

        return render_template("obs-old.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("obs-old.html", message=f'Error: {str(e)}', data=None)

@app.route('/tattersalls', methods=['POST'])

def tattersalls():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('tattersalls.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('tattersalls.html', message='No selected file')
        
        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path
        
        # Read the selected Excel file into a DataFrame
        df = pd.read_excel(file_path)

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        # Adding a new column SALEYEAR
        # Convert 'SALEYEAR' to numeric
        df['SALEYEAR'] = pd.to_numeric(request.form['saleyear'], errors='coerce') 

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        df['SALEDATE'] = request.form['sale_dates']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Adding a new column DAY
        df['DAY'] = df['Day']

         # Function to update sale dates based on user input
        def update_sale_dates(df, sale_dates_input):
            sale_dates = [date.strip() for date in sale_dates_input.split(',')]
            # Convert the sale dates to datetime objects
            sale_date_objects = [datetime.strptime(date, '%Y-%m-%d') for date in sale_dates]
            for i, sale_date_obj in enumerate(sale_date_objects):
                day_increment = i + 1  # Increment the day by the index (starting from 1)
                for j, day in enumerate(df['DAY']):
                    if not pd.isnull(day):
                        if day == day_increment:
                            df.at[j, 'SALEDATE'] = sale_date_obj.strftime('%Y-%m-%d')

        # Get sale dates from user input
        sale_dates_input = request.form['sale_dates']

        # Update sale dates
        update_sale_dates(df, sale_dates_input)

        # Dropping a column SESSION
        if 'Day' in df.columns:
            df.drop(columns=['Day'], inplace=True)

        # Adding a new column HIP
        if 'Lot' in df.columns:
            df['HIP'] = df['Lot']

        # Adding a new column HIPNUM
        if 'Lot' in df.columns:
            df['HIPNUM'] = df['Lot']

        # Dropping a column HIP1
        if 'Lot' in df.columns:
            df.drop(columns=['Lot'], inplace=True)

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['HORSE'] = df['Name'].fillna("")
        else:
            df['HORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['CHORSE'] = df['Name'].fillna("")
        else:
            df['CHORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'Name' in df.columns:
                    # Dropping a column NAME
                    df.drop(columns=['Name'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        # Adding a new column DATEFOAL
        if 'Date Foaled' in df.columns:
            df['DATEFOAL'] = pd.to_datetime(df['Date Foaled'], errors='coerce')

        # Convert DATEFOAL to just the year as an integer
        datefoal = df['DATEFOAL'].dt.year

                # Function to calculate the age from yearfoal and datefoal
        def calculate_age(datefoal, saleyear):
            born_year = pd.to_numeric(datefoal, errors='coerce')  # Convert to numeric, handle invalid values
            sale_year = saleyear  # Convert to numeric, handle invalid values
            age = sale_year - born_year
            return age

        # Calling the calculate_age() function with yearfoal and datefoal
        age = calculate_age(datefoal, df['SALEYEAR'])
        
        print(df['SALEYEAR'].dtype)  # This should be an integer
        print(df['DATEFOAL'].dtype)  # This should be datetime64[ns]

        # Adding a new column AGE
        df['AGE'] = age
        
        if 'Date Foaled' in df.columns:
            df.drop(columns=['Date Foaled'], inplace=True)

        # Adding a new column COLOR
        if 'Colour' in df.columns:
            df['COLOR'] = df['Colour'].fillna("")

        # Dropping a column COLOR1
        if 'Colour' in df.columns:
            df.drop(columns=['Colour'], inplace=True)

        # Adding a new column SEX
        if 'Sex' in df.columns:
            df['SEX'] = df['Sex'].fillna("")

        # Dropping a column SEX1
        if 'Sex' in df.columns:
            df.drop(columns=['Sex'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait
        
        # Assign the result to the 'TYPE' column
        df['TYPE'] = ''

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = ''

        # Adding a new column SIRE
        if 'Sire' in df.columns:
            df['SIRE'] =  df['Sire'].fillna("")

        # Adding a new column CSIRE
        if 'Sire' in df.columns:
            df['CSIRE'] = df['Sire'].fillna("")

        # Adding a new column DAM
        if 'Dam' in df.columns:
            df['DAM'] = df['Dam'].fillna("")

        # Adding a new column CDAM
        if 'Dam' in df.columns:
            df['CDAM'] = df['Dam'].fillna("")

        df.drop(columns=['Grandsire'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'Damsire' in df.columns:
            df['SIREOFDAM'] = df['Damsire'].fillna("")

        # Adding a new column CSIREOFDAM
        if 'Damsire' in df.columns:
            df['CSIREOFDAM'] = df['Damsire'].fillna("")

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'Covered by' in df.columns:
            df['BREDTO'] = df['Covered by'].fillna("")

        # Dropping a column CONSIGNOR NAME
        if 'Covered by' in df.columns:
            df.drop(columns=['Covered by'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = '1901-01-01'
        df['LASTBRED'] = pd.to_datetime(lastbred)

        # Adding a new column CONLNAME
        conlname = df["Consignor"]
        df['CONSLNAME'] = conlname.fillna("")

        # Dropping a column PROPERTY LINE
        df.drop(columns=['Consignor'], inplace=True)

        # Adding a new column CONSNO
        consno = ''
        df['CONSNO'] = consno

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        # Adding a new column PURLNAME
        purlname = df['Purchaser']
        df['PURLNAME'] = purlname.fillna("")

        # Dropping a column PURCHASER
        df.drop(columns=['Purchaser'], inplace=True)

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Adding a new column PRICE
        price = df['Price (gns)'].fillna(0.0)
        df['PRICE'] = price

        # Adding a new column PRICE1
        df.drop(columns=['Price (gns)'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = ''
        df['URL'] = url

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = ""
        df['PRIVATESALE'] = privatesale
            
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = (df['SALEYEAR'] - 1).fillna(0000)

        df['UTT'] = 0.0
        df['STATUS'] = ""

        df['TDAM'] = df['Dam'].fillna("")

        df['tSire'] = df['Sire'].fillna("")

        df['tSireofdam'] = df['Damsire'].fillna("")

        # Dropping a column SIRE1
        if 'Sire' in df.columns:
            df.drop(columns=['Sire'], inplace=True)

        # Dropping a column DAM1
        if 'Dam' in df.columns:
            df.drop(columns=['Dam'], inplace=True)

        # Dropping a column SIRE OF DAM
        if 'Damsire' in df.columns:
            df.drop(columns=['Damsire'], inplace=True)

        df.drop(columns=['Year Foaled'], inplace=True)
        df.drop(columns=['Sale'], inplace=True)
        df.drop(columns=['Stabling'], inplace=True)
        df.drop(columns=['Year'], inplace=True)

        # Save the formatted file back to the server
        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_excel(formatted_file_path, index=False)  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)

        return render_template("tattersalls.html", message='File uploaded to database successfully', data=df.to_html())
    
    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("tattersalls.html", message=f'Error: {str(e)}', data=None)

@app.route('/arquana', methods=['POST'])

def arquana():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('arquana.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('arquana.html', message='No selected file')
        
        file_path = handle_file_upload(request)  # This handles the file upload and returns the file path
        
        # Read the selected Excel file into a DataFrame
        df = pd.read_excel(file_path)

        # Prompt the user to insert the salecode using a dialog
        salecode = request.form['salecode']

        # Check if the user provided a salecode or canceled the dialog
        if salecode is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            salecode

        else:
            print("Salecode input canceled.")

        # Adding a new column SALEYEAR
        saleyear = int(request.form['saleyear'])
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        df['SALEDATE'] = request.form['sale_dates']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Adding a new column DAY
        df['DAY'] = 1

        # Dropping a column SESSION
        if 'Day' in df.columns:
            df.drop(columns=['Day'], inplace=True)

        # Adding a new column HIP
        if 'Lot' in df.columns:
            df['HIP'] = df['Lot'].fillna('')

        # Adding a new column HIPNUM
        if 'Lot' in df.columns:
            df['HIPNUM'] = df['Lot'].fillna('')

        # Dropping a column HIP1
        if 'Lot' in df.columns:
            df.drop(columns=['Lot'], inplace=True)

        # Check if 'Nom' is a column in the DataFrame
        if 'Nom' in df.columns:
            # Filter out rows where 'Nom' starts with 'N('
            df_filtered = df[~df['Nom'].str.startswith('N(', na=False)]
            
            # Create a new 'HORSE' and 'CHORSE' column, initially set as empty string
            df['HORSE'] = ''
            df['CHORSE'] = ''
            
            # Populate the 'HORSE' and 'CHORSE' columns with 'Nom' where condition is met
            df.loc[df_filtered.index, 'HORSE'] = df_filtered['Nom']
            df.loc[df_filtered.index, 'CHORSE'] = df_filtered['Nom']
            
            # Fill NaN values with empty string ('') for any remaining missing values
            df['HORSE'] = df['HORSE'].fillna('')
            df['CHORSE'] = df['CHORSE'].fillna('')
        else:
            df['HORSE'] = ''
            df['CHORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'Nom' in df.columns:
                    # Dropping a column NAME
                    df.drop(columns=['Nom'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        # Adding a new column DATEFOAL
        datefoal = df['Date de naissance']
        df['DATEFOAL'] = pd.to_datetime(datefoal, errors='coerce').fillna(pd.to_datetime('1900-01-01'))

        # Function to calculate the age from DATEFOAL
        def calculate_age(saleyear, datefoal):
            # Convert to datetime (in case it wasn't done earlier)
            born = pd.to_datetime(datefoal, errors='coerce')  # Coerce errors to NaT (Not a Time) for invalid dates
            age = saleyear - born.dt.year  # Extract year using .dt accessor
            return age

        # Calling the calculate_age() function
        age = calculate_age(saleyear, df['DATEFOAL'])

        # Adding a new column AGE
        df['AGE'] = age.fillna(0)
        df.drop(columns=['Date de naissance'], inplace=True)

        # Adding a new column COLOR
        if 'Colour' in df.columns:
            df['COLOR'] = df['Colour']
        else:
            df['COLOR'] = ""

        # Dropping a column COLOR1
        if 'Colour' in df.columns:
            df.drop(columns=['Colour'], inplace=True)

        # Adding a new column SEX
        if 'Sexe' in df.columns:
            # Fill NA values with empty string and remove any trailing period (.)
            df['SEX'] = df['Sexe'].fillna('').str.replace(r'\.$', '', regex=True)

            # Map "M" to "C" in the SEX column
            df['SEX'] = df['SEX'].replace('M', 'C')

        # Dropping a column SEX1
        if 'Sexe' in df.columns:
            df.drop(columns=['Sexe'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        # Adding a new column TYPE
        condition_covered_by = df['Pleine de'].notna()
        # condition_foal = df['Produit'] == 'foal'
        condition_weanling = df['DATEFOAL'].dt.year == saleyear
        condition_datefoal = df['DATEFOAL'].dt.year == (saleyear - 1)
        # Define choices
        choices = np.select(
            [condition_covered_by, condition_weanling, condition_datefoal],
            ['B', 'W', 'Y'],
            default=''
        )

        # Assign the result to the 'TYPE' column
        df['TYPE'] = choices

        # Adding a new column RECORD
        record = ''
        df['RECORD'] = record

        # Adding a new column ET
        et = ''
        df['ET'] = et

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = df['Suffixe'].fillna('')

        # Adding a new column SIRE
        if 'Père' in df.columns:
            df['SIRE'] =  df['Père'].fillna('')

        # Adding a new column CSIRE
        if 'Père' in df.columns:
            df['CSIRE'] = df['Père'].fillna('')


        # Adding a new column DAM
        if 'Mère' in df.columns:
            df['DAM'] = df['Mère'].fillna('')

        # Adding a new column CDAM
        if 'Mère' in df.columns:
            df['CDAM'] = df['Mère'].fillna('')

        df.drop(columns=['Produit'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'Père de Mère' in df.columns:
            df['SIREOFDAM'] = df['Père de Mère'].fillna('')

        # Adding a new column CSIREOFDAM
        if 'Père de Mère' in df.columns:
            df['CSIREOFDAM'] = df['Père de Mère'].fillna('')

        df.drop(columns=['Cour / Box'], inplace=True)
        df.drop(columns=['Suffixe'], inplace=True)
        df.drop(columns=['Suffixe Père'], inplace=True)
        df.drop(columns=['Suffixe Mère'], inplace=True)

        # Adding a new column DAMOFDAM
        damofdam = ''
        df['DAMOFDAM'] = damofdam

        # Adding a new column CDAMOFDAM
        cdamofdam = ''
        df['CDAMOFDAM'] = cdamofdam

        # Adding a new column DAMTATT
        damtatt = ''
        df['DAMTATT'] = damtatt

        # Adding a new column DAMYOF
        damyof = 0
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'Pleine de' in df.columns:
            df['BREDTO'] = df['Pleine de'].fillna("")
        else:
            df['BREDTO'] = ''

        # Dropping a column CONSIGNOR NAME
        if 'Pleine de' in df.columns:
            df.drop(columns=['Pleine de'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = '1901-01-01'
        df['LASTBRED'] = pd.to_datetime(lastbred)

        # Adding a new column CONLNAME
        conlname = df["Vendeur"]
        df['CONSLNAME'] = conlname

        # Dropping a column PROPERTY LINE
        df.drop(columns=['Vendeur'], inplace=True)

        # Adding a new column CONSNO
        consno = ''
        df['CONSNO'] = consno

        # Adding a new column PEMCODE
        pemcode = ''
        df['PEMCODE'] = pemcode

        # Adding a new column PURFNAME
        purfname = ''
        df['PURFNAME'] = purfname

        # Adding a new column PURLNAME
        purlname = df['Acheteur'].fillna('')  # Fill NaN values with empty string

        # Check if "Racheté" is in the 'issue' column and set 'PURLNAME' to "RNA" where found
        df['PURLNAME'] = purlname  # Start by copying 'Acheteur' to 'PURLNAME'
        df['PURLNAME'] = df.apply(
            lambda row: 'RNA' if 'Racheté' in str(row['Issue']) else (row['PURLNAME'] if row['PURLNAME'] else ''), 
            axis=1
        )

        # Dropping a column PURCHASER
        df.drop(columns=['Acheteur'], inplace=True)

        # Dropping a column ISSUE
        df.drop(columns=['Issue'], inplace=True)

        # Adding a new column SBCITY
        sbcity = ''
        df['SBCITY'] = sbcity

        # Adding a new column SBSTATE
        sbstate = ''
        df['SBSTATE'] = sbstate

        # Adding a new column SBCOUNTRY
        sbcountry = ''
        df['SBCOUNTRY'] = sbcountry

        # Adding a new column PRICE
        price = df['Enchères']
        df['PRICE'] = price.fillna(0.0)

        # Adding a new column PRICE1
        df.drop(columns=['Enchères'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = ''
        df['URL'] = url

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = ''
        df['PRIVATESALE'] = privatesale
            
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['DATEFOAL'].dt.year.fillna(0000)

        df['UTT'] = 0.0
        df['STATUS'] = ""

        df['TDAM'] = df['Mère'].fillna("")

        df['tSire'] = df['Père'].fillna("")

        df['tSireofdam'] = df['Père de Mère'].fillna("")

        # Dropping a column SIRE1
        if 'Père' in df.columns:
            df.drop(columns=['Père'], inplace=True)

        # Dropping a column DAM1
        if 'Mère' in df.columns:
            df.drop(columns=['Mère'], inplace=True)

        # Dropping a column SIRE OF DAM
        if 'Père de Mère' in df.columns:
            df.drop(columns=['Père de Mère'], inplace=True)

        # Save the formatted file back to the server
        formatted_file_path = os.path.join(UPLOAD_FOLDER, f"formatted_{os.path.basename(file_path)}")
        df.to_excel(formatted_file_path, index=False)  # Save the formatted DataFrame to CSV

        upload_data_to_mysql(df)

        return render_template("arquana.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("arquana.html", message=f'Error: {str(e)}', data=None)

if __name__ == '__main__':
    app.run(debug=False)
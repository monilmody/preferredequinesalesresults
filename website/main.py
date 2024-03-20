from flask import Flask
from flask import render_template, request
import numpy as np
from views import views
import pandas as pd
from datetime import datetime
from datetime import date
from sqlalchemy import Column, String, Date, Float, ForeignKey, Integer, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import time
from mysql.connector import Error as MySQLError
import sys
print(sys.executable)

app = Flask(__name__)
app.register_blueprint(views, url_prefix="/views")

# # MySQL database connection
# db_user = 'your_username'
# db_password = ''
# db_host = 'localhost'
# db_port = 3306
# db_name = 'horse'

# engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")

Base = declarative_base()
csv_data = pd.DataFrame({})

def upload_data_to_mysql(df):
    global csv_data
    db_host = "localhost"
    db_name = "horse"
    db_user = "admin"
    db_pass = "1234"
    
    try:
        # Create a MySQL engine
        engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}")

        # Create a session factory
        Session = sessionmaker(bind=engine)

        # Create a new session
        session = Session()

        # Upload data to MySQL database
        table_name = 'tsales'
        table_name1 = 'tdamsire'

        # Define the table schema for tsales
        class Tsales(Base):
            __tablename__ = 'tsales'
            __table_args__ = {'extend_existing': True}
            SALE_ID = Column(Integer, primary_key=True, autoincrement=True)
            SALEYEAR = Column(Integer)
            SALETYPE = Column(String(1))
            SALECODE = Column(String(20))
            SALEDATE = Column(Date)
            BOOK = Column(String(2))
            DAY = Column(Integer)
            HIP = Column(String(6))
            HIPNUM = Column(Integer)
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
            ELIG = Column(String(2))
            BREDTO = Column(String(20))
            LASTBRED = Column(Date)
            CONSLNAME = Column(String(60))
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
            DAMSIRE_ID = Column(Integer, ForeignKey('tdamsire.DAMSIRE_ID'))
            tdamsire = relationship("Tdamsire", back_populates="tsales")

        # Define the table schema for tdamsire
        class Tdamsire(Base):
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
            tsales = relationship("Tsales", back_populates="tdamsire")


         # Define tables
        Base.metadata.create_all(engine)

        # Define the columns you want to insert into each table
        columns_for_tsales = ["SALEYEAR", "SALETYPE", "SALECODE", "SALEDATE", "BOOK", "DAY", "HIP", "HIPNUM", "HORSE", "CHORSE", "RATING", "TATTOO", "DATEFOAL", "AGE", "COLOR", "SEX", "GAIT", "TYPE", "RECORD", "ET", "ELIG", "BREDTO", "LASTBRED", "CONSLNAME", "CONSNO", "PEMCODE", "PURFNAME", "PURLNAME", "SBCITY", "SBSTATE", "SBCOUNTRY", "PRICE", "CURRENCY", "URL", "NFFM", "PRIVATESALE", "BREED", "YEARFOAL"]
        columns_for_tdamsire = ["SIRE", "CSIRE", "DAM", "CDAM", "SIREOFDAM", "CSIREOFDAM", "DAMOFDAM", "CDAMOFDAM", "DAMTATT", "DAMYOF", "DDAMTATT"]

        # table_schema = {
        #     "SALEYEAR": Integer,
        #     "SALETYPE": String(1),
        #     "SALECODE": String(20),
        #     "SALEDATE": Date,
        #     "BOOK": String(2),
        #     "DAY": Integer,
        #     "HIP": String(6),
        #     "HIPNUM": Integer,
        #     "HORSE": String(35),
        #     "CHORSE": String(35),
        #     "RATING": String(5),
        #     "TATTOO": String(6),
        #     "DATEFOAL": Date,
        #     "AGE": Integer,
        #     "COLOR": String(5),
        #     "SEX": String(3),
        #     "GAIT": String(3),
        #     "TYPE": String(3),
        #     "RECORD": String(25),
        #     "ET": String(1),
        #     "ELIG": String(2),
        #     "BREDTO": String(20),
        #     "LASTBRED": Date,
        #     "CONSLNAME": String(60),
        #     "CONSNO": String(20),
        #     "PEMCODE": String(15),
        #     "PURFNAME": String(30),
        #     "PURLNAME": String(70),
        #     "SBCITY": String(25),
        #     "SBSTATE": String(10),
        #     "SBCOUNTRY": String(15),
        #     "PRICE": Float,
        #     "CURRENCY": String(3),
        #     "URL": String(150),
        #     "NFFM": String(2),
        #     "PRIVATESALE": String(2),
        #     "BREED": String(2),
        #     "YEARFOAL": Integer
        # }

        # table_schema1 = {
        #     "SIRE": String(50),
        #     "CSIRE": String(50),
        #     "DAM": String(50),
        #     "CDAM": String(50),
        #     "SIREOFDAM": String(50),
        #     "CSIREOFDAM": String(50),
        #     "DAMOFDAM": String(50),
        #     "CDAMOFDAM": String(50),
        #     "DAMTATT": String(6),
        #     "DAMYOF": Integer,
        #     "DDAMTATT": String(6)
        # }

        # Retry logic for connection issues
                # chunk_size = 1000  # Adjust the chunk size based on your data size
                # for i in range(0, len(df), chunk_size):
                #     df_chunk = df[i:i + chunk_size]
                #     # df_chunk.to_sql(table_name1, con=engine, if_exists='append', index=False, dtype=table_schema1)
                #     # df_chunk.to_sql(table_name, con=engine, if_exists='append', index=False, dtype=table_schema)

                #     # Extract only the desired columns for tsales
                #     df_chunk_tsales = df_chunk[columns_for_tsales]

                #     # Exclude auto-incremented column DAMSIRE_ID
                #     columns_to_ignore = ["DAMSIRE_ID", "SALE_ID"]
                #     columns_to_insert = [col for col in df_chunk_tsales.columns if col not in columns_to_ignore]
                #     df_chunk_tsales[columns_to_insert].to_sql(table_name, con=engine, if_exists='append', index=False, dtype=table_schema)

                #     # Extract only the desired columns for tdamsire
                #     df_chunk_tdamsire = df_chunk[columns_for_tdamsire]

                #     # Exclude auto-incremented column DAMSIRE_ID
                #     columns_to_ignore = ["DAMSIRE_ID"]
                #     columns_to_insert1 = [col for col in df_chunk_tdamsire.columns if col not in columns_to_ignore]
                #     df_chunk_tdamsire[columns_to_insert1].to_sql(table_name1, con=engine, if_exists='append', index=False, dtype=table_schema1)
        for _, row in df.iterrows():
                    try:
                        # Insert into tdamsire first
                        tdamsire_data = {col: row[col] for col in columns_for_tdamsire}
                        tdamsire = Tdamsire(**tdamsire_data)
                        session.add(tdamsire)

                        # Use the generated DAMSIRE_ID in tsales
                        tsales_data = {col: row[col] for col in columns_for_tsales}
                        #tsales_data['DAMSIRE_ID'] = tdamsire.DAMSIRE_ID
                        tsales = Tsales(**tsales_data)
                        tsales.tdamsire = tdamsire
                        session.add(tsales)

                    except (MySQLError, Exception) as e:
                        session.rollback()
                        print(f"Error: {str(e)}")
                        retries += 1
                        time.sleep(2)  # Wait for a few seconds before retrying
                        continue

        # Commit the session after the loop
        session.commit()
        print(f'Data uploaded to tables {table_name} and {table_name1} in the database {db_name}')

        
        #return render_template("keenland.html", message=f'Data has been uploaded to the database successfully', data=df.to_html())

    except (RuntimeError, Exception) as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        session.rollback()
        return e
    finally:
        session.close()

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

@app.route('/tattersalls_redirect')
def tattersallsRedirect():
    return render_template('tattersalls.html')

@app.route('/arquana_redirect')
def arquanaRedirect():
    return render_template('arquana.html')

# @app.route('/upload', methods=['POST'])
# def upload():
    
#     global csv_data

#     # Check if the post request has the file part
#     if 'file' not in request.files:
#         return render_template('index.html', message='No file part')

#     file = request.files['file']
#     # If the user does not select a file, browser also
#     # submit an empty part without filename
#     if file.filename == '':
#         return render_template('index.html', message='No selected file')
    
#     # try:
#     #     # Read the CSV file
#     #     df1 = pd.read_csv(file)
        
#     #     # Update or modify the CSV data as needed
#     #     # For example, append the new data to the existing data
#     #     csv_data = pd.concat([csv_data, df1], ignore_index=True)

#     #     return render_template('index.html', message='File uploaded successfully', data=csv_data.to_html())
#     # except Exception as e:
#     #     return render_template('index.html', message=f'Error processing the file: {str(e)}')
#     return render_template('index.html', message='fill Successfully Uploaded')

@app.route('/keenland', methods=['POST'])

def keenland():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('keenland.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('keenland.html', message='No selected file')
        
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

        while True:
            # Get user input for SALE date
            saledate = request.form['saledate']

            # Check if the user provided a date or canceled the dialog
            if saledate is not None:
                try:
                    # Try to parse the input as a valid date
                    saledate = pd.to_datetime(saledate)
                    break  # Exit the loop if parsing is successful
                except ValueError:
                    print("Invalid date format. Please enter a valid date (YYYY-MM-DD).")
            else:
                print("saledate input canceled.")
                break  # Exit the loop if the user cancels the dialog

        saleyear = request.form['saleyear']

        # Check if the user provided a salecode or canceled the dialog
        if saleyear is not None:

            # Now you can work with the DataFrame 'df' and 'salecode' as needed

            # For example, you can print the first few rows and the salecode:
            #print(df.head())
            saleyear

        else:
            print("saleyear input canceled.")
        # Renaming HIP to HIP1
        # df.rename(columns={'Hip': 'HIP1'}, inplace=True)

        # # Renaming PRICE to PRICE1
        # df.rename(columns={'price': 'PRICE1'}, inplace=True)

        # Renaming PRIVATE SALE to PRIVATE SALE1
        # df.rename(columns={'PRIVATE SALE': 'PRIVATE SALE1'}, inplace=True)

        # # Calculating the year of birth from the datefoal
        # saledate_series = pd.to_datetime(df['SALE DATE'], errors='coerce')

        # df[df['SALE DATE'].isna()]

        # # Convert the date column to "YEAR-MONTH-DAY" format
        # df['SALE DATE'] = saledate_series.dt.strftime('%Y-%m-%d')

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        saletype = 'Y'
        df['SALETYPE'] = saletype

        # Adding a new column SALECODE
        df['SALECODE'] = salecode


        df['SALEDATE'] = pd.to_datetime(saledate)

        # # Initialize a counter
        # counter = 0

        # # Initialize a list to store the counter values
        # counter_values = []

        # # Iterate through the list of dates
        # for i, date_str in enumerate(df['SALE DATE']):
        #     # Convert the date string to a datetime object
        #     date = datetime.datetime.strptime(date_str, '%m/%d/%Y')
        #     formatted_date = date.strftime('%Y-%m-%d')
        #     # Check if this is the first date or if the date has changed from the previous one
        #     if i == 0 or date != prev_date:
        #         counter += 1  # Increment the counter when the date changes
        #         prev_date = date  # Update the previous date
                
        #     counter_values.append(counter)

        if 'Book' in df.columns:
            df['BOOK'] = df['Book']
        else:
            df['BOOK'] = 1
        
        # # Adding a new column DAY
        # if 'SALEDATE' in df.columns:
        df['DAY'] = df['Session']

        # # Initialize a counter
        # counter = 0

        # # Initialize a list to store the counter values
        # counter_values = []

        # # Iterate through the list of dates
        # for i, date_str in enumerate(df['SESSION']):
        #     # Convert the date string to a datetime object
        #     date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            
        #     # Check if this is the first date or if the date has changed from the previous one
        #     if i == 0 or date != prev_date:
        #         counter += 1  # Increment the counter when the date changes
        #         prev_date = date  # Update the previous date
                
        #     counter_values.append(counter)

        # # Adding a new column DAY
        # df['DAY'] = counter_values

        # Adding a new column HIP
        df['HIP'] = df['Hip']

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['Hip']

        # Dropping a column HIP1
        if 'HIP1' in df.columns:
            df.drop(columns=['HIP1'], inplace=True)

        # Check if 'NAME' is a column in the DataFrame
        # if 'Horse Name' in df.columns:
        #             # Create a new 'HORSE' column and populate it with 'NAME'
        #             df['HORSE'] = df['Horse Name']
        # else:
        #     df['HORSE'] = ''

        default_horse = "No Horse"
        df['HORSE'] = df['Horse Name'].fillna(default_horse)

        # # Check if 'NAME' is a column in the DataFrame
        # if 'Horse Name' in df.columns:
        #             # Create a new 'HORSE' column and populate it with 'NAME'
        #             df['CHORSE'] = df['Horse Name']
        # else:
        #     df['CHORSE'] = ''

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

        # Adding sate_mapping key: value pair to replace the value
        # state_mapping = {
        #     'KENTUCKY': 'KY',
        #     'NEW YORK': 'NY',
        #     'PENNSYLVANIA': 'PA',
        #     'FLORIDA': 'FL',
        #     'ONTARIO': 'ON',
        #     'VIRGINIA': 'VA',
        #     'LOUISIANA': 'LA',
        #     'MARYLAND': 'MD',
        #     'ARKANSAS': 'AR'
        # }

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        if 'Area_Foaled' in df.columns:
            df['ELIG'] = df['Area_Foaled']
        else:
            df['ELIG'] = ''

        if 'Area_Foaled' in df.columns:
            df.drop(columns=['Area_Foaled'], inplace=True)

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
            df['SIREOFDAM'] = df['Broodmare Sire']
        elif 'Sire Of Dam' in df.columns:
            df['SIREOFDAM'] = df['Sire Of Dam']
        else: 
            df['SIREOFDAM'] = ''

        # Adding a new column CSIREOFDAM
        if 'Broodmare Sire' in df.columns:
            df['CSIREOFDAM'] = df['Broodmare Sire']
        elif 'Sire Of Dam' in df.columns:
            df['CSIREOFDAM'] = df['Sire Of Dam']
        else: 
            df['CSIREOFDAM'] = ''

        # Dropping columns Broodmare Sire and Sire Of Dam
        if 'Broodmare Sire' in df.columns:
            df.drop(columns=['Broodmare Sire'], inplace=True)
        elif 'Sire Of Dam' in df.columns:
            df.drop(columns=['Sire Of Dam'], inplace=True)

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
            df['BREDTO'] = df['Covering Sire']
        elif 'ConveringSire' in df.columns:
            df['BREDTO'] = df['CoveringSire']
        else:
            df['BREDTO'] = ''
            
        # Adding a new column LASTBRED
        lastbred = '1901-01-01'
        df['LASTBRED'] = pd.to_datetime(lastbred)

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


        # Dropping a column PURCHASER
        if 'Purchaser' in df.columns:
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

        price_mapping = {
            '---': ''
        }
        # Adding a new column PRICE
        if 'Price' in df.columns:
            df['PRICE'] = df['Price'].replace(price_mapping)
        else:
            df['PRICE'] = 0.0

        if 'Price' in df.columns:
            df.drop(columns=['Price'], inplace=True)

        # Adding a new column PRICE1
        # df.drop(columns=['PRICE1'], inplace=True)

        # Adding a new column SALE TITLE
        # df.drop(columns=['SALE TITLE'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = '' 
        df['URL'] = url

        # Dropping a column VIRTUAL INSPECTION
        # df.drop(columns=['VIRTUAL INSPECTION'], inplace=True)

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = ''
        df['PRIVATESALE'] = privatesale

        # Dropping a column PRIVATE SALE1
        # df.drop(columns=['PRIVATE SALE1'], inplace=True)

        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        if 'DOB' in df.columns:
            df['YEARFOAL'] = df['DATEFOAL'].dt.year
        else:
            df['YEARFOAL'] = df.fillna(['0000'])

        # Dropping a column BARN
        # df.drop(columns=['BARN'], inplace=True)

        # Dropping a column COVER DATE
        # df.drop(columns=['COVER DATE'], inplace=True)

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

        # Dropping a column SOLD AS DESCRIPTION
        # df.drop(columns=['SOLD AS DESCRIPTION'], inplace=True)

        # Dropping a column FOALED
        # df.drop(columns=['FOALED'], inplace=True)

        # # Determining the output path for the modified file
        # output_file_path = 'C:\\Users\\monil\\Downloads\\{}.csv'.format(salecode)

        # # Checking whether the file is already in the file explorer
        # if os.path.exists(output_file_path):
        #     # If the file exists, remove it
        #     os.remove(output_file_path)

        # # Saving the file as a csv extension
        # df.to_csv(output_file_path, index=False)

        # # Opening the file once it is converted to csv file
        # os.system(f'start {output_file_path}')
            
        print("reached here 1")
          
        # db_host = "localhost"
        # port = 3306
        # db_name = "horses"
        # db_user = "root"
        # db_pass = ""
        
        #     # Create a MySQL engine
        # engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}:{port}/{db_name}")

        # # Upload data to MySQL database
        # table_name = 'tsales'
        # table_name1 = 'tdamsire'
        # table_schema = {
            # "SALEYEAR": INT,
            # "SALETYPE": VARCHAR(1),
            # "SALECODE": VARCHAR(20),
            # "SALEDATE": DATE,
            # "BOOK": VARCHAR(2),
            # "DAY": INT,
            # "HIP": VARCHAR(6),
            # "HIPNUM": INT,
            # "HORSE": VARCHAR(35),
            # "CHORSE": VARCHAR(35),
            # "RATING": VARCHAR(5),
            # "TATTOO": VARCHAR(6),
            # "DATEFOAL": DATE,
            # "AGE": INT,
            # "COLOR": VARCHAR(5),
            # "SEX": VARCHAR(3),
            # "GAIT": VARCHAR(3),
            # "TYPE": VARCHAR(3),
            # "RECORD": VARCHAR(25),
            # "ET": VARCHAR(1),
            # "ELIG": VARCHAR(2),
            # "BREDTO": VARCHAR(20),
            # "LASTBRED": DATE,
            # "CONSLNAME": VARCHAR(60),
            # "CONSNO": VARCHAR(20),
            # "PEMCODE": VARCHAR(15),
            # "PURFNAME": VARCHAR(30),
            # "PURLNAME": VARCHAR(70),
            # "SBCITY": VARCHAR(25),
            # "SBSTATE": VARCHAR(10),
            # "SBCOUNTRY": VARCHAR(15),
            # "PRICE": DOUBLE,
            # "CURRENCY": VARCHAR(3),
            # "URL": VARCHAR(150),
            # "NFFM": VARCHAR(2),
            # "PRIVATESALE": VARCHAR(2),
            # "BREED": VARCHAR(2),
            # "YEARFOAL": INT
        # }

        # table_schema1 = {
            # "SIRE": VARCHAR(50),
            # "CSIRE": VARCHAR(50),
            # "DAM": VARCHAR(50),
            # "CDAM": VARCHAR(50),
            # "SIREOFDAM": VARCHAR(50),
            # "CSIREOFDAM": VARCHAR(50),
            # "DAMOFDAM": VARCHAR(50),
            # "CDAMOFDAM": VARCHAR(50),
            # "DAMTATT": VARCHAR(6),
            # "DAMYOF": INT,
            # "DDAMTATT": VARCHAR(6)
        # }
        # df.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=table_schema)
        # df.to_sql(table_name1, con=engine, if_exists='replace', index=False, dtype=table_schema1)
        # print(f'Data uploaded to table {table_name} in the database {db_name}')

        # csv_data = pd.concat([csv_data, df], ignore_index=True)
        #return render_template("index.html", message='File uploaded successfully', data=df.to_html())
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
        saleyear = 2023
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        saletype = 'Y'
        df['SALETYPE'] = saletype

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        if 'SESSION' in df.columns:
            df['SALEDATE'] = df['SESSION']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Initialize a counter
        counter = 0

        # Initialize a list to store the counter values
        counter_values = []

        # Iterate through the list of dates
        for i, date_str in enumerate(df['SESSION']):
            # Convert the date string to a datetime object
            date = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Check if this is the first date or if the date has changed from the previous one
            if i == 0 or date != prev_date:
                counter += 1  # Increment the counter when the date changes
                prev_date = date  # Update the previous date
                
            counter_values.append(counter)

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

        # Check if 'NAME' is a column in the DataFrame
        if 'NAME' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['HORSE'] = df['NAME']
        else:
            df['HORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'NAME' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['CHORSE'] = df['NAME']
        else:
            df['CHORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'NAME' in df.columns:
                    # Dropping a column NAME
                    df.drop(columns=['NAME'], inplace=True)

        # Adding a new column RATING
        rating = ''
        df['RATING'] = rating

        # Adding a new column TATTOO
        tattoo = ''
        df['TATTOO'] = tattoo

        # Adding a new column DATEFOAL
        if 'YEAR OF BIRTH' in df.columns:
            df['DATEFOAL'] = pd.to_datetime(df['YEAR OF BIRTH'])

        # Dropping a column YEAR OF BIRTH
        if 'YEAR OF BIRTH' in df.columns:
            df.drop(columns=['YEAR OF BIRTH'], inplace=True)

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

        # Adding a new column COLOR
        if 'COLOR1' in df.columns:
            df['COLOR'] = df['COLOR1']

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
            df['TYPE'] = 'Y'

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
            'NEW MEXICO': 'NM'
        }

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        df['ELIG'] = df['FOALED'].replace(state_mapping)

        # Adding a new column SIRE
        if 'SIRE1' in df.columns:
            df['SIRE'] =  df['SIRE1']

        # Adding a new column CSIRE
        if 'SIRE1' in df.columns:
            df['CSIRE'] = df['SIRE1']

        # Dropping a column SIRE1
        if 'SIRE1' in df.columns:
            df.drop(columns=['SIRE1'], inplace=True)

        # Adding a new column DAM
        if 'DAM1' in df.columns:
            df['DAM'] = df['DAM1']

        # Adding a new column CDAM
        if 'DAM1' in df.columns:
            df['CDAM'] = df['DAM1']

        # Dropping a column DAM1
        if 'DAM1' in df.columns:
            df.drop(columns=['DAM1'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'SIRE OF DAM' in df.columns:
            df['SIREOFDAM'] = df['SIRE OF DAM']

        # Adding a new column CSIREOFDAM
        if 'SIRE OF DAM' in df.columns:
            df['CSIREOFDAM'] = df['SIRE OF DAM']

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
        damyof = ''
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'CONSIGNOR NAME' in df.columns:
            df['BREDTO'] = df['CONSIGNOR NAME']
        else:
            df['BREDTO'] = ''
        # Dropping a column CONSIGNOR NAME
        if 'CONSIGNOR NAME' in df.columns:
            df.drop(columns=['CONSIGNOR NAME'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = ''
        df['LASTBRED'] = lastbred

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
        df.drop(columns=['SALE TITLE'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

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

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['DATEFOAL'].dt.year

        # Dropping a column BARN
        df.drop(columns=['BARN'], inplace=True)

        # Dropping a column COVER DATE
        df.drop(columns=['COVER DATE'], inplace=True)

        # Dropping a column SOLD AS CODE
        if 'SOLD AS CODE' in df.columns:
            df.drop(columns=['SOLD AS CODE'], inplace=True)

        if 'COVERING SIRE' in df.columns:
            df.drop(columns=['COVERING SIRE'], inplace=True)

        # Dropping a column SOLD AS DESCRIPTION
        df.drop(columns=['SOLD AS DESCRIPTION'], inplace=True)

        # Dropping a column FOALED
        df.drop(columns=['FOALED'], inplace=True)

        # Dropping a column PRIVATE SALE
        df.drop(columns=['PRIVATE SALE'], inplace=True)

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
        df['SALEYEAR'] = request.form['saleyear']

        # Adding a new column SALETYPE
        saletype = 'M'
        df['SALETYPE'] = saletype

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        df['SALEDATE'] = request.form['saledate']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # # Initialize a counter
        # counter = 0

        # # Initialize a list to store the counter values
        # counter_values = []

        # # Iterate through the list of dates
        # for i, date_str in enumerate(df['SESSION']):
        #     # Convert the date string to a datetime object
        #     date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            
        #     # Check if this is the first date or if the date has changed from the previous one
        #     if i == 0 or date != prev_date:
        #         counter += 1  # Increment the counter when the date changes
        #         prev_date = date  # Update the previous date
                
        #     counter_values.append(counter)

        # Adding a new column DAY
        day = 1
        df['DAY'] = day

        # Dropping a column SESSION
        if 'SESSION' in df.columns:
            df.drop(columns=['SESSION'], inplace=True)

        # Adding a new column HIP
        df['HIP'] = df['Lot']

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['Lot']

        # Dropping a column HIP1
        if 'Lot' in df.columns:
            df.drop(columns=['Lot'], inplace=True)

        # Create a new 'HORSE' column and populate it with 'NAME'
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

        # Adding a new column DATEFOAL
        datefoal = ''
        df['DATEFOAL'] = datefoal

        # # Dropping a column YEAR OF BIRTH
        # if 'YEAR OF BIRTH' in df.columns:
        #     df.drop(columns=['YEAR OF BIRTH'], inplace=True)

        # # Function to calculate the age from DATEFOAL
        # def calculate_age(datefoal):
        #     today = date.today()
        #     born = pd.to_datetime(datefoal, errors='coerce')  # Convert to datetime, handle invalid dates
        #     age = today.year - born.dt.year - ((today.month * 100 + today.day) < (born.dt.month * 100 + born.dt.day))
        #     return age

        # # Calling the calculate_age() function
        # age = calculate_age(df['DATEFOAL'])

        # Adding a new column AGE
        age = ''
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

        # # Adding a new column TYPE
        # if(df['Year'] >= 2022):
        #     df['TYPE'] = 'Y'

        # if (df['Covering Sire'].empty):
        #     df['TYPE'] = ''
        # elif(df['Covering Sire'].notnull): 
        #     df['TYPE'] = 'B'

        # Adding a new column 'TYPE'
        df['TYPE'] = ''

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

        # Adding sate_mapping key: value pair to replace the value
        # state_mapping = {
        #     'KENTUCKY': 'KY',
        #     'NEW YORK': 'NY',
        #     'PENNSYLVANIA': 'PA',
        #     'FLORIDA': 'FL',
        #     'ONTARIO': 'ON',
        #     'VIRGINIA': 'VA',
        #     'LOUISIANA': 'LA',
        #     'MARYLAND': 'MD',
        #     'ARKANSAS': 'AR',
        #     'INDIANA': 'IN',
        #     'OHIO': 'OH',
        #     'CALIFORNIA': 'CA',
        #     'TEXAS': 'TX',
        #     'IOWA': 'IA',
        #     'NEW MEXICO': 'NM'
        # }

        # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
        elig = ''
        df['ELIG'] = elig

        # Adding a new column SIRE
        if 'Sire' in df.columns:
            df['SIRE'] =  df['Sire']

        # Adding a new column CSIRE
        if 'Sire' in df.columns:
            df['CSIRE'] = df['Sire']

        # Dropping a column SIRE1
        if 'Sire' in df.columns:
            df.drop(columns=['Sire'], inplace=True)

        # Adding a new column DAM
        if 'Dam' in df.columns:
            df['DAM'] = df['Dam']

        # Adding a new column CDAM
        if 'Dam' in df.columns:
            df['CDAM'] = df['Dam']

        # Dropping a column DAM1
        if 'Dam' in df.columns:
            df.drop(columns=['Dam'], inplace=True)

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
        damyof = ''
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'Covering Sire' in df.columns:
            df['BREDTO'] = df['Covering Sire'].fillna("")

        # Dropping a column CONSIGNOR NAME
        if 'Consignor' in df.columns:
            df.drop(columns=['Covering Sire'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = ''
        df['LASTBRED'] = lastbred

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
        df['PURLNAME'] = purlname

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
        df['PRICE'] = price.fillna("")

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

        # Dropping a column BARN
        df.drop(columns=['Year'], inplace=True)

        # Dropping a column SOLD AS CODE
        if 'Stabling' in df.columns:
            df.drop(columns=['Stabling'], inplace=True)

        if 'Status' in df.columns:
            df.drop(columns=['Status'], inplace=True)

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
        df['SALEYEAR'] = request.form['saleyear']

        # Adding a new column SALETYPE
        saletype = 'Y'
        df['SALETYPE'] = saletype

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        df['SALEDATE'] = request.form['saledate']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # # Initialize a counter
        # counter = 0

        # # Initialize a list to store the counter values
        # counter_values = []

        # # Iterate through the list of dates
        # for i, date_str in enumerate(request.form['saledate']):
        #     # Convert the date string to a datetime object
        #     date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

        #     # Check if this is the first date or if the date has changed from the previous one
        #     if i == 0 or date != prev_date:
        #         counter += 1  # Increment the counter when the date changes
        #         prev_date = date  # Update the previous date
                
        #     counter_values.append(counter)

        # Adding a new column DAY
        day = 1
        df['DAY'] = day

        # Adding a new column HIP
        df['HIP'] = df['Hip']

        # Adding a new column HIPNUM
        df['HIPNUM'] = df['Hip']

        # Dropping a column HIP1
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

        # Adding a new column DATEFOAL
        df['DATEFOAL'] = pd.to_datetime(df['Foaling Date'])

        # Dropping a column Foaling Date
        df.drop(columns=['Foaling Date'], inplace=True)

        # Function to calculate the age from DATEFOAL
        def calculate_age(datefoal):
            today = date.today()
            born = pd.to_datetime(datefoal, errors='coerce')  # Convert to datetime, handle invalid dates
            age = today.year - born.dt.year - ((today.month * 100 + today.day) < (born.dt.month * 100 + born.dt.day))
            return age

        # Calling the calculate_age() function
        age = calculate_age(df['DATEFOAL'])

        # Adding a new column AGE
        df['AGE'] = age.fillna("")

        # Adding a new column COLOR
        if 'Color' in df.columns:
            df['COLOR'] = df['Color'].fillna("")

        # Dropping a column COLOR1
        if 'Color' in df.columns:
            df.drop(columns=['Color'], inplace=True)

        # Adding a new column SEX
        if 'Sex' in df.columns:
            df['SEX'] = df['Sex'].fillna("")

        # Dropping a column SEX1
        if 'Sex' in df.columns:
            df.drop(columns=['Sex'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        # Adding a new column TYPE
        if 'SOLD AS CODE' in df.columns:
            df['TYPE'] = df['SOLD AS CODE']
        else: 
            df['TYPE'] = 'Y'

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

        # Adding a new column CSIRE
        if 'Sire' in df.columns:
            df['CSIRE'] = df['Sire'].fillna("")

        # Dropping a column SIRE1
        if 'Sire' in df.columns:
            df.drop(columns=['Sire'], inplace=True)

        # Adding a new column DAM
        if 'Dam' in df.columns:
            df['DAM'] = df['Dam'].fillna("")

        # Adding a new column CDAM
        if 'Dam' in df.columns:
            df['CDAM'] = df['Dam'].fillna("")

        # Dropping a column DAM1
        if 'Dam' in df.columns:
            df.drop(columns=['Dam'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'Damsire' in df.columns:
            df['SIREOFDAM'] = df['Damsire'].fillna("")

        # Adding a new column CSIREOFDAM
        if 'Damsire' in df.columns:
            df['CSIREOFDAM'] = df['Damsire'].fillna("")

        # Dropping a column SIRE OF DAM
        if 'Damsire' in df.columns:
            df.drop(columns=['Damsire'], inplace=True)

        df.drop(columns=['Sort by Dam'], inplace=True)
        df.drop(columns=['Status'], inplace=True)
        df.drop(columns=['Out date'], inplace=True)
        df.drop(columns=['Alpha Sort'], inplace=True)

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
        damyof = ''
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        bredto = ""
        df['BREDTO'] = bredto

        # Adding a new column LASTBRED
        lastbred = ''
        df['LASTBRED'] = lastbred

        # Adding a new column CONLNAME
        conlname = df['Consignor']
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

        df.drop(columns=['Barn'], inplace=True)

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
        price = df['Price']
        df['PRICE'] = price

        # Adding a new column PRICE1
        df.drop(columns=['Price'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        
        url = ""
        df['URL'] = url

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = df['PS']
        df['PRIVATESALE'] = privatesale.fillna("")

        df.drop(columns=['PS'], inplace=True)
      
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['DATEFOAL'].dt.year.fillna("")

        upload_data_to_mysql(df)

        return render_template("obs.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("obs.html", message=f'Error: {str(e)}', data=None)


@app.route('/tattersalls', methods=['POST'])

def tattersalls():
    
    global csv_data
    try:
        if 'file' not in request.files:
            return render_template('tattersalls.html', message='No file path')
        file_path = request.files['file']

        if file_path.filename == '':
            return render_template('tattersalls.html', message='No selected file')
        
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
        df['SALEYEAR'] = df['Year']

        # Adding a new column SALETYPE
        saletype = 'Y'
        df['SALETYPE'] = saletype

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        df['SALEDATE'] = request.form['saledate']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Adding a new column DAY
        df['DAY'] = df['Day']

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
        datefoal = 0000-00-00
        df['DATEFOAL'] = datefoal

        # Function to calculate the age from DATEFOAL
        def calculate_age(Year, year_foaled):
            age = Year - year_foaled
            return age

        # Calling the calculate_age() function
        age = calculate_age(df['Year'], df['Year Foaled'])

        # Adding a new column AGE
        df['AGE'] = age.fillna(0)
        df.drop(columns=['Year'], inplace=True)

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

        # # Adding a new column TYPE
        # if 'Covered by' in df.columns:
        #     df['TYPE'] = df.apply(lambda x: 'B' if pd.notna(x['Covered by']) else '', axis=1)
        # elif df['AGE'] <= 1: 
        #     df['TYPE'] = 'Y'
        # else:
        #     df['TYPE'] = "RH"

        # Define conditions
        condition_covered_by = df['Covered by'].notna()
        condition_age = (df['AGE'] <= 1)

        # Define choices
        choices = np.select(
            [condition_covered_by, condition_age],
            ['B', 'Y'],
            default='RH'
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
        df['ELIG'] = ''

        # Adding a new column SIRE
        if 'Sire' in df.columns:
            df['SIRE'] =  df['Sire'].fillna("")

        # Adding a new column CSIRE
        if 'Sire' in df.columns:
            df['CSIRE'] = df['Sire'].fillna("")

        # Dropping a column SIRE1
        if 'Sire' in df.columns:
            df.drop(columns=['Sire'], inplace=True)

        # Adding a new column DAM
        if 'Dam' in df.columns:
            df['DAM'] = df['Dam'].fillna("")

        # Adding a new column CDAM
        if 'Dam' in df.columns:
            df['CDAM'] = df['Dam'].fillna("")

        # Dropping a column DAM1
        if 'Dam' in df.columns:
            df.drop(columns=['Dam'], inplace=True)

        df.drop(columns=['Grandsire'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'Damsire' in df.columns:
            df['SIREOFDAM'] = df['Damsire'].fillna("")

        # Adding a new column CSIREOFDAM
        if 'Damsire' in df.columns:
            df['CSIREOFDAM'] = df['Damsire'].fillna("")

        # Dropping a column SIRE OF DAM
        if 'Damsire' in df.columns:
            df.drop(columns=['Damsire'], inplace=True)

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
        damyof = ''
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
        lastbred = ''
        df['LASTBRED'] = lastbred

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
        price = df['Price (gns)']
        df['PRICE'] = price.fillna("")

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

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL']

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['Year Foaled'].fillna("")
        df.drop(columns=['Year Foaled'], inplace=True)
        df.drop(columns=['Sale'], inplace=True)
        df.drop(columns=['Stabling'], inplace=True)

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
        saleyear = request.form['saleyear']
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        saletype = 'Y'
        df['SALETYPE'] = saletype

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        if 'SESSION' in df.columns:
            df['SALEDATE'] = df['SESSION']

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # # Initialize a counter
        # counter = 0

        # # Initialize a list to store the counter values
        # counter_values = []

        # # Iterate through the list of dates
        # for i, date_str in enumerate(df['SESSION']):
        #     # Convert the date string to a datetime object
        #     date = datetime.strptime(date_str, '%Y-%m-%d')
            
        #     # Check if this is the first date or if the date has changed from the previous one
        #     if i == 0 or date != prev_date:
        #         counter += 1  # Increment the counter when the date changes
        #         prev_date = date  # Update the previous date
                
        #     counter_values.append(counter)

        # Adding a new column DAY
        df['DAY'] = 1

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
        if 'Nom' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['HORSE'] = df['Nom']
        else:
            df['HORSE'] = ''

        # Check if 'NAME' is a column in the DataFrame
        if 'Nom' in df.columns:
                    # Create a new 'HORSE' column and populate it with 'NAME'
                    df['CHORSE'] = df['Nom']
        else:
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
        df['DATEFOAL'] = datefoal

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

        df.drop(columns=['Date de naissance'], inplace=True)

        # Adding a new column COLOR
        if 'Colour' in df.columns:
            df['COLOR'] = df['Colour']

        # Dropping a column COLOR1
        if 'Colour' in df.columns:
            df.drop(columns=['Colour'], inplace=True)

        # Adding a new column SEX
        if 'Sexe' in df.columns:
            df['SEX'] = df['Sexe']

        # Dropping a column SEX1
        if 'Sexe' in df.columns:
            df.drop(columns=['Sexe'], inplace=True)

        # Adding a new column GAIT
        gait = ''
        df['GAIT'] = gait

        # Adding a new column TYPE
        condition_covered_by = df['Covered by'].notna()
        condition_foal = df['Produit'] == 'foal'
        # Define choices
        choices = np.select(
            [condition_covered_by, condition_foal],
            ['B', 'W'],
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
        df['ELIG'] = ''

        # Adding a new column SIRE
        if 'Père' in df.columns:
            df['SIRE'] =  df['Père']

        # Adding a new column CSIRE
        if 'Père' in df.columns:
            df['CSIRE'] = df['Père']

        # Dropping a column SIRE1
        if 'Père' in df.columns:
            df.drop(columns=['Père'], inplace=True)

        # Adding a new column DAM
        if 'Mère' in df.columns:
            df['DAM'] = df['Mère']

        # Adding a new column CDAM
        if 'Mère' in df.columns:
            df['CDAM'] = df['Mère']

        # Dropping a column DAM1
        if 'Mère' in df.columns:
            df.drop(columns=['Mère'], inplace=True)

        df.drop(columns=['Produit'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'Père de Mère' in df.columns:
            df['SIREOFDAM'] = df['Père de Mère']

        # Adding a new column CSIREOFDAM
        if 'Père de Mère' in df.columns:
            df['CSIREOFDAM'] = df['Père de Mère']

        # Dropping a column SIRE OF DAM
        if 'Père de Mère' in df.columns:
            df.drop(columns=['Père de Mère'], inplace=True)

        df.drop(columns=['Issue'], inplace=True)
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
        damyof = ''
        df['DAMYOF'] = damyof

        # Adding a new column DDAMTATT
        ddamtatt = ''
        df['DDAMTATT'] = ddamtatt

        # Adding a new column BREDTO
        if 'Pleine de' in df.columns:
            df['BREDTO'] = df['Pleine de'].fillna("")

        # Dropping a column CONSIGNOR NAME
        if 'Pleine de' in df.columns:
            df.drop(columns=['Pleine de'], inplace=True)

        # Adding a new column LASTBRED
        lastbred = ''
        df['LASTBRED'] = lastbred

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
        purlname = df['Acheteur']
        df['PURLNAME'] = purlname

        # Dropping a column PURCHASER
        df.drop(columns=['Acheteur'], inplace=True)

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
        df['PRICE'] = price

        # Adding a new column PRICE1
        df.drop(columns=['Enchères'], inplace=True)

        # Adding a new column CURRENCY
        currency = ''
        df['CURRENCY'] = currency

        # Adding a new column URL
        url = ''
        df['URL'] = url.fillna('')

        # Adding a new column NFFM
        nffm = ''
        df['NFFM'] = nffm

        # Adding a new column PRIVATE SALE
        privatesale = ''
        df['PRIVATESALE'] = privatesale.fillna('')
            
        # Adding a new column BREED
        breed = 'T'
        df['BREED'] = breed

        # Calculating the year of birth from the datefoal
        datefoal_series = df['DATEFOAL'].dt.year

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['DATEFOAL'].dt.year

        upload_data_to_mysql(df)

        return render_template("arquana.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("arquana.html", message=f'Error: {str(e)}', data=None)

if __name__ == '__main__':
    app.run(debug=False)

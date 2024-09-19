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
import re
print(sys.executable)

app = Flask(__name__)
app.register_blueprint(views, url_prefix="/views")

# # MySQL database connection
# db_user = 'preferredequine'
# db_password = ''
# db_host = '172.31.44.125'
# db_port = 3306
# db_name = 'horse'

# engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")

Base = declarative_base()
csv_data = pd.DataFrame({})

def upload_data_to_mysql(df):
    global csv_data
    db_host = "172.31.44.125"
    db_name = "horse"
    db_user = "preferredequine"
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
            UTT = Column(String(255))
            STATUS = Column(String(255))
            TDAM = Column(String(255))
            tSire = Column(String(255))
            tSireofdam = Column(String(255))
            DAMSIRE_ID = Column(Integer, ForeignKey('tdamsire.DAMSIRE_ID'))
            # tdamsire = relationship("main_Tdamsire", back_populates="tsales")

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


        # Define tables
        Base.metadata.create_all(engine)

        # Define the relationship after both classes have been defined
        main_Tsales.tdamsire = relationship("main_Tdamsire", back_populates="tsales")

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

@app.route("/obs_redirect-mixed")
def obsRedirectMix():
    return render_template("obs.html")

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

        # while True:
        #     # Get user input for SALE date
        #     saledate = request.form['saledate']

        #     # Check if the user provided a date or canceled the dialog
        #     if saledate is not None:
        #         try:
        #             # Try to parse the input as a valid date
        #             saledate = pd.to_datetime(saledate)
        #             break  # Exit the loop if parsing is successful
        #         except ValueError:
        #             print("Invalid date format. Please enter a valid date (YYYY-MM-DD).")
        #     else:
        #         print("saledate input canceled.")
        #         break  # Exit the loop if the user cancels the dialog

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
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode


        # df['SALEDATE'] = pd.to_datetime(saledate)

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
        print(df[['DAY', 'SALEDATE']])
        
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
        if 'Elig' in df.columns:
            df['ELIG'] = df['Elig'].fillna("")

        if 'Elig' in df.columns:
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
        
        # Replace '---' with np.nan in 'Price' and convert to float
        if 'Price' in df.columns:
            df['Price'] = df['Price'].replace(price_mapping)
            df['PRICE'] = pd.to_numeric(df['Price'], errors='coerce')  # Convert to float
        else:
            df['PRICE'] = np.nan
        
        rna_price = None

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
        df['SALEYEAR'] = request.form['saleyear']

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

        # Adding a new column SALECODE
        df['SALECODE'] = salecode

        # Adding a new column SALEDATE
        if 'SESSION' in df.columns:
            # Convert 'SESSION' to datetime with the original format
            df['SALEDATE'] = pd.to_datetime(df['SESSION'], format='%m/%d/%Y')
            
            # Format 'SALEDATE' to 'YYYY-MM-DD' for MySQL compatibility
            df['SALEDATE'] = df['SALEDATE'].dt.strftime('%Y-%m-%d')

        # Adding a new column BOOK
        book = 1
        df['BOOK'] = book

        # Initialize a counter
        counter = 0

        # Initialize a list to store the counter values
        counter_values = []

        # Iterate through the list of dates
        for i, date_str in enumerate(df['SESSION']):
            try:
                if '/' in date_str:
                    # Split the date string into month, day, and year parts
                    month_str, day_str, year_str = date_str.split('/')
                    
                    # Add leading zeros if needed
                    month_str = month_str.zfill(2)
                    day_str = day_str.zfill(2)
                    
                    # Combine the parts back into a formatted date string
                    formatted_date_str = f'{year_str}-{month_str}-{day_str}'
                else:
                    # The date string is already in the desired format
                    formatted_date_str = date_str
                
                # Convert the formatted date string to a datetime object
                date1 = datetime.strptime(formatted_date_str, '%Y-%m-%d')
                
                # Convert the datetime object to the desired format
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
            df['DATEFOAL'] = pd.to_datetime(df['YEAR OF BIRTH'])

        # Dropping a column YEAR OF BIRTH
        if 'YEAR OF BIRTH' in df.columns:
            df.drop(columns=['YEAR OF BIRTH'], inplace=True)

        # Adding a new column YEARFOAL and getting the year from DATEFOAL
        df['YEARFOAL'] = df['DATEFOAL'].dt.year
        
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
        if 'CONSIGNOR NAME' in df.columns:
            df['BREDTO'] = df['CONSIGNOR NAME']
        else:
            df['BREDTO'] = ''
        # Dropping a column CONSIGNOR NAME
        if 'CONSIGNOR NAME' in df.columns:
            df.drop(columns=['CONSIGNOR NAME'], inplace=True)

        lastbred = '1901-01-01'
        df['LASTBRED'] = pd.to_datetime(lastbred)
        
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
        saleyear = request.form['saleyear']
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

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
        datefoal = df['Year']
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

        condition_covered_by = df['Covering Sire'].notna()
        # condition_foal = df['Produit'] == 'foal'
        condition_weanling = datefoal.dt.year == saleyear
        condition_datefoal = datefoal.dt.year == (saleyear - 1)
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

        df['UTT'] = 0.0
        df['STATUS'] = ""
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
        df['TYPE'] = df['horsetype'].fillna("")

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

        # Dropping a column SIRE1
        if 'sire_name' in df.columns:
            df.drop(columns=['sire_name'], inplace=True)

        # Adding a new column DAM
        if 'dam_name' in df.columns:
            df['DAM'] = df['dam_name'].fillna("")

        # Adding a new column CDAM
        if 'dam_name' in df.columns:
            df['CDAM'] = df['dam_name'].fillna("")

        # Dropping a column DAM1
        if 'dam_name' in df.columns:
            df.drop(columns=['dam_name'], inplace=True)

        # Adding a new column SIREOFDAM
        if 'dam_sire' in df.columns:
            df['SIREOFDAM'] = df['dam_sire'].fillna("")

        # Adding a new column CSIREOFDAM
        if 'dam_sire' in df.columns:
            df['CSIREOFDAM'] = df['dam_sire'].fillna("")

        # Dropping a column SIRE OF DAM
        if 'dam_sire' in df.columns:
            df.drop(columns=['dam_sire'], inplace=True)

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

        df.drop(columns=['ut_distance'], inplace=True)
        df.drop(columns=['ut_actual_date'], inplace=True)
        df.drop(columns=['ut_group'], inplace=True)
        df.drop(columns=['ut_set'], inplace=True)
        df.drop(columns=['lastbred'], inplace=True)
        df.drop(columns=['horsetype'], inplace=True)

        upload_data_to_mysql(df)

        return render_template("obs.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("obs.html", message=f'Error: {str(e)}', data=None)

@app.route('/obs-mixed', methods=['POST'])

def obsmixed():
    
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
        df['SALETYPE'] = request.form['type']

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

        df['UTT'] = df['ut_time'].fillna(0.0)

        df['STATUS'] = ""
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
        saleyear = request.form['saleyear']
        df['SALEYEAR'] = saleyear

        # Adding a new column SALETYPE
        df['SALETYPE'] = request.form['type']

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

         # Adding a new column TYPE
        condition_covered_by = df['Covered by'].notna()
        # condition_foal = df['Produit'] == 'foal'
        condition_weanling = df['Year Foaled'] == saleyear
        condition_datefoal = df['Year Foaled'] == (saleyear - 1)
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

        df['UTT'] = 0.0
        df['STATUS'] = ""
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
        df['SALETYPE'] = request.form['type']

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
        condition_covered_by = df['Pleine de'].notna()
        # condition_foal = df['Produit'] == 'foal'
        condition_weanling = datefoal.dt.year == saleyear
        condition_datefoal = datefoal.dt.year == (saleyear - 1)
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

        df['UTT'] = 0.0
        df['STATUS'] = ""

        upload_data_to_mysql(df)

        return render_template("arquana.html", message='File uploaded to database successfully', data=df.to_html())

    except Exception as e:
        # Log the exception or print the error message for debugging
        print(f"Error: {str(e)}")
        return render_template("arquana.html", message=f'Error: {str(e)}', data=None)

if __name__ == '__main__':
    app.run(debug=False)

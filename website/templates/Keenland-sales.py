# import os
# import tkinter as tk
# from tkinter import filedialog, simpledialog
# import pandas as pd
# from datetime import datetime
# from datetime import date
# import datetime
# from flask import Flask

# # Author: Monil Samir Mody
# # Updated Date: September 15, 2023
# # Description: This Python file updates the excel file with the following requirement for Fasig Tipton.

# # Open a file dialog for the user to choose a file
# # file_path = filedialog.__name__

# app = Flask(__name__)

# @app.route('/keenland', methods=['GET'])
# # Check if the user selected a file or canceled the dialog
# def run():
#     file_path = "C:/Users/monil/Downloads/WebSummaryPagesCSV.csv"
#     # Read the selected Excel file into a DataFrame
#     df = pd.read_csv(file_path)

#     # Prompt the user to insert the salecode using a dialog
#     salecode = simpledialog.askstring("Input", "Please enter the SALECODE:")

#     # Check if the user provided a salecode or canceled the dialog
#     if salecode is not None:

#         # Now you can work with the DataFrame 'df' and 'salecode' as needed

#         # For example, you can print the first few rows and the salecode:
#         #print(df.head())
#         salecode

#     else:
#         print("Salecode input canceled.")

# # Renaming HIP to HIP1
# # df.rename(columns={'Hip': 'HIP1'}, inplace=True)

# # # Renaming PRICE to PRICE1
# # df.rename(columns={'price': 'PRICE1'}, inplace=True)

# # Renaming PRIVATE SALE to PRIVATE SALE1
# # df.rename(columns={'PRIVATE SALE': 'PRIVATE SALE1'}, inplace=True)
    
#     df['SALEDATE1'] = pd.to_datetime(df['SALE DATE'])

# # Adding a new column SALEYEAR

#     df['SALEYEAR'] = df['SALEDATE1'].dt.year

# # # Calculating the year of birth from the datefoal
# # saledate_series = df['SALEDATE']

# # # Adding a new column YEARFOAL and getting the year from DATEFOAL
# # df['SALEYEAR'] = saledate_series.dt.year

# # Adding a new column SALETYPE
#     saletype = 'Y'
#     df['SALETYPE'] = saletype

#     # Adding a new column SALECODE
#     df['SALECODE'] = salecode


#     df['SALEDATE'] = df['SALEDATE1']

#     df.drop(columns=['SALEDATE1'], inplace=True)

#     # # Initialize a counter
#     # counter = 0

#     # # Initialize a list to store the counter values
#     # counter_values = []

#     # # Iterate through the list of dates
#     # for i, date_str in enumerate(df['SALE DATE']):
#     #     # Convert the date string to a datetime object
#     #     date = datetime.datetime.strptime(date_str, '%m/%d/%Y')
#     #     formatted_date = date.strftime('%Y-%m-%d')
#     #     # Check if this is the first date or if the date has changed from the previous one
#     #     if i == 0 or date != prev_date:
#     #         counter += 1  # Increment the counter when the date changes
#     #         prev_date = date  # Update the previous date
            
#     #     counter_values.append(counter)

#     if 'Book' in df.columns:
#         df['BOOK'] = df['Book']

#     # # Adding a new column DAY
#     # if 'SALEDATE' in df.columns:
#     df['DAY'] = df['Session']

#     df.drop(columns=['SALE DATE'], inplace=True)

#     # # Initialize a counter
#     # counter = 0

#     # # Initialize a list to store the counter values
#     # counter_values = []

#     # # Iterate through the list of dates
#     # for i, date_str in enumerate(df['SESSION']):
#     #     # Convert the date string to a datetime object
#     #     date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        
#     #     # Check if this is the first date or if the date has changed from the previous one
#     #     if i == 0 or date != prev_date:
#     #         counter += 1  # Increment the counter when the date changes
#     #         prev_date = date  # Update the previous date
            
#     #     counter_values.append(counter)

#     # # Adding a new column DAY
#     # df['DAY'] = counter_values

#     # Adding a new column HIP
#     df['HIP'] = df['Hip']

#     # Adding a new column HIPNUM
#     df['HIPNUM'] = df['Hip']

#     # Dropping a column HIP1
#     if 'HIP1' in df.columns:
#         df.drop(columns=['HIP1'], inplace=True)

#     # Check if 'NAME' is a column in the DataFrame
#     if 'Horse Name' in df.columns:
#                 # Create a new 'HORSE' column and populate it with 'NAME'
#                 df['HORSE'] = df['Horse Name']
#     else:
#         df['HORSE'] = ''

#     # Check if 'NAME' is a column in the DataFrame
#     if 'Horse Name' in df.columns:
#                 # Create a new 'HORSE' column and populate it with 'NAME'
#                 df['CHORSE'] = df['Horse Name']
#     else:
#         df['CHORSE'] = ''

#     # Check if 'NAME' is a column in the DataFrame
#     if 'Horse Name' in df.columns:
#                 # Dropping a column NAME
#                 df.drop(columns=['Horse Name'], inplace=True)

#     # Adding a new column RATING
#     rating = ''
#     df['RATING'] = rating

#     # Adding a new column TATTOO
#     tattoo = ''
#     df['TATTOO'] = tattoo

#     # Adding a new column DATEFOAL
#     df['DATEFOAL'] = pd.to_datetime(df['DOB'])

#     # Function to calculate the age from DATEFOAL
#     def calculate_age(datefoal):
#         today = date.today()
#         born = pd.to_datetime(datefoal, errors='coerce')  # Convert to datetime, handle invalid dates
#         age = today.year - born.dt.year - ((today.month * 100 + today.day) < (born.dt.month * 100 + born.dt.day))
#         return age

#     # Calling the calculate_age() function
#     age = calculate_age(df['DATEFOAL'])

#     # Adding a new column AGE
#     df['AGE'] = age

#     color_mapping = {
#         'BAY': 'B',
#         'CHESTNUT': 'CH',
#         'Dark Bay/Brown': 'BR',
#         'Gray/Roan': 'GR',
#         'DARK BAY/BROWN': 'BR',
#         'GRAY/ROAN': 'GR',
#         'Bay': 'B',
#         'Chestnut': 'CH',
#         'Black': 'BLK',
#         'BLACK': 'BLK'
#     }

#     # Adding a new column COLOR
#     df['COLOR'] = df['Color'].replace(color_mapping)

#     sex_mapping = {
#         'Colt': 'C',
#         'Filly': 'F',
#         'Mare': 'M',
#         'Gelding': 'G',
#         'Horse': 'H',
#         'Ridgling': 'R'
#     }
#     # Adding a new column SEX
#     if 'Sex' in df.columns:
#         df['SEX'] = df['Sex'].replace(sex_mapping)
#     else:
#         df['SEX'] = ''

#     # Adding a new column GAIT
#     gait = ''
#     df['GAIT'] = gait

#     type_mapping = {
#         'Racing or Broodmare Prospect': 'BRP',
#         'Broodmare': 'B',
#         'Weanling': 'W',
#         'Broodmare Prospect': 'BP',
#         'Stallion Prospect': 'SP',
#         'Stallion': 'T',
#         'YR': 'Y',
#         'Yearling': 'Y',
#         'Racing Prospect': 'RP',
#         'Racing or Stallion Prospect': 'SRP'
#     }
#     # Adding a new column TYPE
#     if 'SoldAs' in df.columns:
#         df['TYPE'] = df['SoldAs'].replace(type_mapping).fillna('Y')
#     elif 'Sold As' in df.columns:
#         df['TYPE'] = df['Sold As'].replace(type_mapping).fillna('Y')
#     elif 'Type' in df.columns:
#         df['TYPE'] = df['Type'].replace(type_mapping).fillna('Y')
#     else:
#         df['TYPE'] = 'Y'

#     if 'Type' in df.columns:
#         df.drop(columns=['Type'], inplace=True)

#     # Adding a new column RECORD
#     record = ''
#     df['RECORD'] = record

#     # Adding a new column ET
#     et = ''
#     df['ET'] = et

#     # Adding sate_mapping key: value pair to replace the value
#     # state_mapping = {
#     #     'KENTUCKY': 'KY',
#     #     'NEW YORK': 'NY',
#     #     'PENNSYLVANIA': 'PA',
#     #     'FLORIDA': 'FL',
#     #     'ONTARIO': 'ON',
#     #     'VIRGINIA': 'VA',
#     #     'LOUISIANA': 'LA',
#     #     'MARYLAND': 'MD',
#     #     'ARKANSAS': 'AR'
#     # }

#     # Replace state names in a new column 'ELIG' with state codes in the 'FOALED' column
#     if 'Area_Foaled' in df.columns:
#         df['ELIG'] = df['Area_Foaled']
#     else:
#         df['ELIG'] = ''

#     if 'Area_Foaled' in df.columns:
#         df.drop(columns=['Area_Foaled'], inplace=True)

#     # Adding a new column SIRE
#     df['SIRE'] =  df['Sire']

#     # Adding a new column CSIRE
#     df['CSIRE'] = df['Sire']

#     # Adding a new column DAM
#     df['DAM'] = df['Dam']

#     # Adding a new column CDAM
#     df['CDAM'] = df['Dam']

#     # Adding a new column SIREOFDAM
#     if 'Broodmare Sire' in df.columns:
#         df['SIREOFDAM'] = df['Broodmare Sire']
#     elif 'Sire Of Dam' in df.columns:
#         df['SIREOFDAM'] = df['Sire Of Dam']
#     else: 
#         df['SIREOFDAM'] = ''

#     # Adding a new column CSIREOFDAM
#     if 'Broodmare Sire' in df.columns:
#         df['CSIREOFDAM'] = df['Broodmare Sire']
#     elif 'Sire Of Dam' in df.columns:
#         df['CSIREOFDAM'] = df['Sire Of Dam']
#     else: 
#         df['CSIREOFDAM'] = ''

#     # Dropping columns Broodmare Sire and Sire Of Dam
#     if 'Broodmare Sire' in df.columns:
#         df.drop(columns=['Broodmare Sire'], inplace=True)
#     elif 'Sire Of Dam' in df.columns:
#         df.drop(columns=['Sire Of Dam'], inplace=True)

#     # Adding a new column DAMOFDAM
#     damofdam = ''
#     df['DAMOFDAM'] = damofdam

#     # Adding a new column CDAMOFDAM
#     cdamofdam = ''
#     df['CDAMOFDAM'] = cdamofdam

#     # Adding a new column DAMTATT
#     damtatt = ''
#     df['DAMTATT'] = damtatt

#     # Adding a new column DAMYOF
#     damyof = ''
#     df['DAMYOF'] = damyof

#     # Adding a new column DDAMTATT
#     ddamtatt = ''
#     df['DDAMTATT'] = ddamtatt

#     # Adding a new column BREDTO
#     if 'Covering Sire' in df.columns:
#         df['BREDTO'] = df['Covering Sire']
#     elif 'ConveringSire' in df.columns:
#         df['BREDTO'] = df['CoveringSire']
#     else:
#         df['BREDTO'] = ''
        
#     # Adding a new column LASTBRED
#     lastbred = ''
#     df['LASTBRED'] = lastbred

#     # Adding a new column CONLNAME
#     if 'Consignor' in df.columns:
#         df['CONLNAME'] = df['Consignor']
#     elif 'PropertyLine1' in df.columns:
#         df['CONLNAME'] = df['PropertyLine1']
#     else:
#         df['CONLNAME'] = ''

#     # Dropping a column PROPERTY LINE
#     if 'PropertyLine1' in df.columns:
#         df.drop(columns=['PropertyLine1'], inplace=True)
#     elif 'Consignor' in df.columns:
#         df.drop(columns=['Consignor'], inplace=True)
#     elif 'Farm Name' in df.columns:
#         df.drop(columns=['Farm Name'], inplace=True)

#     # Adding a new column CONSNO
#     consno = ''
#     df['CONSNO'] = consno

#     # Adding a new column PEMCODE
#     pemcode = ''
#     df['PEMCODE'] = pemcode

#     # Adding a new column PURFNAME
#     purfname = ''
#     df['PURFNAME'] = purfname

#     # Adding a new column PURLNAME
#     if 'Purchaser' in df.columns:
#         df['PURLNAME'] = df['Purchaser']
#     else: 
#         df['PURLNAME'] = ''


#     # Dropping a column PURCHASER
#     if 'Purchaser' in df.columns:
#         df.drop(columns=['Purchaser'], inplace=True)

#     # Adding a new column SBCITY
#     sbcity = ''
#     df['SBCITY'] = sbcity

#     # Adding a new column SBSTATE
#     sbstate = ''
#     df['SBSTATE'] = sbstate

#     # Adding a new column SBCOUNTRY
#     sbcountry = ''
#     df['SBCOUNTRY'] = sbcountry

#     price_mapping = {
#         '---': ''
#     }
#     # Adding a new column PRICE
#     if 'Price' in df.columns:
#         df['PRICE'] = df['Price'].replace(price_mapping)
#     else:
#         df['PRICE'] = ''

#     if 'Price' in df.columns:
#         df.drop(columns=['Price'], inplace=True)
    
#     # Adding a new column PRICE1
#     # df.drop(columns=['PRICE1'], inplace=True)

#     # Adding a new column SALE TITLE
#     # df.drop(columns=['SALE TITLE'], inplace=True)

#     # Adding a new column CURRENCY
#     currency = ''
#     df['CURRENCY'] = currency

#     # Adding a new column URL
#     url = '' 
#     df['URL'] = url

#     # Dropping a column VIRTUAL INSPECTION
#     # df.drop(columns=['VIRTUAL INSPECTION'], inplace=True)

#     # Adding a new column NFFM
#     nffm = ''
#     df['NFFM'] = nffm

#     # Adding a new column PRIVATE SALE
#     privatesale = ''
#     df['PRIVATESALE'] = privatesale

#     # Dropping a column PRIVATE SALE1
#     # df.drop(columns=['PRIVATE SALE1'], inplace=True)

#     # Adding a new column BREED
#     breed = 'T'
#     df['BREED'] = breed

#     # Adding a new column YEARFOAL and getting the year from DATEFOAL
#     df['YEARFOAL'] = df['DATEFOAL'].dt.year

#     # Dropping a column BARN
#     # df.drop(columns=['BARN'], inplace=True)

#     # Dropping a column COVER DATE
#     # df.drop(columns=['COVER DATE'], inplace=True)

#     # Dropping a column SOLD AS CODE
#     if 'SOLD AS CODE' in df.columns:
#         df.drop(columns=['SOLD AS CODE'], inplace=True)

#     if 'Session' in df.columns:
#         df.drop(columns=['Session'], inplace=True)

#     if 'Book' in df.columns:
#         df.drop(columns=['Book'], inplace=True)    

#     if 'Hip' in df.columns:
#         df.drop(columns=['Hip'], inplace=True)

#     if 'PropertyLine2' in df.columns:
#         df.drop(columns=['PropertyLine2'], inplace=True)

#     if 'Description' in df.columns:
#         df.drop(columns=['Description'], inplace=True)

#     if 'Farm Name' in df.columns:
#         df.drop(columns=['Farm Name'], inplace=True)

#     df.drop(columns=['DOB'], inplace=True)

#     df.drop(columns=['Color'], inplace=True)

#     df.drop(columns=['Sex'], inplace=True)

#     df.drop(columns=['Sire'], inplace=True)

#     df.drop(columns=['Dam'], inplace=True)

#     if 'CoveringSire' in df.columns:
#         df.drop(columns=['CoveringSire'], inplace=True)
#     elif 'Covering Sire' in df.columns:
#         df.drop(columns=['Covering Sire'], inplace=True)

#     if 'Breeding Status' in df.columns:
#         df.drop(columns=['Breeding Status'], inplace=True)
        
#     if 'Out' in df.columns:
#         df.drop(columns=['Out'], inplace=True)

#     if 'Location' in df.columns:
#         df.drop(columns=['Location'], inplace=True)

#     if 'LastService' in df.columns:
#         df.drop(columns=['LastService'], inplace=True)

#     if 'Pregnancy' in df.columns:
#         df.drop(columns=['Pregnancy'], inplace=True)

#     if 'SoldAs' in df.columns:
#         df.drop(columns=['SoldAs'], inplace=True)
#     elif 'Sold As' in df.columns:
#         df.drop(columns=['Sold As'], inplace=True)

#     if 'Price' in df.columns:
#         df.drop(columns=['Price'], inplace=True)

#     if 'Breeders Cup Eligible' in df.columns:
#         df.drop(columns=['Breeders Cup Eligible'], inplace=True)

#     # Dropping a column SOLD AS DESCRIPTION
#     # df.drop(columns=['SOLD AS DESCRIPTION'], inplace=True)

#     # Dropping a column FOALED
#     # df.drop(columns=['FOALED'], inplace=True)

#     # Determining the output path for the modified file
#     output_file_path = 'C:\\Users\\monil\\Downloads\\{}.csv'.format(salecode)

#     # Checking whether the file is already in the file explorer
#     if os.path.exists(output_file_path):
#         # If the file exists, remove it
#         os.remove(output_file_path)

#     # Saving the file as a csv extension
#     df.to_csv(output_file_path, index=False)

#     # Opening the file once it is converted to csv file
#     os.system(f'start {output_file_path}')

#     return df

# if __name__ == '__main__':
#     app.run(debug=True)
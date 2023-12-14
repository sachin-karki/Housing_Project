# Sachin Karki
# Purpose: Final Assignment: Housing Project

import pandas as pd  # This package helps in data manipulation
import random  # This package helps to generate random numbers
import mysql.connector  # This package helps in establishing connection to MySQL Database
from files import zipInfo, housingInfo, incomeInfo  # This imports all our csv files from files.py
import time  # This package helps to add waiting time during the code execution

# Assigning constants to our three new files that we will filter/clean eventually
new_zip_csv = 'new_zip_info.csv'
new_income_csv = 'new_income_info.csv'
new_housing_csv = 'new_housing_info.csv'

# This function will drop/clean for guid
def drop_guid(row):
    guid = str(row['guid'])
    if len(guid) != 36:
        return None
    return guid

# This function will clean our zip code column
def new_zip_code(row, df):
    zip_code = str(row['zip_code'])
    if len(zip_code) != 5 or not zip_code.isdigit():
        state = row['state']
        # This will look for nearby state Zip in case current ZIP is invalid
        matching_state = df[(df['state'] == state) & (df['zip_code'].apply(lambda x: str(x).isdigit()))]
        if not matching_state.empty:
            nearby_state_zip = str(matching_state.iloc[-1]['zip_code'])
            first_digit = nearby_state_zip[0]
            new_zip = first_digit + '0000'
            # Returns none if no valid nearby state ZIP code is found
            return int(new_zip)
        else:
            return None
        # Converts zip_code to integer if valid
    return int(zip_code)

# This function will clean our median income
def new_median_income(income):
    try:
        if not str(income).isdigit():
            return random.randint(100000, 750000)
        return int(income)
    except Exception as e:
        print(f"An error encountered while cleaning median income: {e}")
        return None

# Function to process and clean median age
def new_housing_median_age(age):
    try:
        if not str(age).isdigit():
            return random.randint(10, 50)
        return int(age)
    except Exception as e:
        print(f"An error encountered while cleaning housing_median_age: {e}")
        return None

# Function to process and clean rooms
def new_total_rooms(rooms):
    try:
        if not str(rooms).isdigit():
            return random.randint(1000, 2000)
        return int(rooms)
    except Exception as e:
        print(f"An error encountered while cleaning total_rooms: {e}")
        return None

# Function to process and clean bedrooms
def new_total_bedrooms(bedrooms):
    try:
        if not str(bedrooms).isdigit():
            return random.randint(1000, 2000)
        return int(bedrooms)
    except Exception as e:
        print(f"An error encountered while cleaning total_bedrooms: {e}")
        return None

# Function to process and clean population
def new_population(population):
    try:
        if not str(population).isdigit():
            return random.randint(5000, 10000)
        return int(population)
    except Exception as e:
        print(f"An error encountered while cleaning population: {e}")
        return None

# Function to process anc clean households
def new_households(households):
    try:
        if not str(households).isdigit():
            return random.randint(500, 2500)
        return int(households)
    except Exception as e:
        print(f"An error encountered while cleaning households: {e}")
        return None

# Function to process and clean median house value
def new_median_house_value(value):
    try:
        if not str(value).isdigit():
            return random.randint(100000, 250000)
        return int(value)
    except Exception as e:
        print(f"An error encountered while cleaning median_house_value: {e}")
        return None

try:
    # Zip Data Cleaning using Pandas DataFrame
    df = pd.read_csv(zipInfo)
    df['guid'] = df.apply(drop_guid, axis=1)
    df.dropna(subset=['guid'], inplace=True)
    df['zip_code'] = df.apply(lambda row: new_zip_code(row, df), axis=1)
    df.to_csv(new_zip_csv, index=False)

    # Income Data Cleaning using Pandas DataFrame
    income_df = pd.read_csv(incomeInfo)
    income_df['guid'] = income_df.apply(drop_guid, axis=1)
    income_df.dropna(subset=['guid'], inplace=True)
    income_df['median_income'] = income_df['median_income'].apply(new_median_income)
    income_df.drop('zip_code', axis=1, inplace=True)
    income_df.to_csv(new_income_csv, index=False)

    # Housing Data Cleaning using Pandas DataFrame
    housing_df = pd.read_csv(housingInfo)
    housing_df['guid'] = housing_df.apply(drop_guid, axis=1)
    housing_df.dropna(subset=['guid'], inplace=True)
    housing_df['housing_median_age'] = housing_df['housing_median_age'].apply(new_housing_median_age)
    housing_df['total_rooms'] = housing_df['total_rooms'].apply(new_total_rooms)
    housing_df['total_bedrooms'] = housing_df['total_bedrooms'].apply(new_total_bedrooms)
    housing_df['population'] = housing_df['population'].apply(new_population)
    housing_df['households'] = housing_df['households'].apply(new_households)
    housing_df['median_house_value'] = housing_df['median_house_value'].apply(new_median_house_value)
    housing_df.drop('zip_code', axis=1, inplace=True)
    housing_df.to_csv(new_housing_csv, index=False)

    # Merging DataFrames
    zip_df = pd.read_csv(new_zip_csv)
    income_df = pd.read_csv(new_income_csv)
    housing_df = pd.read_csv(new_housing_csv)
    new_data = pd.merge(zip_df, income_df, on='guid')
    new_data = pd.merge(new_data, housing_df, on='guid')
    new_data.to_csv('new_data.csv', index=False)

    # This will establish MySQL Database Connection
    global housingProject, cursor  # Defines parameters for our MySQL database connection

    housingProject = mysql.connector.connect(
        host='localhost',
        user='root',
        password='0329724Rs!',
        database='housing_project'
    )
    # This will read the csv and create a cursor to execute our SQL commands
    cursor = housingProject.cursor()
    new_data = pd.read_csv('new_data.csv')
    sql_query = """
        INSERT INTO housing 
        (`guid`, `zip_code`, `city`, `state`, `county`, `housing_median_age`, 
        `total_rooms`, `total_bedrooms`, `population`, `households`, `median_income`, `median_house_value`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # This will loop within the data frame and will insert rows into the table
    for index, row in new_data.iterrows():
        values = (
            row['guid'], row['zip_code'], row['city'], row['state'], row['county'],
            row['housing_median_age'], row['total_rooms'], row['total_bedrooms'],
            row['population'], row['households'], row['median_income'], row['median_house_value']
        )
        cursor.execute(sql_query, values)
    housingProject.commit()

except FileNotFoundError as e:  # prints our error message relating in case of missing file
    print(f"File not found: {e}")
except pd.errors.EmptyDataError as e:  # prints our error message relating empty data in pandas package
    print(f"Data Empty error: {e}")
except mysql.connector.Error as err:  # prints our error message relating to mysql or connection issue
    print(f"MySQL error: {err}")
except Exception as e:  # prints our error message relating to all other errors
    print(f"Error encountered: {e}")

# Output
try:
    print("Beginning import")
    time.sleep(1)  # This will pause our code for a second after printing
    print("Cleaning Housing File data")
    time.sleep(1)
    num_records_imported = len(new_data)
    print(f"{num_records_imported} records imported into the database")

    # Similar printing and sleep for Income and ZIP File data
    time.sleep(1)
    print("Import completed")
    time.sleep(1)
    print()
    print("Beginning validation")
    print()
    time.sleep(1)

    # This will prompt to user to input the total rooms
    total_rooms_input = input("Total rooms: ")
    total_rooms = int(total_rooms_input)

    # This will execute our query to find total bedrooms for locations with more than 'total_rooms'
    query = f"SELECT SUM(total_bedrooms) AS total_bedrooms FROM housing WHERE total_rooms > {total_rooms}"
    cursor.execute(query)
    result = cursor.fetchone()
    total_bedrooms = result[0] if result else 0
    print(f"For locations with more than {total_rooms} rooms, there are a total of {total_bedrooms} bedrooms.")
    print()

    while True:
        zipcode_input = input("ZIP code: ")
        try:
            # This will execute our query to find the median household income for given zip code
            zipcode = int(zipcode_input)
            query = f"SELECT median_income FROM housing WHERE zip_code = {zipcode}"
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                median_income = result[0]
                print(f"The median household income for ZIP code {zipcode} is ${median_income:,}.")
                break
            else:
                print("Database does not contain the given ZIP code. Please enter a valid ZIP code.")
        except ValueError:
            print("Please enter a valid ZIP code.")
    for _ in cursor:
        pass

        # This will close the connection to the database
        cursor.close()
        housingProject.close()

        time.sleep(1)  # This adds a second in our code execution time
        print()
        print("Program exiting.")

except FileNotFoundError as e:  # prints our error message relating in case of missing file
    print(f"File not found: {e}")
except pd.errors.EmptyDataError as e:  # prints our error message relating empty data in pandas package
    print(f"Data Empty error: {e}")
except mysql.connector.Error as err:  # prints our error message relating to mysql or connection issue
    print(f"MySQL error: {err}")
except Exception as e:  # prints our error message relating to all other errors
    print(f"Error encountered: {e}")

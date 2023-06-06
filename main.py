# 1. Ask for year and term
# 2. Get information about the classes using the API
#    - define list with all Business Majors and loop through it
#    - filter out classes that were cancelled
#    - save results in pandas dataframe (code, instructor, term, crn)
# 3. Remove duplicates (if code and instructor are the same, syllabus will be the same, remove all but one from df)
# 4. Get full names of the instructors using another API call (using the crn from the step before)
#    - replace old instructor column in the dataframe with first_name and last_name columns
# 5. Get the emails of the instructors using OSU's Lightweight Directory Access Protocol API
#    - loop thhrough dataframe and retrieve email for each professor using their first and last name
#    - add emails to the dataframe (new column)
# 6. Download dataframe as a csv file (should have this format: https://canvas.oregonstate.edu/files/96751469)

# Tips and code from Dr. Reitsma:
# https://canvas.oregonstate.edu/courses/1928058/pages/some-services-wich-can-significantly-improve-jws-process-2?module_item_id=23062509


import requests
import json
import pandas as pd
import re
import sys
from ldap3 import Server, Connection, ALL, SUBTREE

# List containing all the different Business Majors at OSU
majors = ["MGMT", "HM", "FIN", "DSGN", "SCLM", "MRKT", "BIS", "BANA", "BA", "ACTG"]

# Dictionary of the term codes and corresponding terms
term_dict = {"01": "Fall", "02": "Winter", "03": "Spring", "04": "Summer"}


def get_year_and_term():
    """Fuction that asks the user via the command line what year and term he wants the data for."""
    pass


def get_classes(year, term_code):
    """Function that uses the classes.oregonstate.edu API to extract all Business classes and their instructors for
       the current term."""

    # List containing dataframes with the data for each major
    data_frames = []

    # OSU course catalog URL
    url = "https://classes.oregonstate.edu/api/?page=fose&route=search"

    # Get all the classes for each Major
    for major in majors:
        # Set up the query
        query_dict = {
            "other": {"srcdb": year + term_code},
            "criteria": [{"field": "subject", "value": major}]
        }

        # Convert query_dict into string
        query_str = json.dumps(query_dict)

        try:
            # Make POST request; pass query_str as data
            response = requests.post(url, data=query_str, timeout=10)

            api_data = json.loads(response.text)

            # Extract crn and title from each result
            results = api_data['results']

            # Filter out cancelled courses
            results = [result for result in results if result.get("isCancelled") != "1"]

            new_data = pd.DataFrame({'Term': term_dict.get(term_code) + " " + year,
                                     'CRN': [result['crn'] for result in results],
                                     'Course': [result['code'] for result in results],
                                     'Instructor': [result['instr'] for result in results]})

            # Append the new DataFrame to the list of data_frames
            data_frames.append(new_data)

        except Exception as e:
            print("Error... API call failed:", e)
            exit(1)

    # Concatenate all the DataFrames in the list to have one dataframe containing all the courses of the term
    df_term_data = pd.concat(data_frames, ignore_index=True)
    print("Courses retrieved from API.")

    return df_term_data


def remove_duplicates(dataframe):
    """Function that removes all duplicate courses from the dataframe. If an instructor teaches the same course at
    different times a day, the syllabus will still be the same so duplicate entries in the dataframe aren't needed. """

    df_no_duplicates = dataframe.drop_duplicates(subset=["Course", "Instructor"], keep="first").reset_index(drop=True)

    # Remove duplicates with "H" at the end of the course (hybrid classes that have the same syllabus as the normal one)
    df_no_duplicates["Course"] = df_no_duplicates["Course"].str.replace("H$", "", regex=True)
    df_no_duplicates = df_no_duplicates.drop_duplicates(subset=["Course", "Instructor"], keep="first").reset_index(
        drop=True)
    print("Duplicates removed.")

    return df_no_duplicates


def merge_classes_for_instructor(dataframe):
    """Function that merges all the courses of one instructor into one cell and drops all the other lines of the
    instructor."""

    df_merged = dataframe.groupby("Instructor").agg({"Course": ", ".join, "Term": "first", "CRN": "first"}).reset_index()
    print("Courses merged.")

    return df_merged


def get_instructor_name(dataframe, term):
    url = "https://classes.oregonstate.edu/api/?page=fose&route=details"

    # Initialize empty lists for first names and last names
    first_names = []
    last_names = []

    for crn in dataframe['CRN']:
        # Set up the query
        query_dict = {"srcdb": term, "key": f"crn:{crn}"}

        # Convert query_dict into string
        query_str = json.dumps(query_dict)

        try:
            # Make POST request; pass query_str as data
            response = requests.post(url, data=query_str, timeout=10)
            api_data = json.loads(response.text)
            instructor_detail = api_data["instructordetail_html"]

            # Extract the name using regular expressions
            match = re.search(r'<div class="instructor-detail">(.+?)</div>', instructor_detail)
            if match:
                full_name = match.group(1)
                first_name, last_name = full_name.split(' ', 1)
            else:
                first_name, last_name = "", ""

            # Append first name and last name to the lists
            first_names.append(first_name)
            last_names.append(last_name)

        except Exception as e:
            print("Error... API call failed:", e)
            exit(1)

    # Drop columns Instructor and CRN
    dataframe = dataframe.drop(['Instructor', 'CRN'], axis=1)

    # Add new columns to the dataframe
    dataframe['First_Name'] = first_names
    dataframe['Last_Name'] = last_names
    print("First and last names added to the dataframe.")

    return dataframe
    

def get_emails(dataframe):
    """Function for querying OSU's Lightweight Directory Access Protocol API for the instructor's emails.
    Requires ONID username and password."""

    # Load credentials from the configuration file
    with open('config.json') as file:
        config = json.load(file)

    # Access the credentials
    ldap_login = config['username']
    ldap_password = config['password']

    # Define the server
    server = Server('onid-k-dc01.onid.oregonstate.edu', get_info=ALL)

    # Define the connection
    connect = Connection(server, user='onid\\' + ldap_login, password=ldap_password)

    # Bind
    if not connect.bind():
        print('error in bind', connect.result)
        exit(1)

    # Create empty email list that saves all the email of the instructors, will later be added as a column
    email_list = []

    # Loop through all the instructors in the dataframe and collect their emails
    for first_name, last_name in dataframe[['First_Name', 'Last_Name']].itertuples(index=False):
        # Set search parameters
        ldap_filter = "(&(sn=" + last_name + ")(givenName=" + first_name + "))"

        # Set attributes to return
        ldap_attributes = ["userPrincipalName"]

        # Search
        try:
            connect.search(search_base='DC=onid,DC=oregonstate,DC=edu',
                           search_filter=ldap_filter,
                           attributes=ldap_attributes,
                           search_scope=SUBTREE)
        except:
            print("Error... searching")
            exit(1)

        # Extract the email address from the response
        if len(connect.response) >= 1:
            email = (connect.response[0]['attributes']['userPrincipalName'])
            email_list.append(email)
        else:
            email_list.append(None)

    # Add email_list as a new column to the DataFrame
    dataframe['Email'] = email_list
    print("Emails added to the dataframe.")

    return dataframe


def etl_pipeline():
    """Function that executes all the other funtions after each other to execute the API calls and retrieve the data.
    After that it downloads the dataframe as a csv file which can be later be used in John Womack's process."""

    df = get_classes("2023", "03")
    df_without_duplicates = remove_duplicates(df)
    df_merged = merge_classes_for_instructor(df_without_duplicates)
    df_with_full_names = get_instructor_name(df_merged, "202303")
    df_emails = get_emails(df_with_full_names)

    # Define the desired column order
    column_order = ['Last_Name', 'First_Name', 'Term', 'Course', 'Email']
    # Rearrange the columns
    final_dataframe = df_emails[column_order]

    # Save the final dataframe to a CSV file
    final_dataframe.to_csv('CSV_going_into_Qualtrics_automated.csv', index=False)
    print("Csv file downloaded.")


if __name__ == "__main__":
    etl_pipeline()

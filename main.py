# 1. Ask for year and term
# 2. Get information about the classes using the API
#    - define list with all Business Majors and loop through it
#    - save results in pandas dataframe (code, instructor, term, crn)
# 3. Remove duplicates (if code and instructor are the same, syllabus will be the same, remove all but one from df)
# 4. Get full names of the instructors using another API call (using the crn from the step before)
#    - replace old instructor column in the dataframe with first_name and last_name columns
# 4. Get the emails of the instructors using OSU's Lightweight Directory Access Protocol API
#    - loop thhrough dataframe and retrieve email for each professor using their first and last name
#    - add emails to the dataframe (new column)
# 5. Download dataframe as a csv file (should have this format: https://canvas.oregonstate.edu/files/96751548/)

# Tips and code from Dr. Reitsma:
# https://canvas.oregonstate.edu/courses/1928058/pages/some-services-wich-can-significantly-improve-jws-process-2?module_item_id=23062509


import requests
import json
import pandas as pd
import re

# List containing all the different Business Majors at OSU
majors = ["MGMT", "HM", "FIN", "DSGN", "SCLM", "MRKT", "BIS", "BANA", "BA", "ACTG"]


def get_classes(year, term):
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
            "other": {"srcdb": year + term},
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

            new_data = pd.DataFrame({'Term': year + term,
                                     'CRN': [result['crn'] for result in results],
                                     'Course': [result['title'] for result in results],
                                     'Instructor': [result['instr'] for result in results]})

            # Append the new DataFrame to the list of data_frames
            data_frames.append(new_data)

        except Exception as e:
            print("Error... API call failed:", e)
            exit(1)

    # Concatenate all the DataFrames in the list to have one dataframe containing all the courses of the term
    df_term_data = pd.concat(data_frames, ignore_index=True)
    print("Courses from API retrieved.")

    return df_term_data


def remove_duplicates(dataframe):
    df_no_duplicates = dataframe.drop_duplicates(subset=["Course", "Instructor"], keep="first").reset_index(drop=True)
    print("Duplicates removed.")

    return df_no_duplicates


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

    # Drop column Instructor
    dataframe = dataframe.drop('Instructor', axis=1)

    # Add new columns to the dataframe
    dataframe['First Name'] = first_names
    dataframe['Last Name'] = last_names
    print("First and last names added to the dataframe.")

    return dataframe
    

def get_emails(first_name, last_name):
    """Function for querying OSU's Lightweight Directory Access Protocol API for the instructor's emails.
    Requires ONID username and password."""
    pass


def etl_pipeline():
    """Function that executes all the other funtions after each other to execute the API calls and retrieve the data.
    After that it downloads the dataframe as a csv file which can be later be used in John Womack's process"""
    pass


# tests
df = get_classes("2023", "03")
df_without_duplicates = remove_duplicates(df)
df_with_full_names = get_instructor_name(df_without_duplicates, "202303")
print(df_with_full_names)
print(df_with_full_names.head())

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

# Dataframe that will later contain all the data retrieved from the API
column_names = ['instructor', 'Term', 'Course', 'CRN']
df = pd.DataFrame(columns=column_names)

# List containing all the different Business Majors at OSU
majors = ["MGMT", "HM", "FIN", "DSGN", "SCLM", "MRKT", "BIS", "BANA", "BA", "ACTG"]


def get_classes(year, term):
    """Function that uses the classes.oregonstate.edu API to extract all Business classes and their instructors for
       the current term."""

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

        # print("query_str: ", query_str)
        try:
            # Make POST request; pass query_str as data
            response = requests.post(url, data=query_str, timeout=10)
        except:
            print("Error... API call failed")
            exit(1)

        # printing data to see what API returns, later the important data should be saved here
        print(response.text)
        print(" ")


def remove_duplicates(dataframe):
    """Function for removing duplicate classes from the dataframe."""
    pass


def get_instructor_names(crn):
    """Function for querying the OSU Course Catalog API for full instructor names."""
    pass


def get_emails(first_name, last_name):
    """Function for querying OSU's Lightweight Directory Access Protocol API for the instructor's emails.
    Requires ONID username and password."""
    pass


def etl_pipeline():
    """Function that executes all the other funtions after each other to execute the API calls and retrieve the data.
    After that it downloads the dataframe as a csv file which can be later be used in John Womack's process"""
    pass


# test for first function
get_classes("2023", "03")

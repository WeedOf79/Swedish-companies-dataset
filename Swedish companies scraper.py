# Valerio "Weed" Malerba, Uppsala, 2023-2024

import requests
from bs4 import BeautifulSoup
import datetime
import time
import pandas as pd
import json
import math
import urllib.parse
import random
import subprocess




def Log(Text, Print):
    f = open("Log.txt", 'a')
    try:
        f.write(str(datetime.datetime.now()) + ":\t" + Text + "\n")
    except:
        f.write(str(datetime.datetime.now()) + ":\t**** UNMAPPABLE CHARACTER DETECTED ****\n")
    f.close()
    if Print:
        print(str(datetime.datetime.now()) + ":\t" + Text + "\n")

def wait_with_random_delay(base_delay):
    # Generates a random number between Base_delay and 5 * Base_delay
    random_delay = random.uniform(base_delay, 2 * base_delay)
    # Adds an extra time of 30 seconds with a probability of 1 in (20 + random number between -10 and +10)
    if random.randint(0, 200) == 0:
        random_delay += random.randint(20, 40)
        print(f"Time for a {int(random_delay)} seconds break!")
        remaining_time = random_delay
        while remaining_time >= 5:
            time.sleep(5)
            remaining_time -= 5
            print(f"{int(remaining_time)} seconds left.")
    else:
        time.sleep(random_delay)
    # Returns the actual waiting time
    return random_delay

def Clean_text(Text):
    Text = Text.replace("\\u00e5", "å")
    Text = Text.replace("\\u00c5", "Å")
    Text = Text.replace("\\u00c4", "Ä")
    Text = Text.replace("\\u00e4", "ä")
    Text = Text.replace("\\u00f6", "ö")
    Text = Text.replace("&amp;", "&")
    Text = Text.replace("\"", "")
    return Text

def Retrieve_company_page_list(PC, P):
    global Error_count
    Result = pd.DataFrame()
    try:
        # Extracts the HTML code part related to the search results
        Search_results_HTML = BeautifulSoup(PC, 'html.parser').find('search')[':search-result-default']
        # From the HTML code, extract the JSON code that actually contains information about the companies.
        # The page content is essentially JSON code, consisting of about twenty companies.
        Search_results_JSON = json.loads(Search_results_HTML)
        # Finally, the list of dictionaries created with json.loads is converted into a DataFrame
        Result = pd.DataFrame(Search_results_JSON)
    except Exception as e:
        Log(f"Error retrieving page list at {PC}: {e}", True)
    return Result

def Get_closure_data(C, delay):
    global Error_count
    Max_attempts = 10
    Name = str(C[1]["jurnamn"]).replace("&amp;", "&")
    # The tables containing data from previous exercises are in a URL different from that shown in the browser
    Closure_URL = "https://www.allabolag.se/" + str(C[1]['linkTo']).split("/")[0] + "/bokslut"
    # The while loop allows you to attempt to access the page multiple times in case it fails on the first try
    Succeeded = False
    Attempt_number = 0
    while not Succeeded and Attempt_number < Max_attempts:
        Company_merged_data = pd.DataFrame()
        Attempt_number = Attempt_number + 1
        try:
            # wait_with_random_delay(delay)
            # All data from the page is placed in a list of dataframes:
            Company_tables = pd.read_html(Closure_URL)
            Succeeded = True
        except:
            # If the first attempt fails, try again after a short wait
            time.sleep(Attempt_number * 1.5)
            print("\t\t\tAttempt " + str(Attempt_number) + " to get data about " + Name + " failed. Retrying...")
    # If accessing the data was not successful, log the error; otherwise, continue with cleaning the obtained data
    if Attempt_number >= Max_attempts and not Succeeded:
        Log("Data request error about " + Name, True)
        Error_count = Error_count + 1
    else:
        try:
            # Set names to the row labels using the data from the first column:
            Company_tables[0].index = Company_tables[0]["Resultaträkning (tkr)"]
            Company_tables[0].drop("Resultaträkning (tkr)", axis=1, inplace=True)
            Company_tables[1].index = Company_tables[1]["Balansräkningar (tkr)"]
            Company_tables[1].drop("Balansräkningar (tkr)", axis=1, inplace=True)
            Company_tables[2].index = Company_tables[2]["Löner & utdelning (tkr)"]
            Company_tables[2].drop("Löner & utdelning (tkr)", axis=1, inplace=True)
            Company_tables[3].drop("Nyckeltal", axis=1, inplace=True)
            # The index names are to be the same as those that will eventually become the columns of the dataset
            Company_tables[3].index = KPIs
            # Remove columns without actual data
            Company_tables[1].drop(["Tillgångar", "Skulder, eget kapital och avsättningar"], inplace=True)
            # The table with performance indices does not have column labels by itself. However, these are the same columns as the other tables.
            # Simply assign column names to the performance index table to be able to integrate it correctly into Company_merged_data
            Company_tables[3].columns = Company_tables[2].columns
            Company_merged_data = pd.concat([Company_tables[0], Company_tables[1], Company_tables[2], Company_tables[3]])
            # Typically, Company_merged_data has as its last column something called "Unnamed: ##", which contains only NaN. So remove it:
            Company_merged_data = Company_merged_data[
                Company_merged_data.columns.drop(list(Company_merged_data.filter(regex="Unnamed")))]
        except:
            Log("Error merging data about " + Name, True)
            Error_count = Error_count + 1
    # Since the fiscal years do not all end in the same month, depending on the company, there would be, for each year, a multitude of different indices (e.g., 2020-12, 2020-06, etc.).
    # This would result in many columns of the final dataset almost empty, so here the indication of the month in the index name is eliminated:
    Company_stacked_data = pd.DataFrame(Company_merged_data.stack())
    Company_stacked_data.index = Company_stacked_data.index.map("_".join)
    Shortened_index = []
    for Index in Company_stacked_data.index:
        Shortened_index.append(Index[:-3])
    Company_stacked_data.index = Shortened_index
    # However, there are cases of companies that have prepared multiple balance sheets in the same year. This results in columns with the same name, causing problems
    # when it comes to pouring data into the final dataset.
    # For simplicity, it is accepted to eliminate duplicate columns. Note: the "duplicated()" method only eliminates columns, so, if you want
    # to eliminate rows, you need to do a double transpose before and after the elimination.
    Result_df = Company_stacked_data.transpose().loc[:, ~Company_stacked_data.transpose().columns.duplicated()].transpose()
    return Result_df

def Get_activity_data(c, delay):
    global Error_count
    max_attempts = 10
    name = str(c[1]["jurnamn"]).replace("&amp;", "&")
    # The generic data about the activity are in another URL
    activity_URL = "https://www.allabolag.se/" + str(c[1]['linkTo']).split("/")[0] + "/verksamhet"
    # The while loop allows you to attempt to access the page multiple times in case it fails on the first try
    succeeded = False
    attempt_number = 0
    activity_details = []
    while not succeeded and attempt_number < max_attempts:
        attempt_number = attempt_number + 1
        try:
            # wait_with_random_delay(delay)
            soup = BeautifulSoup(requests.get(activity_URL).content, 'html.parser')
            try:
                status = soup.find('dt', string='Status').find_next('dd').get_text(strip=True)
            except:
                status = "-"
            try:
                registration_date = soup.find('dt', string='Bolaget registrerat').find_next('dd').get_text(strip=True)
            except:
                registration_date = "0000-01-01"
            try:
                ownership = soup.find('dt', string='Ägandeförhållande').find_next('dd').get_text(strip=True)
            except:
                ownership = "-"
            try:
                municipality = soup.find('dt', string='Kommunsäte').find_next('dd').get_text(strip=True)
            except:
                municipality = "-"
            activity_details = pd.DataFrame({"Status": status, "Bolaget registrerat": registration_date, "Ägandeförhållande": ownership, "Kommunsäte": municipality}, index = [0])
            # This dataframe has different columns time after time and generally the soup's structure looks different for e. g. deregistered companies. To get all the information in the right places would need
            # a much more complex code. Since this seems to happen only with companies which aren't active anymore, the choice here is just to skip them as soon as the following check returns true:
            try:
                activity_details = activity_details[["Status", "Bolaget registrerat", "Ägandeförhållande", "Kommunsäte"]]
            except:
                # Sometimes the field "Kommunsäte" doesn't appear in the soup. This happens e. g. with companies that have gone in bankruptcy recently, as their web page looks a bit different.
                # For these companies, the field is set to "-".
                activity_details = activity_details[["Status", "Bolaget registrerat", "Ägandeförhållande"]]
                activity_details = activity_details.copy()
                activity_details.loc[0, "Kommunsäte"] = "-"
            succeeded = True
        except Exception as e:
            # If the first attempt fails, try again after a short wait
            time.sleep(2 + attempt_number * 1.5)
            print("\t\t\tAttempt " + str(attempt_number) + " to get data about " + name + f" failed, due to '{e}'. Retrying...")
            Log(f"Error retriveing activity data for {Name}: {e}", False)
    return activity_details




# INIT

List = []
# First, open the page and look for company-related data.
# Each page should contain identifiers for 20 different companies.
Error_count = 0
Delay = 0.0
Raw_data_folder = "D:\Documents\Python Scripts\Scrapers\Bolagsskrapare\Raw data\\"
Count = 0

Company_dataset_columns = [
    "orgnr",
	"abv_hgrupp",
	"abv_ugrupp",
	"ba_postort",
	"linkTo",
    "Status",
    "Bolaget registrerat",
    "Ägandeförhållande",
    "Kommunsäte",
	"Anläggningstillgångar_2012",
	"Anläggningstillgångar_2013",
	"Anläggningstillgångar_2014",
	"Anläggningstillgångar_2015",
	"Anläggningstillgångar_2016",
	"Anläggningstillgångar_2017",
	"Anläggningstillgångar_2018",
	"Anläggningstillgångar_2019",
	"Anläggningstillgångar_2020",
	"Anläggningstillgångar_2021",
	"Anläggningstillgångar_2022",
	"Antal_anställda_2012",
	"Antal_anställda_2013",
	"Antal_anställda_2014",
	"Antal_anställda_2015",
	"Antal_anställda_2016",
	"Antal_anställda_2017",
	"Antal_anställda_2018",
	"Antal_anställda_2019",
	"Antal_anställda_2020",
	"Antal_anställda_2021",
	"Antal_anställda_2022",
	"Årets resultat_2012",
	"Årets resultat_2013",
	"Årets resultat_2014",
	"Årets resultat_2015",
	"Årets resultat_2016",
	"Årets resultat_2017",
	"Årets resultat_2018",
	"Årets resultat_2019",
	"Årets resultat_2020",
	"Årets resultat_2021",
	"Årets resultat_2022",
	"Avsättningar (tkr)_2012",
	"Avsättningar (tkr)_2013",
	"Avsättningar (tkr)_2014",
	"Avsättningar (tkr)_2015",
	"Avsättningar (tkr)_2016",
	"Avsättningar (tkr)_2017",
	"Avsättningar (tkr)_2018",
	"Avsättningar (tkr)_2019",
	"Avsättningar (tkr)_2020",
	"Avsättningar (tkr)_2021",
	"Avsättningar (tkr)_2022",
	"Bruttovinstmarginal_2012",
	"Bruttovinstmarginal_2013",
	"Bruttovinstmarginal_2014",
	"Bruttovinstmarginal_2015",
	"Bruttovinstmarginal_2016",
	"Bruttovinstmarginal_2017",
	"Bruttovinstmarginal_2018",
	"Bruttovinstmarginal_2019",
	"Bruttovinstmarginal_2020",
	"Bruttovinstmarginal_2021",
	"Bruttovinstmarginal_2022",
	"Du Pont-modellen_2012",
	"Du Pont-modellen_2013",
	"Du Pont-modellen_2014",
	"Du Pont-modellen_2015",
	"Du Pont-modellen_2016",
	"Du Pont-modellen_2017",
	"Du Pont-modellen_2018",
	"Du Pont-modellen_2019",
	"Du Pont-modellen_2020",
	"Du Pont-modellen_2021",
	"Du Pont-modellen_2022",
	"Eget kapital_2012",
	"Eget kapital_2013",
	"Eget kapital_2014",
	"Eget kapital_2015",
	"Eget kapital_2016",
	"Eget kapital_2017",
	"Eget kapital_2018",
	"Eget kapital_2019",
	"Eget kapital_2020",
	"Eget kapital_2021",
	"Eget kapital_2022",
	"Kassalikviditet_2012",
	"Kassalikviditet_2013",
	"Kassalikviditet_2014",
	"Kassalikviditet_2015",
	"Kassalikviditet_2016",
	"Kassalikviditet_2017",
	"Kassalikviditet_2018",
	"Kassalikviditet_2019",
	"Kassalikviditet_2020",
	"Kassalikviditet_2021",
	"Kassalikviditet_2022",
	"Kortfristiga skulder_2012",
	"Kortfristiga skulder_2013",
	"Kortfristiga skulder_2014",
	"Kortfristiga skulder_2015",
	"Kortfristiga skulder_2016",
	"Kortfristiga skulder_2017",
	"Kortfristiga skulder_2018",
	"Kortfristiga skulder_2019",
	"Kortfristiga skulder_2020",
	"Kortfristiga skulder_2021",
	"Kortfristiga skulder_2022",
	"Långfristiga skulder_2012",
	"Långfristiga skulder_2013",
	"Långfristiga skulder_2014",
	"Långfristiga skulder_2015",
	"Långfristiga skulder_2016",
	"Långfristiga skulder_2017",
	"Långfristiga skulder_2018",
	"Långfristiga skulder_2019",
	"Långfristiga skulder_2020",
	"Långfristiga skulder_2021",
	"Långfristiga skulder_2022",
	"Löner till övriga anställda_2012",
	"Löner till övriga anställda_2013",
	"Löner till övriga anställda_2014",
	"Löner till övriga anställda_2015",
	"Löner till övriga anställda_2016",
	"Löner till övriga anställda_2017",
	"Löner till övriga anställda_2018",
	"Löner till övriga anställda_2019",
	"Löner till övriga anställda_2020",
	"Löner till övriga anställda_2021",
	"Löner till övriga anställda_2022",
	"Löner till styrelse & VD_2012",
	"Löner till styrelse & VD_2013",
	"Löner till styrelse & VD_2014",
	"Löner till styrelse & VD_2015",
	"Löner till styrelse & VD_2016",
	"Löner till styrelse & VD_2017",
	"Löner till styrelse & VD_2018",
	"Löner till styrelse & VD_2019",
	"Löner till styrelse & VD_2020",
	"Löner till styrelse & VD_2021",
	"Löner till styrelse & VD_2022",
	"Minoritetsintressen_2015",
	"Minoritetsintressen_2016",
	"Minoritetsintressen_2017",
	"Minoritetsintressen_2018",
	"Minoritetsintressen_2020",
	"Nettoomsättning per anställd (tkr)_2012",
	"Nettoomsättning per anställd (tkr)_2013",
	"Nettoomsättning per anställd (tkr)_2014",
	"Nettoomsättning per anställd (tkr)_2015",
	"Nettoomsättning per anställd (tkr)_2016",
	"Nettoomsättning per anställd (tkr)_2017",
	"Nettoomsättning per anställd (tkr)_2018",
	"Nettoomsättning per anställd (tkr)_2019",
	"Nettoomsättning per anställd (tkr)_2020",
	"Nettoomsättning per anställd (tkr)_2021",
	"Nettoomsättning per anställd (tkr)_2022",
	"Nettoomsättning_2012",
	"Nettoomsättning_2013",
	"Nettoomsättning_2014",
	"Nettoomsättning_2015",
	"Nettoomsättning_2016",
	"Nettoomsättning_2017",
	"Nettoomsättning_2018",
	"Nettoomsättning_2019",
	"Nettoomsättning_2020",
	"Nettoomsättning_2021",
	"Nettoomsättning_2022",
	"Nettoomsättningsförändring_2012",
	"Nettoomsättningsförändring_2013",
	"Nettoomsättningsförändring_2014",
	"Nettoomsättningsförändring_2015",
	"Nettoomsättningsförändring_2016",
	"Nettoomsättningsförändring_2017",
	"Nettoomsättningsförändring_2018",
	"Nettoomsättningsförändring_2019",
	"Nettoomsättningsförändring_2020",
	"Nettoomsättningsförändring_2021",
	"Nettoomsättningsförändring_2022",
	"Obeskattade reserver_2012",
	"Obeskattade reserver_2013",
	"Obeskattade reserver_2014",
	"Obeskattade reserver_2015",
	"Obeskattade reserver_2016",
	"Obeskattade reserver_2017",
	"Obeskattade reserver_2018",
	"Obeskattade reserver_2019",
	"Obeskattade reserver_2020",
	"Obeskattade reserver_2021",
	"Obeskattade reserver_2022",
	"Omsättning_2012",
	"Omsättning_2013",
	"Omsättning_2014",
	"Omsättning_2015",
	"Omsättning_2016",
	"Omsättning_2017",
	"Omsättning_2018",
	"Omsättning_2019",
	"Omsättning_2020",
	"Omsättning_2021",
	"Omsättning_2022",
	"Omsättningstillgångar_2012",
	"Omsättningstillgångar_2013",
	"Omsättningstillgångar_2014",
	"Omsättningstillgångar_2015",
	"Omsättningstillgångar_2016",
	"Omsättningstillgångar_2017",
	"Omsättningstillgångar_2018",
	"Omsättningstillgångar_2019",
	"Omsättningstillgångar_2020",
	"Omsättningstillgångar_2021",
	"Omsättningstillgångar_2022",
	"Övrig omsättning_2012",
	"Övrig omsättning_2013",
	"Övrig omsättning_2014",
	"Övrig omsättning_2015",
	"Övrig omsättning_2016",
	"Övrig omsättning_2017",
	"Övrig omsättning_2018",
	"Övrig omsättning_2019",
	"Övrig omsättning_2020",
	"Övrig omsättning_2021",
	"Övrig omsättning_2022",
	"Personalkostnader per anställd (tkr)_2012",
	"Personalkostnader per anställd (tkr)_2013",
	"Personalkostnader per anställd (tkr)_2014",
	"Personalkostnader per anställd (tkr)_2015",
	"Personalkostnader per anställd (tkr)_2016",
	"Personalkostnader per anställd (tkr)_2017",
	"Personalkostnader per anställd (tkr)_2018",
	"Personalkostnader per anställd (tkr)_2019",
	"Personalkostnader per anställd (tkr)_2020",
	"Personalkostnader per anställd (tkr)_2021",
	"Personalkostnader per anställd (tkr)_2022",
	"Resultat efter finansnetto_2012",
	"Resultat efter finansnetto_2013",
	"Resultat efter finansnetto_2014",
	"Resultat efter finansnetto_2015",
	"Resultat efter finansnetto_2016",
	"Resultat efter finansnetto_2017",
	"Resultat efter finansnetto_2018",
	"Resultat efter finansnetto_2019",
	"Resultat efter finansnetto_2020",
	"Resultat efter finansnetto_2021",
	"Resultat efter finansnetto_2022",
	"Rörelsekapital/omsättning_2012",
	"Rörelsekapital/omsättning_2013",
	"Rörelsekapital/omsättning_2014",
	"Rörelsekapital/omsättning_2015",
	"Rörelsekapital/omsättning_2016",
	"Rörelsekapital/omsättning_2017",
	"Rörelsekapital/omsättning_2018",
	"Rörelsekapital/omsättning_2019",
	"Rörelsekapital/omsättning_2020",
	"Rörelsekapital/omsättning_2021",
	"Rörelsekapital/omsättning_2022",
	"Rörelseresultat (EBIT)_2012",
	"Rörelseresultat (EBIT)_2013",
	"Rörelseresultat (EBIT)_2014",
	"Rörelseresultat (EBIT)_2015",
	"Rörelseresultat (EBIT)_2016",
	"Rörelseresultat (EBIT)_2017",
	"Rörelseresultat (EBIT)_2018",
	"Rörelseresultat (EBIT)_2019",
	"Rörelseresultat (EBIT)_2020",
	"Rörelseresultat (EBIT)_2021",
	"Rörelseresultat (EBIT)_2022",
	"Rörelseresultat (EBITDA)_2012",
	"Rörelseresultat (EBITDA)_2013",
	"Rörelseresultat (EBITDA)_2014",
	"Rörelseresultat (EBITDA)_2015",
	"Rörelseresultat (EBITDA)_2016",
	"Rörelseresultat (EBITDA)_2017",
	"Rörelseresultat (EBITDA)_2018",
	"Rörelseresultat (EBITDA)_2019",
	"Rörelseresultat (EBITDA)_2020",
	"Rörelseresultat (EBITDA)_2021",
	"Rörelseresultat (EBITDA)_2022",
    "Skulder och eget kapital_2012",
    "Skulder och eget kapital_2013",
    "Skulder och eget kapital_2014",
    "Skulder och eget kapital_2015",
    "Skulder och eget kapital_2016",
    "Skulder och eget kapital_2017",
    "Skulder och eget kapital_2018",
    "Skulder och eget kapital_2019",
    "Skulder och eget kapital_2020",
    "Skulder och eget kapital_2021",
    "Skulder och eget kapital_2022",
    "Sociala kostnader_2012",
    "Sociala kostnader_2013",
    "Sociala kostnader_2014",
    "Sociala kostnader_2015",
    "Sociala kostnader_2016",
    "Sociala kostnader_2017",
    "Sociala kostnader_2018",
    "Sociala kostnader_2019",
    "Sociala kostnader_2020",
    "Sociala kostnader_2021",
    "Sociala kostnader_2022",
    "Soliditet_2012",
    "Soliditet_2013",
    "Soliditet_2014",
    "Soliditet_2015",
    "Soliditet_2016",
    "Soliditet_2017",
    "Soliditet_2018",
    "Soliditet_2019",
    "Soliditet_2020",
    "Soliditet_2021",
    "Soliditet_2022",
    "Tecknat ej inbetalt kapital_2012",
    "Tecknat ej inbetalt kapital_2013",
    "Tecknat ej inbetalt kapital_2014",
    "Tecknat ej inbetalt kapital_2015",
    "Tecknat ej inbetalt kapital_2016",
    "Tecknat ej inbetalt kapital_2017",
    "Tecknat ej inbetalt kapital_2018",
    "Tecknat ej inbetalt kapital_2019",
    "Tecknat ej inbetalt kapital_2020",
    "Tecknat ej inbetalt kapital_2021",
    "Tecknat ej inbetalt kapital_2022",
    "Utdelning till aktieägare_2012",
    "Utdelning till aktieägare_2013",
    "Utdelning till aktieägare_2014",
    "Utdelning till aktieägare_2015",
    "Utdelning till aktieägare_2016",
    "Utdelning till aktieägare_2017",
    "Utdelning till aktieägare_2018",
    "Utdelning till aktieägare_2019",
    "Utdelning till aktieägare_2020",
    "Utdelning till aktieägare_2021",
    "Utdelning till aktieägare_2022",
    "Varav resultatlön till övriga anställda_2012",
    "Varav resultatlön till övriga anställda_2013",
    "Varav resultatlön till övriga anställda_2014",
    "Varav resultatlön till övriga anställda_2015",
    "Varav resultatlön till övriga anställda_2016",
    "Varav resultatlön till övriga anställda_2017",
    "Varav resultatlön till övriga anställda_2018",
    "Varav resultatlön till övriga anställda_2019",
    "Varav resultatlön till övriga anställda_2020",
    "Varav resultatlön till övriga anställda_2021",
    "Varav resultatlön till övriga anställda_2022",
    "Varav tantiem till styrelse & VD_2012",
    "Varav tantiem till styrelse & VD_2013",
    "Varav tantiem till styrelse & VD_2014",
    "Varav tantiem till styrelse & VD_2015",
    "Varav tantiem till styrelse & VD_2016",
    "Varav tantiem till styrelse & VD_2017",
    "Varav tantiem till styrelse & VD_2018",
    "Varav tantiem till styrelse & VD_2019",
    "Varav tantiem till styrelse & VD_2020",
    "Varav tantiem till styrelse & VD_2021",
    "Varav tantiem till styrelse & VD_2022",
    "Vinstmarginal_2012",
    "Vinstmarginal_2013",
    "Vinstmarginal_2014",
    "Vinstmarginal_2015",
    "Vinstmarginal_2016",
    "Vinstmarginal_2017",
    "Vinstmarginal_2018",
    "Vinstmarginal_2019",
    "Vinstmarginal_2020",
    "Vinstmarginal_2021",
    "Vinstmarginal_2022"
]

KPIs = [
        "Antal_anställda",
        "Nettoomsättning per anställd (tkr)",
        "Personalkostnader per anställd (tkr)",
        "Rörelseresultat (EBITDA)",
        "Nettoomsättningsförändring",
        "Du Pont-modellen",
        "Vinstmarginal",
        "Bruttovinstmarginal",
        "Rörelsekapital/omsättning",
        "Soliditet",
        "Kassalikviditet"
    ]

URLs = [
        "https://www.allabolag.se/what/ab/xv/OFFENTLIG%20F%C3%96RVALTNING%20&%20SAMH%C3%84LLE/xv/BRANSCH-,%20ARBETSGIVAR-%20&%20YRKESORG./xv/RESEBYR%C3%85%20&%20TURISM/xv/AVLOPP,%20AVFALL,%20EL%20&%20VATTEN/xv/LIVSMEDELSFRAMST%C3%84LLNING/xb/AB",
        "https://www.allabolag.se/what/ab/xv/UTHYRNING%20&%20LEASING/xb/AB",
        "https://www.allabolag.se/what/ab/xv/MOTORFORDONSHANDEL/xb/AB",
        "https://www.allabolag.se/what/ab/xv/H%C3%85R%20&%20SK%C3%96NHETSV%C3%85RD/xb/AB",
        "https://www.allabolag.se/what/ab/xv/BEMANNING%20&%20ARBETSF%C3%96RMEDLING/xb/AB",
        "https://www.allabolag.se/what/ab/xv/MEDIA/xb/AB/xe/1",
        "https://www.allabolag.se/what/ab/xv/MEDIA/xb/AB/xe/2/xe/3/xe/4/xe/5/xe/6/xe/7/xe/8/xe/9",
        "https://www.allabolag.se/what/ab/xv/JORDBRUK,%20SKOGSBRUK,%20JAKT%20&%20FISKE/xb/AB/xe/1/xe/2",
        "https://www.allabolag.se/what/ab/xv/JORDBRUK,%20SKOGSBRUK,%20JAKT%20&%20FISKE/xb/AB/xe/9/xe/8/xe/7/xe/6/xe/5/xe/4/xe/3",
        "https://www.allabolag.se/what/ab/xv/REPARATION%20&%20INSTALLATION/xb/AB/xe/1",
        "https://www.allabolag.se/what/ab/xv/REPARATION%20&%20INSTALLATION/xb/AB/xe/2/xe/3/xe/4/xe/8/xe/9/xe/7/xe/6",
        "https://www.allabolag.se/what/ab/xv/REKLAM,%20PR%20&%20MARKNADSUNDERS%C3%96KNING/xb/AB/xe/1",
        "https://www.allabolag.se/what/ab/xv/REKLAM,%20PR%20&%20MARKNADSUNDERS%C3%96KNING/xb/AB/xe/9/xe/8/xe/7/xe/6/xe/5/xe/4/xe/3/xe/2",
        "https://www.allabolag.se/what/ab/xv/KULTUR,%20N%C3%96JE%20&%20FRITID/xb/AB/xe/9/xe/8/xe/7/xe/6/xe/5/xe/4/xe/3/xe/2",
        "https://www.allabolag.se/what/ab/xv/KULTUR,%20N%C3%96JE%20&%20FRITID/xb/AB/xe/1",
        "https://www.allabolag.se/what/ab/xv/UTBILDNING,%20FORSKNING%20&%20UTVECKLING/xb/AB/xe/1/xe/3/xe/4",
        "https://www.allabolag.se/what/ab/xv/UTBILDNING,%20FORSKNING%20&%20UTVECKLING/xb/AB/xe/2/xe/5/xe/6/xe/7/xe/8/xe/9",
        "https://www.allabolag.se/what/ab/xv/TRANSPORT%20&%20MAGASINERING/xb/AB/xe/1/xe/3/xe/4/xe/5/xe/6/xe/7/xe/8/xe/9",
        "https://www.allabolag.se/what/ab/xv/TRANSPORT%20&%20MAGASINERING/xb/AB/xe/2",
        "https://www.allabolag.se/what/ab/xv/TEKNISK%20KONSULTVERKSAMHET/xb/AB/xe/1",
        "https://www.allabolag.se/what/ab/xv/TEKNISK%20KONSULTVERKSAMHET/xl/1/xl/14/xl/12/xl/5/xl/13/xl/3/xl/6/xb/AB/xe/2",
        "https://www.allabolag.se/what/ab/xv/TEKNISK%20KONSULTVERKSAMHET/xl/6/xl/20/xl/17/xl/4/xl/24/xl/18/xl/21/xl/19/xl/25/xl/8/xl/22/xl/7/xl/23/xl/10/xl/9/xb/AB/xe/2",
        "https://www.allabolag.se/what/ab/xv/TEKNISK%20KONSULTVERKSAMHET/xl/6/xl/20/xl/17/xl/4/xl/24/xl/18/xl/21/xl/19/xl/25/xl/8/xl/22/xl/7/xl/23/xl/10/xl/9/xl/1/xl/3/xl/13/xl/5/xl/12/xl/14/xb/AB/xe/3/xe/4/xe/5/xe/6/xe/7/xe/8/xe/9",
        "https://www.allabolag.se/what/ab/xv/HOTELL%20&%20RESTAURANG/xl/1/xl/23/xl/10/xl/7/xb/AB",
        "https://www.allabolag.se/what/ab/xv/HOTELL%20&%20RESTAURANG/xl/9/xl/22/xl/8/xl/25/xl/21/xl/18/xl/24/xl/4/xl/17/xl/20/xl/6/xl/13/xl/5/xb/AB",
        "https://www.allabolag.se/what/ab/xv/HOTELL%20&%20RESTAURANG/xl/12/xl/3/xl/19/xl/14/xb/AB",
        "https://www.allabolag.se/what/ab/xv/H%C3%84LSA%20&%20SJUKV%C3%85RD/xl/1/xb/AB",
        "https://www.allabolag.se/what/ab/xv/H%C3%84LSA%20&%20SJUKV%C3%85RD/xl/14/xl/12/xl/5/xb/AB",
        "https://www.allabolag.se/what/ab/xfv/H%C3%84LSA%20&%20SJUKV%C3%85RD/xl/13/xl/3/xl/6/xl/20/xl/17/xl/4/xl/24/xl/18/xl/21/xl/19/xl/25/xl/8/xl/22/xl/7/xl/23/xl/10/xl/9/xb/AB",
        "https://www.allabolag.se/what/ab/xv/TILLVERKNING%20&%20INDUSTRI/xb/AB/xr/-1200",
        "https://www.allabolag.se/what/ab/xv/TILLVERKNING%20&%20INDUSTRI/xb/AB/xr/1200-50000",
        "https://www.allabolag.se/what/ab/xv/TILLVERKNING%20&%20INDUSTRI/xb/AB/xr/50001-5000000",
        "https://www.allabolag.se/what/ab/xv/BANK,%20FINANS%20&%20F%C3%96RS%C3%84KRING/xb/AB/xr/1-5000",
        "https://www.allabolag.se/what/ab/xv/BANK,%20FINANS%20&%20F%C3%96RS%C3%84KRING/xb/AB/xr/5001-50000000",
        "https://www.allabolag.se/what/ab/xv/DETALJHANDEL/xb/AB/xr/1-500",
        "https://www.allabolag.se/what/ab/xv/DETALJHANDEL/xb/AB/xr/501-3500",
        "https://www.allabolag.se/what/ab/xv/DETALJHANDEL/xb/AB/xr/3501-30000",
        "https://www.allabolag.se/what/ab/xv/DETALJHANDEL/xb/AB/xr/30001-10000000",
        "https://www.allabolag.se/what/ab/xv/DATA,%20IT%20&%20TELEKOMMUNIKATION/xb/AB/xr/1-500",
        "https://www.allabolag.se/what/ab/xv/DATA,%20IT%20&%20TELEKOMMUNIKATION/xb/AB/xr/501-1500",
        "https://www.allabolag.se/what/ab/xv/DATA,%20IT%20&%20TELEKOMMUNIKATION/xb/AB/xr/1501-10000",
        "https://www.allabolag.se/what/ab/xv/DATA,%20IT%20&%20TELEKOMMUNIKATION/xb/AB/xr/10001-50000000",
        "https://www.allabolag.se/what/ab/xv/PARTIHANDEL/xb/AB/xr/1-500",
        "https://www.allabolag.se/what/ab/xv/PARTIHANDEL/xb/AB/xr/501-5000",
        "https://www.allabolag.se/what/ab/xv/PARTIHANDEL/xb/AB/xr/5001-50000",
        "https://www.allabolag.se/what/ab/xv/PARTIHANDEL/xb/AB/xr/50001-5000000",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/1-220",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/221-500",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/501-900",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/901-1600",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/1601-3000",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/3001-8000",
        "https://www.allabolag.se/what/ab/xv/FASTIGHETSVERKSAMHET/xb/AB/xr/8001-500000",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/1-50",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/51-200",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/201-400",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/401-800",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/801-1200",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/1201-2000",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/2001-9000",
        "https://www.allabolag.se/what/ab/xv/JURIDIK,%20EKONOMI%20&%20KONSULTTJ%C3%84NSTER/xb/AB/xr/9001-500000",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/1-150",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/151-500",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/501-900",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/901-1400",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/1401-2000",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/2001-3000",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/3001-5000",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/5001-10000",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/10001-50000",
        "https://www.allabolag.se/what/ab/xv/BYGG-,%20DESIGN-%20&%20INREDNINGSVERKSAMHET/xb/AB/xr/50001-50000000"
      ]




# MAIN

Log("**************** BEGIN ****************", False)

for URL in URLs:
    Data = pd.DataFrame()
    Log("Processing " + URL, True)
    Unquoted_URL = urllib.parse.unquote(URL).split("/")
    # First, determine how many result pages exist for the given URL.
    # The following steps retrieve this number:
    Code_with_number_of_results = str(BeautifulSoup(requests.get(URL).content, 'html.parser').find('div', class_="page search-results"))
    Number_of_results_approx_position = Code_with_number_of_results.find('"per_page":20,"prev_page_url":null,"to":20,"total":')
    Number_of_pages = math.ceil(int(Code_with_number_of_results[Number_of_results_approx_position + 51:].split("}")[0]) / 20)
    # Once the number of pages is calculated, iterate through them to find company names.
    # They will be used later to search for specific data for each company (in a dedicated URL).
    for Page in range(1, Number_of_pages):
    # for Page in range(1, 2):
        print("Getting data from page " + str(Page))
        try:
            Soup = BeautifulSoup(requests.get(URL + "?page=" + str(Page + 1)).content, 'html.parser')
            Page_content = str(Soup.find('div', class_="page search-results"))
        except:
            # If the first attempt to access fails, make a second attempt after a short wait.
            try:
                # wait_with_random_delay(Delay)
                print("First attempt to read page " + str(Page) + " failed. Retrying...")
                Soup = BeautifulSoup(requests.get(URL + "?page=" + str(Page + 1)).content, 'html.parser')
                Page_content = str(Soup.find('div', class_="page search-results"))
                print("Second attempt succeeded!")
            except Exception as e:
                Page_content = ""
                Log(f"Error reading page {Page}: {e}", True)
                Error_count = Error_count + 1
        Company_page_list = Retrieve_company_page_list(Page_content, Page)
        Data = pd.concat([Data, Company_page_list], axis=0)
    # Remove columns of data from the web page that are not interesting
    try:
        Data = Data.drop(['ftgtyp', 'bolpres', 'companyPresentation', 'score', 'remarks', 'hasremarks', 'relatedmetadata', 'hasrelatedmetadata', 'status'], axis=1)
    except:
        Log("Column drop error at " + Unquoted_URL[6] + "/.../" + Unquoted_URL[-1], True)
        Error_count = Error_count + 1

    # For each company in Data, visit the dedicated web page to integrate its data
    Company_dataset = pd.DataFrame(columns=Company_dataset_columns)
    Company_dataset = pd.concat([Company_dataset, Data], join="outer", ignore_index=True)
    Company_dataset.index = Company_dataset["jurnamn"]
    Company_dataset.drop(["jurnamn"], axis=1, inplace=True)
    # Now, Company_dataset contains the basic data of a page of companies, with several columns related to exercises that need to be filled
    for Company in Data.iterrows():
        Count = Count + 1
        Name = Company[1]["jurnamn"].replace("&amp;", "&")
        Company_DF = pd.DataFrame(Company[1]).transpose()
        Company_DF.index = [Name]
        Company_DF.drop(["jurnamn"], axis=1, inplace=True)
        Company_activity_data = Get_activity_data(Company, Delay)
        Company_activity_data.set_index(Company_DF.index)
        Company_activity_data.index = [Name]
        # Assign the same index to Company_closure_data as the row with the same orgnr in Company_dataset:
        Company_closure_data = Get_closure_data(Company, Delay)
        Company_closure_data.columns = [Name]
        # The content of Company_closure_data integrates Company_dataset, making it complete. To do this, you need to find the row
        # of Company_dataset corresponding to it. This can be done, for example, by looking for the one that contains the same "orgnr".
        try:
            print(str(Count) + "\t\t" + Name)
            # This makes it easy to replace the content of Company_dataset with that of Company_closure_data and Company_activity_data:
            Company_dataset.loc[Company_DF.index, :] = pd.concat([Company_DF, Company_activity_data, Company_closure_data.transpose()], axis=1)
        except:
            Log("Error merging data for " + Name, True)
            Error_count = Error_count + 1
    # Replace "&amp;", which may interfere with CSV generation
    Company_dataset.replace({"&amp;": "&"}, regex=True, inplace=True)
    
    # Now that Final_dataset contains all the information for each company, it's time to write the result to a file
    try:
        Filename = "Company_data " + Unquoted_URL[6] + " - " + Unquoted_URL[-1] + " - " + str(datetime.date.today().year) + str(
            datetime.date.today().month) + str(datetime.date.today().day) + "-" + str(
            datetime.datetime.today().hour) + str(datetime.datetime.today().minute) + str(
            datetime.datetime.today().second) + ".csv"
        Company_dataset.to_csv(Raw_data_folder + Filename, sep=";", encoding="utf-16")
        Log(Filename + " successfully written.", True)
    except:
        Log("CSV write error.", True)
        Error_count = Error_count + 1
Log("Error count = " + str(Error_count) + "\nTotal companies analyzed: " + str(Count), True)
Log("**************** END ****************\n\n", False)
subprocess.run(["shutdown", "/h"])

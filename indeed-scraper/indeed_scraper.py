"""
This python file covers the scraping of indeed jobs data for a number of positions and countries, preforming
some simple operation on the collected data, then storing them on a google cloud database
"""

# importing the needed libraries
import pandas as pd
import requests
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup as bs
import warnings
import pytest
import pg8000
import sqlalchemy
from google.cloud.sql.connector import Connector
import os
warnings.simplefilter(action='ignore', category=FutureWarning)


# selecting which countries to collect the data from and their alpha-2 codes
# also selecting the positions that we want to collect the data about we
countries = {'United Arab Emirates':'ae', 'Egypt':'eg', 'Canada': 'ca', 'Australia': 'au', 'United Kingdom': 'gb', 
             'United States':'www', 'Germany': 'de', 'France': 'fr', 'Italy': 'it', 'India' :'in'}
jobs = ['Data Analyst', 'Data Engineer', 'Data Scientist', 'Machine Learning Engineer', 'Software Engineer', 
        'DevOps Engineer', 'Mechanical Engineer', 'Electrical Engineer', 'Mechatronics Engineer', 'Robotics Engineer', 
        'Civil Engineer', 'Quality Engineer', 'Biomedical Engineer', 'Sales Engineer']


# creating a list with each country, position and initial url
search_urls_list = []
temp = 'https://{}.indeed.com/jobs?q={}&sort=date'   # the raw url which is used to generate all the results urls
for country, key in countries.items():
    for position in jobs:
        url = temp.format(key, position.lower())
        if url not in search_urls_list:
            search_urls_list.append([country, position, [url]])  # appending each country, position and url to the list


# then we create some functions to help scrape and clean the data


def all_jobs_pages(url):
    """
    all_jobs_pages function is for getting all the pages urls for a certain search
    :param url: the first indeed URL link
    :return: list of all available indeed pages URLs
    """
    pages_list = []
    # we start by looking for the 100's page(indeed only offers 67 pages per query but just to make sure)
    response = requests.get(url+'&start=1000')
    soup = bs(response.text, 'html.parser')
    # then we find the text for the last page which will be the number of pages the query has
    try:
        last_page = soup.find('ul', 'pagination-list').find_all('li')
        last_page = last_page[-1]
        last = int(last_page.text)
        # then we generate all the available urls and add them to the output list
        for i in range(1, last+1):
            page = url+'&start={}'.format(i*10)
            pages_list.append(page)
    except AttributeError:
        pass   
    return pages_list


def get_jobs_data(url):
    """
    get_jobs_data function gets all job posting properties (title, location, date, ect..) for the given url
    :param url: the indeed URL link that we want to scrape the data from
    :return: list of all available indeed pages URLs
    """
    jobs_data = []     # an empty list that will hold nested lists for jobs data
    response = requests.get(url)
    soup = bs(response.text, 'html.parser')
    url_core = url.split('com')[0]+'com'    # the main url to later be used in creating each job url
    job_cards = soup.find_all('div', 'job_seen_beacon')      # this is the tag that holds each job card
    for card in job_cards:
        try:
            job_title = card.find('td', 'resultContent')     # first we get the title
            job_title = job_title.div.h2.a.text
            job_location = card.find('div', 'companyLocation').text    # then the location of the job
            company = card.find('span', 'companyName').text            # then the company with the posting
            post_date = card.find('span', 'date').text.replace('Posted','')   # then the date
            job_url_ext = card.table.tbody.tr.td.div.h2.a.get('href')
            job_url = url_core + job_url_ext                 # we scrape the job url then add it to the main url
            description_response = requests.get(job_url)     # then we scrape the job url page for full description
            description_soup = bs(description_response.text, 'html.parser')
            job_description = description_soup.find('div', 'jobsearch-jobDescriptionText').text.replace('\n',' ')
            # then we append the collected data as a list to the output list
            jobs_data.append([job_title, job_location, company, post_date, job_description, job_url])
        except AttributeError:
            pass
        except ConnectionError:
            pass
    return jobs_data


# we start the scraping process by adding all available pages urls to the search_urls_list that we created
for i in range(len(search_urls_list)):
    search_urls_list[i][2] = search_urls_list[i][2] + all_jobs_pages(search_urls_list[i][2][0])


# we create the dataframe that will hold all the collected data
all_jobs_df = pd.DataFrame(columns=['Position', 'Country', 'Job Title', 'Job Location', 'Company', 'Data Posted', 
                                    'Description', 'Job URL'])

# then we start the actual scraping process
for i in range(search_urls_list):           # iterating through the search_urls_list to get the to the urls list
    country = search_urls_list[i][0]
    position = search_urls_list[i][1]
    for n in range(len(search_urls_list[i][2])):    # iterate through the urls list
        page_data = get_jobs_data(search_urls_list[i][2][n])   # getting all jobs data in the page
        for job in page_data:
            # adding each result to the dataframe
            all_jobs_df = all_jobs_df.append({'Position': position, 'Country': country, 'Job Title': job[0], 
                                              'Job Location': job[1], 'Company': job[2], 'Data Posted': job[3], 
                                              'Description': job[4], 'Job URL': job[5]}, ignore_index=True)


# now we start cleaning the data
dates = []
today_ = date.today()   # get today's date
for i in range(len(all_jobs_df)):
    # we need to check for numeric values in the post date as it can be (today or posted just now)
    if any(n.isdigit() for n in all_jobs_df.loc[i]['Data Posted']):
        # then we extract the numbers from the data to get the post date in a usable way
        x = int(''.join(n for n in all_jobs_df.loc[i]['Data Posted'] if n.isdigit()))
        if x > 30:
            x = 30
        else:
            pass
        date_ = today_ - timedelta(days = x)   # we sub the number of days from today's date to get the post date
        date_ = date_.strftime("%d/%m/%Y")
        dates.append(date_)
    else:
        date_ = today_.strftime("%d/%m/%Y")    # if the data doesn't have a numeric value it was probably posted today
        dates.append(date_)
# we add the new dates to the dataframe and drop the old date format
all_jobs_df['Date Posted'] = dates
all_jobs_df.drop('Data Posted', axis=1, inplace=True)


# we create a list of all the available skills to check if any of them are in the job description
tech_skills = [['Python', 'SQL', 'Excel', ' R ', 'MATLAB', 'Power BI', 'Tableau', 'NoSQL', 'Probability', 'Statistics', 
                'Algorithms', 'Linear Algebra', 'Java ', 'C++', 'Julia', 'Scala', 'TensorFlow', 'Database Management', 
                'Deep Learning', 'Cloud Computing', 'AWS', 'Azure', 'Google Cloud', ' EDA ', 'IBM Cloud', 'Hadoop', 
                'Apache Spark', 'Keras', 'PyTorch', 'Big Data', 'Storm', 'Flink', ' Hive ', 'Kafka', 'Hevo', 'Matillion', 
                'Talend', 'Flume', 'Sqoop', 'Mahout', 'KNIME', 'Rapid Miner', 'Weka', 'Informatica PowerCenter', ' Glue', 
                'Stitch', 'Kinesis', 'Redis Cache', 'SAS', 'SPSS', 'OpenStack', 'Openshift', 'Qlik', 'Tibco Spotfire', 
                'Scikit', 'Theano', 'Catalyst', 'XGBoost', 'LightGBM', 'CatBoost', 'Fast.ai', 'Ignite', 'Gensim','Caffe',
                'JavaScript', 'HTML', 'CSS', ' C ', 'C#', 'Bash', 'Shell', 'PHP', 'TypeScript', 'Ruby', ' Go ', 'Linux', 
                'Puppet', 'Chef', 'Ansible', ' Git', 'Nagios', 'Zabbix', 'Splunk', 'Agile'], 
               ['Solidworks', 'CATIA', 'Fusion 360', ' CAD ', ' FEA ', 'CAM', 'Inventor','VBA', 'Ansys', 'Six Sigma',
                'Lean', 'Excel', 'Circuit design', 'PLC', 'Automation', 'SCADA', 'HDL', 'Java', ' C ', 'C++', 'Python',
                'MATLAB', 'Simulink', 'HMI', 'CNC', 'Eagle', 'OrCAD', 'Altium', 'Octave' ,'ROS ', 'Gazebo', 'Fanuc', 
                'Linux', 'Opencv', 'PID']]
soft_skills = ['Critical Thinking', 'Communication', 'Collaboration', 'Presentation Skills', 'Business knowledge', 
               'Problem Solving', 'Curiosity', 'Research', 'Domain knowledge', 'Attention to detail', 'Project management',
               'Teamwork', 'Interpersonal skills', 'Active learning', 'Decision Making']


# we check if any of the skills are in the job description
tech_list = []
soft_list = []
for i in range(len(all_jobs_df)):    # we iterate through the list to get each description
    tech_found = []
    soft_found = []
    description = all_jobs_df.loc[i]['Description']       # we get the job description
    if all_jobs_df.loc[i]['Position'] in jobs[:5]:        # checking the job to choose an appropriate skill list
        pos = 0
    else:
        pos = 1
    for t_skill in tech_skills[pos]:
        if t_skill.lower() in description.lower():
            tech_found.append(t_skill)
    for s_skill in soft_skills:
        if s_skill.lower() in description.lower():
            soft_found.append(s_skill)
    # add the list of skills found in the description to the final list
    tech_list.append(tech_found)
    soft_list.append(soft_found)


all_jobs_df['Technical Skills'] = tech_list   # create new columns for the skills
all_jobs_df['Soft Skills'] = soft_list


# now we create a table in the Google cloud database instance and insert the collected data to it
# first we add the key of the IAM authorization as GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"path_to_IAM_authorization_key.json"


# this part with completed following the instructions in Cloud SQL Connector for Python Drivers
# found at https://github.com/GoogleCloudPlatform/cloud-sql-python-connector
connector = Connector()

INSTANCE_CONNECTION_NAME = f"{'indeed-project'}:{'europe-west8'}:{'indeed-project-database'}"


def getconn():
    with Connector() as connector:
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user="IAM_Service_account",
            db="indeed_data",
            enable_iam_auth=True
        )
    return conn


# create connection pool
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)



# connect to connection pool
with pool.connect() as db_conn:
    # create the table for the jobs
    db_conn.execute(
      "CREATE TABLE IF NOT EXISTS Indeed_jobs_data "
      "(id SERIAL NOT NULL, position VARCHAR(255) NOT NULL, "
      "country VARCHAR(255) NOT NULL, job_title VARCHAR NOT NULL, "
      "job_location VARCHAR NOT NULL, company VARCHAR NOT NULL, "
      "job_description VARCHAR NOT NULL, job_url VARCHAR NOT NULL, "
      "date_posted VARCHAR(255) , technical_skills TEXT [], soft_skills TEXT [], "
      "PRIMARY KEY (id));"
    )


def sql_list_insert(skills_list):
    """
    sql_list_insert function creates a string that can be used to insert lists into PostgreSQL database
    :param skills_list: a list that will be converted into a string
    :return: a string to be used in inserting lists to the database
    """
    x = "{"
    for i in skills_list:
        if i == skills_list[-1]:
            x = x+'"{}"'.format(i)
        else:
            x = x+'"{}",'.format(i)
    x = x+"}"
    return x


# now we insert the data frame values to the database
with pool.connect() as db_conn: 
    # insert data into Indeed_jobs_data table
    insert_stmt = sqlalchemy.text(
      "INSERT INTO Indeed_jobs_data (position, country, job_title, job_location, company, "
      "job_description, job_url, date_posted, technical_skills, soft_skills) "
      "VALUES (:position, :country, :title, :location, :company, "
      ":description, :url, :date, :t_skills, :s_skills) ",
    )
    for i in range(len(all_jobs_df)):
        job = all_jobs_df.iloc[i]
        db_conn.execute(insert_stmt, position=job['Position'], country=job['Country'], title=job['Job Title'],
                        location=job['Job Location'], company=job['Company'], description=job['Description'],
                        url=job['Job URL'], date=job['Date Posted'], t_skills=sql_list_insert(job['Technical Skills']),
                        s_skills=sql_list_insert(job['Soft Skills']))
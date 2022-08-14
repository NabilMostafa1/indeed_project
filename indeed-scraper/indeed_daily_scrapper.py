"""
This python file covers the daily scraping of indeed jobs data for a number of positions and countries.
the script is uploaded to heroku and schduled to run daily at 8:00 PM UTC,
then sends a confirmation mail with the scraping results
"""

"""
please note: initialy i was using google cloud platform sql database as i thought it was free
or had a free tier but then they started charging me money so i decided to swich to an oracle free tier database
and as it turns out it's much easier to deal with than gcp database.
"""

# importing the needed libraries
import pandas as pd
import requests
from datetime import date
from bs4 import BeautifulSoup as bs
import warnings
import oracledb
import smtplib
from email.message import EmailMessage
import pyuser_agent
import cloudscraper
warnings.simplefilter(action='ignore', category=FutureWarning)


# selecting which countries to collect the data from and their alpha-2 codes
# also selecting the positions that we want to collect the data about we
countries = {'United Arab Emirates': 'ae', 'Egypt': 'eg', 'Canada': 'ca', 'Australia': 'au', 'United Kingdom': 'gb',
             'United States': 'www', 'Germany': 'de', 'France': 'fr', 'Italy': 'it', 'India': 'in'}
jobs = ['Data Analyst', 'Data Engineer', 'Data Scientist', 'Machine Learning Engineer', 'Software Engineer',
        'DevOps Engineer', 'Mechanical Engineer', 'Electrical Engineer', 'Mechatronics Engineer', 'Robotics Engineer',
        'Civil Engineer', 'Quality Engineer', 'Biomedical Engineer', 'Sales Engineer']


# during my initial scraping i was using request liberary and it worked like a charm 
# but after a while i was facing a cloudflare security check which  couldn't get through 
# that's why i have used cloudscraper instead
user_agent = pyuser_agent.UA()
user_agent_str = str(user_agent.chrome)
header = {'User-Agent': user_agent_str}
scraper = cloudscraper.create_scraper()


# creating a list with each country, position and initial url
search_urls_list = []
temp = 'https://{}.indeed.com/jobs?q={}&sort=date&fromage=1'
for country, key in countries.items():
    for position in jobs:
        url = temp.format(key, position.lower())
        if url not in search_urls_list:
            search_urls_list.append([country, position, [url]])


# i used the same functions used in the initial scraping stage

def all_jobs_pages(url):
    """
    all_jobs_pages function is for getting all the pages urls for a certain search
    :param url: the first indeed URL link
    :return: list of all available indeed pages URLs
    """
    pages_list = []
    response = scraper.get(url+'&start=1000', headers=header)
    soup = bs(response.text, 'html.parser')
    try:
        last_page = soup.find('ul', 'pagination-list').find_all('li')
        last_page = last_page[-1]
        last = int(last_page.text)
        for i in range(1, last+1):
            page = url + '&start={}'.format(i * 10)
            pages_list.append(page)
    except AttributeError:
        pass
    return pages_list


# get the description for all jobs in the page
def get_jobs_data(url):
    """
    get_jobs_data function gets all job posting properties (title, location, date, ect..) for the given url
    :param url: the indeed URL link that we want to scrape the data from
    :return: list of all available indeed pages URLs
    """
    jobs_data = []
    response = scraper.get(url, headers=header)
    soup = bs(response.text, 'html.parser')
    url_core = url.split('com')[0]+'com'
    job_cards = soup.find_all('div', 'job_seen_beacon')
    for card in job_cards:
        try:
            job_title = card.find('td', 'resultContent')
            job_title = job_title.div.h2.a.text
            job_location = card.find('div', 'companyLocation').text
            company = card.find('span', 'companyName').text
            post_date = card.find('span', 'date').text.replace('Posted', '')
            job_url_ext = card.table.tbody.tr.td.div.h2.a.get('href')
            job_url = url_core + job_url_ext
            description_response = scraper.get(job_url, headers=header)
            description_soup = bs(description_response.text, 'html.parser')
            job_description = description_soup.find('div', 'jobsearch-jobDescriptionText').text.replace('\n',' ')
            jobs_data.append([job_title, job_location, company, post_date, job_description, job_url])
        except AttributeError:
            continue
        except ConnectionError:
            continue
    return jobs_data



# added a function to extract the skills from the jobs description
def skills_extract(position, description):
    """
    skills_extract function gives a list of all the skills in the job description matching the list of givin skills 
    :param position: the position as str to match against the skills list
           description: the job description as str
    :return: list of skills found
    """
    global tech_skills, soft_skills
    tech_found = []
    soft_found = []
    description = description.lower()
    if position in jobs[:5]:
        pos=0
    else:
        pos=1
    for t_skill in tech_skills[pos]:
        if t_skill.lower() in description:
            tech_found.append(t_skill)
    for s_skill in soft_skills:
        if s_skill.lower() in description:
            soft_found.append(s_skill)
    return tech_found, soft_found


def upload_to_database(df):
    """
    upload_to_database function creates a connection with the oracle database and inserts the values into the database
    :param df: the newly collected dataframe that will be added to the database
    :return: none
    """
    con = oracledb.connect(user="admin", password=psw, dsn=mydsn,config_dir=config_dirc,
                           wallet_location=wallet_loc, wallet_password=psw)
    cur = con.cursor()
    insert_stmt = (
          "INSERT INTO indeed_jobs_data (position, country, job_title, job_location, company, "
          "job_description, job_url, date_posted, technical_skills, soft_skills) "
          "VALUES (:position, :country, :title, :location, :company, "
          ":description, :url, :posted, :t_skills, :s_skills)"
        )
    for i in range(len(df)):
        job = df.iloc[i]
        try:
            cur.execute(insert_stmt, [job['Position'], job['Country'], job['Job Title'],job['Job Location'], job['Company'],
                                job['Description'], job['Job URL'], job['Date Posted'],
                                str(job['Technical Skils']), str(job['Soft Skills'])])
            con.commit()
        except:
            continue
    cur.close()
    con.close()



# we start the scraping process by adding all available pages urls to the search_urls_list that we created
for i in range(len(search_urls_list)):
    search_urls_list[i][2] = search_urls_list[i][2] + all_jobs_pages(search_urls_list[i][2][0])


# we create the dataframe that will hold all the collected data
all_jobs_df = pd.DataFrame(columns=['Position', 'Country', 'Job Title', 'Job Location', 'Company', 'Date Posted',
                                    'Description', 'Job URL', 'Technical Skils', 'Soft Skills'])

# we get today's date
today_ = date.today()
today_ = today_.strftime("%d/%m/%Y")
# we create a list of all the available skills to check if any of them are in the job description
tech_skills = [['Python', 'SQL', 'Excel', ' R ', 'MATLAB', 'Power BI', 'Tableau', 'NoSQL', 'Probability', 'Statistics',
                'Algorithms', 'Linear Algebra', 'Java ', 'C++', 'Julia', ' Scala ', 'TensorFlow', 'Database Management',
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


# then we start the actual scraping process
for i in range(search_urls_list):           # iterating through the search_urls_list to get the to the urls list
    country = search_urls_list[i][0]
    position = search_urls_list[i][1]
    for n in range(len(search_urls_list[i][2])):    # iterate through the urls list
        page_data = get_jobs_data(search_urls_list[i][2][n])   # getting all jobs data in the page
        for job in page_data:               # iterating through each of the collected jobs 
            # creating the skills list
            tech_skl, soft_skl = skills_extract(position, job[4])
            # adding each result to the dataframe
            all_jobs_df = all_jobs_df.append({'Position': position, 'Country': country, 'Job Title': job[0],
                                              'Job Location': job[1], 'Company': job[2], 'Date Posted': today_,
                                              'Description': job[4], 'Job URL': job[5], 'Technical Skils': tech_skl,
                                              'Soft Skills': soft_skl}, ignore_index=True)


init_df_length = len(all_jobs_df)


# dropping the duplicates in the df
all_jobs_df.drop_duplicates(['Position', 'Country', 'Job Title', 'Job Location',
                             'Company', 'Date Posted', 'Description', 'Job URL'], inplace=True)

duplicates = len(all_jobs_df) - init_df_length



mydsn = 'connection-type(found_in_tnsnames)'
config_dirc = "path-to-sqlnet.ora-file"
wallet_loc = "path-to-ewallet.pem-file"
psw = 'mydbpassword'


# getting the initial count of the database table
con = oracledb.connect(user="admin", password=psw, dsn=mydsn,
                       config_dir=config_dirc, wallet_location=wallet_loc,
                       wallet_password=psw)
cur = con.cursor()
initial_count = cur.execute('SELECT COUNT(*) FROM indeed_jobs_data').fetchall()
initial_count = initial_count[0][0]
cur.close()
con.close()


# uploading the collected data to the orace database
upload_to_database(all_jobs_df)


# getting the final count of the database table after inserting the new values to see 
# if any errors ocurred during adding the new values
con = oracledb.connect(user="admin", password=psw, dsn=mydsn,config_dir=config_dirc,
                       wallet_location=wallet_loc, wallet_password=psw)
cur = con.cursor()
final_count = cur.execute('SELECT COUNT(*) FROM indeed_jobs_data').fetchall()
final_count = final_count[0][0]
cur.close()
con.close()


# sending the confirmation email
Email_Address = 'my-business-mail@gmail.com'
Email_Password = 'mypassword'

msg = EmailMessage()
msg['Subject'] = 'Oracle Database Update Dated {}.'.format(today_)
msg['From'] = Email_Address
msg['To'] = 'my-personal-mail@gmail.com'
msg.set_content("""
                    Updating Data Report Dated {}.\n
                    The Number of Jobs collected is {},\n
                    With a Duplicate Count of {},\n
                    Number of rows in the oracle database before uploading collected Data is {},\n
                    Number of rows in the oracle database after uploading collected Data is {},\n
                    So {} new jobs have been added.\n
                """.format(today_, init_df_length, duplicates, initial_count,
                           final_count, final_count-initial_count))

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login(Email_Address, Email_Password)
    smtp.send_message(msg)


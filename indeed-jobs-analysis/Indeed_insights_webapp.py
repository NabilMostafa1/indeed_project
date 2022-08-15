"""
This python file covers creation of a webapp to visualize and share the collected data and insights
"""

# importing the needed libraries
import pandas as pd
import random
import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
import dash_loading_spinners as dls
import plotly.graph_objects as go
from wordcloud import WordCloud
from io import BytesIO
import base64
import oracledb
import os
from datetime import datetime, date, timedelta
import time


mydsn = 'connection-type(found_in_tnsnames)'
config_dirc = "path-to-sqlnet.ora-file"
wallet_loc = "path-to-ewallet.pem-file"
psw = 'mydbpassword'


# now we create some functions

# this function deals with the oracle CLOB datatype which i have used to store the description 
# and converts it into a long char 
def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_NVARCHAR, arraysize=cursor.arraysize)


# we create a lst of the positions and countries
positions = ['Data Analyst', 'Data Engineer', 'Data Scientist', 'Machine Learning Engineer', 'Software Engineer', 
             'DevOps Engineer', 'Mechanical Engineer', 'Electrical Engineer', 'Mechatronics Engineer', 'Robotics Engineer', 
             'Civil Engineer', 'Quality Engineer', 'Biomedical Engineer', 'Sales Engineer']
countries = ['All Countries', 'United Arab Emirates', 'Canada', 'United States', 'Egypt', 'Australia', 'United Kingdom', 
             'Germany', 'France', 'Italy', 'India']



# a function to get the selected dataframe
def query_selected(position, country):
    """
    query_selected function is for getting the selected data and returning a dataframe
    :param position: the selected profession as a str
           country: the selected country as a str
    :return: the queried dataframe
    """
    # we inialize a connection and create a cursor
    con = oracledb.connect(user="admin", password=psw, dsn=mydsn, config_dir=config_dirc,
                           wallet_location=wallet_loc, wallet_password=psw)
    con.outputtypehandler = output_type_handler
    cur = con.cursor()
    if country == 'All Countries':    # selecting based only on position if all countries is selected
        results = cur.execute("SELECT * FROM indeed_jobs_data WHERE position='{}'".format(position)).fetchall()
    else:                             # selecting based on position and country
        results = cur.execute(
            "SELECT * FROM indeed_jobs_data WHERE position='{}' AND country='{}'".format(position, country)).fetchall()

    df = pd.DataFrame(results)        # saving the query result to a dataframe and
    cur.close()
    con.close()    # closing the connection
    df = df[range(1, 11)]
    # cleaning the dataframe by renaming the columns and droping duplicates          
    df.rename(
        columns={1: 'position', 2: 'country', 3: 'job_title', 4: 'job_location', 5: 'company', 6: 'job_description',
                 7: 'job_url', 8: 'date_posted', 9: 'technical_skills', 10: 'soft_skills'}, inplace=True)
    df.drop_duplicates(['job_title', 'job_location', 'company', 'job_description', 'job_url'], inplace=True)
    # converting the skills into a list as they came as str
    for i in range(len(df)):
        s_skl = []
        t_skl = []
        t = df.iloc[i]['technical_skills'].split("'")
        s = df.iloc[i]['soft_skills'].split("'")

        for ts in t:
            if len(ts) >= 3:
                t_skl.append(ts)
        for ss in s:
            if len(ss) >= 3:
                s_skl.append(ss)
        df.iloc[i]['technical_skills'] = t_skl
        df.iloc[i]['soft_skills'] = s_skl
    # changing the date_posted datatype from object to date
    df['date_posted'] = pd.to_datetime(df['date_posted'], dayfirst=True)

    return df

# a function to get the salaries
def job_salary(position, country):
    """
    job_salary function is for getting the selected salary and url
    :param position: the selected profession as a str
           country: the selected country as a str
    :return: salary: the salary for the selected position and country
             salary_url: the link to the salary url
    """
    con = oracledb.connect(user="admin", password=psw, dsn=mydsn,config_dir=config_dirc,
                           wallet_location=wallet_loc, wallet_password=psw)
    con.outputtypehandler = output_type_handler
    cur = con.cursor()
    if country == 'All Countries':    # if 'All Countries' is selected i return an empty results as i don't have the data for it
        salary = ''
        salary_url = ''
    else:                        
        salary = cur.execute("SELECT salary FROM indeed_salaries WHERE position='{}' AND country='{}'".format(position, country)).fetchall()
        salary = salary[0][0]
        salary_url = cur.execute("SELECT url FROM indeed_salaries WHERE position='{}' AND country='{}'".format(position, country)).fetchall()
        salary_url = salary_url[0][0]
    cur.close()
    con.close()
    return salary, salary_url


# the following functions are ploting functions using plotly


def plot_top10_skills(selected_df, ind):
    """
    plot_top10_skills function is for ploting the top 10 skills
    :param selected_df: the dataframe  queried
           ind: soft_skills or technical_skills as a str
    :return: fig: a plotly figure
    """
    count_dict = {}
    position = selected_df.iloc[0]['position']
    colors = random.sample(range(30), 10)
    # we iterate through the dataframe counting each occurence of a skill and adding them to a dict
    for i in range(len(selected_df)):
        for s in selected_df.iloc[i][ind]:
            if s not in count_dict:
                count_dict[s] = 1
            else:
                count_dict[s] += 1
    # then we sort the dict and select only the top 10 results
    count_dict = dict(sorted(count_dict.items(), key=lambda item: item[1], reverse=True))
    count_dict = {i: count_dict[i] for i in list(count_dict)[:10]}
    # we create a figure object then plot the data into a bar chart
    fig = go.Figure()
    fig.add_trace(go.Bar(x=list(count_dict.keys()), y=list(count_dict.values()),
                         marker=dict(color=colors, colorscale='sunset')))
    fig.update_layout(title={'text': '<b>Top 10 Requested {} Skills for <br>{} Jobs</b>'.format(ind.split('_')[0].title(), position),
                             'x': 0.5, 'xanchor': 'center', 'font_size': 18},
                      xaxis={'title': '<b>{} Skills</b>'.format(ind.split('_')[0].title()),'fixedrange':True},
                      yaxis={'title': '<b>Count</b>','fixedrange':True},
                      paper_bgcolor="rgba(255, 255, 255, 0.5)")
    return fig




def plot_top10_companies(selected_df, ind='company'):
    """
    plot_top10_companies function is for ploting the top 10 company with jobs
    :param selected_df: the dataframe  queried
           ind: company(defult) or location as a str
    :return: fig: a plotly figure
    """
    position = selected_df.iloc[0]['position']
    companies = selected_df[ind].unique()
    colors = random.sample(range(30), 10)
    count_values = {}
    for comp in companies:
        count_values[comp] = selected_df[selected_df[ind] == comp].count()[0]
    count_values = dict(sorted(count_values.items(), key=lambda item: item[1], reverse=True))
    count_values = {i: count_values[i] for i in list(count_values)[:10]}
    fig = go.Figure()
    fig.add_trace(go.Bar(x=list(count_values.keys()), y=list(count_values.values()),
                         marker=dict(color=colors, colorscale='sunset')))
    config = {'displayModeBar': False}
    fig.update_layout(title={'text': '<b>Highest 10 Companies Offering <br> {} Jobs</b>'.format(position),
                             'x': 0.5, 'xanchor': 'center', 'font_size': 18},
                      xaxis={'title': '<b>Companies</b>','fixedrange':True},
                      yaxis={'title': '<b>Count</b>','fixedrange':True},
                      paper_bgcolor="rgba(255, 255, 255, 0.5)")
    return fig



def plot_top10_locations(selected_df, ind='job_location'):
    """
    plot_top10_locations function is for ploting the most popular top 10 locations
    :param selected_df: the dataframe  queried
           ind: location(defult) or company as a str
    :return: fig: a plotly figure
    """
    locations = selected_df[ind].unique()
    position = selected_df.iloc[0]['position']
    count_values = {}
    selected_len = len(selected_df)
    for loc in locations:
        loc_perc = selected_df[selected_df[ind] == loc].count()[0]/selected_len
        count_values[loc] = selected_df[selected_df[ind] == loc].count()[0]
    count_values = dict(sorted(count_values.items(), key=lambda item: item[1], reverse=True))
    count_values = {i: count_values[i] for i in list(count_values)[:10]}
    # we create a figure object just like the 2 previous functions but the only diffrence is that this will be a donut chart
    fig = go.Figure()
    fig.add_trace(go.Pie(labels=list(count_values.keys()), values=list(count_values.values()), hole=0.3))
    fig.update_layout(title={'text': '<b>\t \tLocations With The Highest\t \t<br>{} Jobs Count</b>'.format(position),
                             'x': 0.5, 'xanchor': 'center', 'font_size': 18},
                      legend = {'title':{'text':'<b>Locations</b>', 'font_size':12},
                                'font_size':9, 'orientation':"v", 'yanchor':"middle", 'y':.5, 
                                'xanchor':"right", 'x':2, 'bgcolor': 'rgba(0,0,0,0)'},
                      paper_bgcolor="rgba(255, 255, 255, 0.5)")
    return fig




def description_wordcloud(selected_df):
    """
    description_wordcloud function is for creating a word cloud of the description
    :param selected_df: the dataframe  queried
    :return: a wordcloud image object
    """
    words = ''.join(c for c in selected_df['job_description'])
    wordcloud = WordCloud(width=1100, height=740, background_color="rgba(10, 9, 9, 0)", mode="RGBA").generate(words)
    return wordcloud.to_image()



def plot_dates(selected_df):
    """
    plot_dates function is for ploting number of jobs per day
    :param selected_df: the dataframe  queried
    :return: fig: a plotly figure
    """
    position = selected_df.iloc[0]['position']
    # due to some errors in my date -not that many- and for better visuals i discard any jobs having a post date before my initial scraping
    # or a date after tomorrow's date, and the same filtering is done with the job cards function
    date_ = "15/06/22"
    start_date = datetime.strptime(date_, "%d/%m/%y")
    tomorrow = datetime.now() + timedelta(days=1)
    selected_df = selected_df[(selected_df['date_posted'] <= tomorrow) & (start_date <= selected_df['date_posted'])]
    jobs_count_per_day = selected_df.groupby('date_posted').count()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=jobs_count_per_day.index, y=jobs_count_per_day['position'], mode='lines+markers'))
    fig.update_layout(title={'text': '<b>{} Jobs Count <br>Per Day</b>'.format(position),
                             'x': 0.5, 'xanchor': 'center', 'font_size': 20},
                      xaxis={'title': '<b>Date</b>','fixedrange':True},
                      yaxis={'title': '<b>Count</b>','fixedrange':True},
                      paper_bgcolor="rgba(255, 255, 255, 0.5)")
    return fig



def latest_6_jobs(selected_df):
    """
    latest_6_jobs function is selecting the latest 6 jobs and creating a card for each one
    :param selected_df: the dataframe  queried
    :return: cards: a list of dbc cards for each job
    """
    today_ = date.today()
    end_date = datetime.now()
    latest = selected_df[selected_df['date_posted'] <= end_date].sort_values(by=['date_posted'], ascending=False)
    cards = []
    for i in range(len(latest)):
        if latest.iloc[i]['position'] in latest.iloc[i]['job_title'] and len(cards) < 6:
            skills = latest.iloc[i]['technical_skills']+latest.iloc[i]['soft_skills']
            card = dbc.Col(
                        dbc.Card(
                            dbc.CardBody([
                                html.H5(['',html.A(latest.iloc[i]['job_title'], href=latest.iloc[i]['job_url'],
                                                   className='text-left text-dark font-weight-bold')]),
                                html.P('Company: {}'.format(latest.iloc[i]['company']),
                                       className='text-left text-dark'),
                                html.P('Location: {}'.format(latest.iloc[i]['job_location']),
                                       className='text-left text-dark'),
                                html.P('Date Posted: {}'.format(str(latest.iloc[i]['date_posted']).split(' ')[0]),
                                       className='text-left text-dark'),
                                html.P('Skills wanted: '+', '.join(s for s in skills),
                                       className='text-left text-dark'),
                            ]),className='border-info mb-2 card2', style={"height": "90%"}),
                xs=12, sm=12, md=12, lg=4, xl=4)
            cards.append(card)
    return cards



# creating some values for the initial loading of the page
selected_df = query_selected('Data Analyst', 'Canada')
w_cloud = description_wordcloud(selected_df)
img = BytesIO()
description_wordcloud(selected_df).save(img, format='PNG')
initial_img = 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())
salary, salary_url = job_salary('Data Analyst', 'Canada')
position = 'Data Analyst'
country = 'Canada'


"""
now we start building the app
the web app containes 5 major rows containing the following:
    1st-row: the main title of the app
    2nd-row: a card containing 2 selects one for the position and one for the country
    3rd-row: a card that have all the graphs with 3 rows:
        1st-row: the graphs for most wanted soft and technical skills
        2nd-row: the most frequent locations and companies
        3rd-row: the dates graph and the wordcloud image wraped in a card
    4th-row: a card with the list of recent jobs card nested in it
    5th-row: a card with some notes
"""
# this is the second row and it contaings 2 selects and a button for confirming the selected query
selection_card = dbc.Card([
    dbc.CardBody([
        html.H4("Select Profession and Country: ", className='text-left text-dark font-weight-bold'),
        dbc.Row([
            dbc.Col(dbc.Select(id='jobtitle-slc', options=[{'label':l, 'value':l} for l in positions], 
                               value='Data Analyst', className='btn btn-primary border-primary dropdown-toggle',
                               style={'textAlign': "center", "width": "100%"}),
                    className='mb-2', xs=12, sm=12, md=12, lg=5, xl=5),
            dbc.Col(dbc.Select(id='country-slc', options=[{'label':l, 'value':l} for l in countries],
                               value='Canada', className='btn btn-primary border-primary dropdown-toggle',
                               style={'textAlign': "center", "width": "100%"}),
                    className='mb-2', xs=12, sm=12, md=12, lg=5, xl=5),
            dbc.Col(dbc.Button('Go!', id='query_df', n_clicks=0, className='btn btn-primary border-primary',
                               style={'width': '100%'}),
                    className='mb-2', xs=12, sm=12, md=12, lg=2, xl=2),
        ],className='mt-2')]
    )], className='border-info')


# this is the third row containging all the graphs
graph_card = dbc.Card([
    dbc.CardBody([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                      html.H4("Number of Collected Data Analyst jobs in Canada", id='job-count-text',
                              className='text-center text-dark font-weight-bold'),
                      html.H2(str(len(selected_df)), id='job-count',className='text-center text-dark font-weight-bold')
                    ])
                ], className='border-primary')
            ],className='mb-2', xs=12, sm=12, md=12, lg=6, xl=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                      html.H4("Average Base Salary of a Data Analyst job in Canada", id='salary-text',
                              className='text-center text-dark font-weight-bold'),
                      html.H2(html.A(children=[salary], href=salary_url, className='text-dark', id='salary-value'),
                              className='text-center text-dark font-weight-bold')
                    ])
                ], className='border-primary')
            ], xs=12, sm=12, md=12, lg=6, xl=6),
        ],className='mb-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(id='tech-graph'),), className='border-primary'), className='mb-2',
                    xs=12, sm=12, md=12, lg=6, xl=6),
            dbc.Col(dbc.Card(dbc.CardBody([dcc.Graph(id='soft-graph')]), className='border-primary'),
                    xs=12, sm=12, md=12, lg=6, xl=6),
        ], className='mb-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([dcc.Graph(id='locations-donut')]), className='border-primary'),
                    className='mb-2', xs=12, sm=12, md=12, lg=6, xl=6),
            dbc.Col(dbc.Card(dbc.CardBody([dcc.Graph(id='company-graph')]), className='border-primary'),
                    xs=12, sm=12, md=12, lg=6, xl=6),
        ], className='mb-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([dcc.Graph(id='post-date')]), className='border-primary'),
                    className='mb-2', xs=12, sm=12, md=12, lg=6, xl=6),
            dbc.Col([
                dls.Ring(children=[
                    dbc.Card([
                        dbc.CardBody(html.H4("Word Cloud For Jobs Description",
                                             className='card-text text-center text-dark font-weight-bold mb-2')),
                        dbc.CardImg(src=initial_img,
                                    id='word-cloud', bottom=True)], className='border-primary card2')],
                    id="loading", color='#05ccf0', speed_multiplier=1.2, fullscreen=True, show_initially=True,
                    width=150,fullscreen_style={'background-color': 'rgba(12, 29, 31, 0.6)'}
                         ),
            ], xs=12, sm=12, md=12, lg=6, xl=6),
        ])
    ])
], className='border-info mt-2')


# this is the forth row containging the recent job cards
jobs_card = dbc.Card(
    dbc.CardBody(children=[html.H3("Some Recent Jobs: ", className="text-left text-dark font-weight-bold mb-4"),
                                            dbc.Row(id='jobs-cards', children=latest_6_jobs(selected_df),
                                                    className='mt-2')],),className='border-info')


# this is the last row whch is the notes row
notes_card = dbc.Card(
    dbc.CardBody([
        html.H5('- Please Note The Following:',className='text-left text-dark font-weight-bold mb-4'),
        dcc.Markdown(
            """
            \t\t * This is just a personal project and it's not related to Indeed in any way or form, also it might be a bit baised towards my own experiences.\n
            """,className='text-left text-dark my-n3 mt-4'),
        dcc.Markdown(
            """
            \t\t * This project is open source and you can find it on my Linked-In or GitHub, so feel free to experiment with it.\n
            """,className='text-left text-dark my-n3'),
        dcc.Markdown(
            """
            \t\t * I'm ralativly new to the Data Analysis field and working through this project has been a great learning experience, so if you have any tips or tricks i would love to hear them.\n
            """,className='text-left text-dark my-n3'),
        dcc.Markdown(
            """
            \t\t * Sorry for the extremely boring UX \N{GRINNING FACE}.\n
            """,className='text-left text-dark my-n3'),
        html.P(children=['Contact me through my ',
                        html.A('Linked-In', href='https://www.linkedin.com/in/nabil-mostafa1/',className='text-dark'),
                        html.A(', or '),
                        html.A('GitHub.', href='https://github.com/NabilMostafa1',className='text-dark')],
               className='text-left text-dark font-weight-bold mt-4 mb-0'),
        html.P(id='placeholder', style={'display': 'none'})
    ],
    ),className='card bg-info')



# now we intialize the app
app = dash.Dash(__name__, meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1.0'}])
server = app.server
# setting the app title and icon
app.title = 'Indeed Jobs Analysis'
app._favicon = ("favicon.ico")

# creating the app layout
app.layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("Indeed Jobs Analysis",
                            className='text-center text-dark mt-3 mb-4 font-weight-bold outline'), width=12)),
    dbc.Row(dbc.Col(selection_card)),
    dbc.Row(dbc.Col(graph_card), className='mb-2'),
    dbc.Row(dbc.Col(jobs_card), className='mb-2'),
    dbc.Row(dbc.Col(notes_card), className='mb-2'),
], fluid=True)


# the first callback function is trigerred with the button click to update the query and graghs
@app.callback(
    Output('job-count-text', 'children'),
    Output('job-count', 'children'),
    Output('salary-text', 'children'),
    Output('salary-value', 'children'),
    Output('salary-value', 'href'),
    Output('jobs-cards', 'children'),
    Output('tech-graph', 'figure'),
    Output('soft-graph', 'figure'),
    Output('locations-donut', 'figure'),
    Output('company-graph', 'figure'),
    Output('post-date', 'figure'),
    Output('word-cloud', 'src'),
    Input('query_df', 'n_clicks'),
    prevent_initial_call=False
)
def update_dashbaord(clicks):
    global position, country
    selected_df = query_selected(position, country)
    salary, salary_url = job_salary(position, country)
    job_count_text = "Number of Collected {} jobs in {}".format(position, country)
    salary_text = "Average Base salary of a {} in {} is".format(position, country)
    img = BytesIO()
    description_wordcloud(selected_df).save(img, format='PNG')
    tech_fig = plot_top10_skills(selected_df, 'technical_skills')
    job_count = len(selected_df)
    job_cards = latest_6_jobs(selected_df)
    soft_fig = plot_top10_skills(selected_df, 'soft_skills')
    company = plot_top10_companies(selected_df)
    location = plot_top10_locations(selected_df)
    job_dates = plot_dates(selected_df)
    return job_count_text, job_count, salary_text, salary, salary_url, job_cards, tech_fig, soft_fig, location, company, job_dates, 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())



# 2nd callback function is for updating the select values
# please note that the function output is a dummy output but for some reason dash callback function must have a callback output
@app.callback(
    Output('placeholder', 'children'),
    Input('jobtitle-slc', 'value'),
    Input('country-slc', 'value'),
)
def get_select_values(slctd_position, slctd_country):
    global position, country
    position = slctd_position
    country = slctd_country
    r = []
    return r



if __name__ == '__main__':
    app.run_server(debug=False)



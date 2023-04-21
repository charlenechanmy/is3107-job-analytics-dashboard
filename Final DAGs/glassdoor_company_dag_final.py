from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
from nrclex import NRCLex
from sklearn.preprocessing import MinMaxScaler
from time import *
import re
import json
import requests
import pandas as pd
import numpy as np
import pymysql
import os
import sqlalchemy

apiKey = "abe709a41fc1572d2b91936755a6e27b"
# Define default arguments for the DAG
default_args = {
    'owner': 'xuanming',    
    'start_date': datetime(2023, 4, 1),
    'retries': 1,    
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG with a unique ID and default arguments
dag = DAG(
    'glassdoor_company_insights_dag',
    default_args=default_args,
    schedule_interval='@weekly'
    # run the DAG once a week
)

def scrape_company_data():
    # Converts integer values of relevant rows from string to integer     
    def convert_to_int(value):
        if value[:-1].replace('.', '').isdigit() and value[-1].lower() == 'k':
            multiplier = 10 ** 3
            value = value[:-1].replace(',', '')
            converted_value = float(value) * multiplier
            return converted_value

    def scrapingcompanies(noOfPages):
        companies = []
        page_counter = 1
        print("Scraping Begun")
        try:
            while page_counter <= noOfPages:
                url = f"https://www.glassdoor.com/Reviews/index.htm?overall_rating_low=0&page={page_counter}&locId=3235921&locType=C&locName=Singapore%20(Singapore)&filterType=RATING_OVERALL"
                payload = {'api_key': apiKey, 'url': url}
                r = requests.get('http://api.scraperapi.com', params=payload)
                print("Page counter ", page_counter)
                soup = BeautifulSoup(r.text.encode('utf-8'), "html.parser")
                company_listings = soup.body.find_all('div',{'data-test': 'employer-card-single'})
                for company in company_listings:
                    companyName = company.find("h2",attrs={'data-test': 'employer-short-name'}).text
                    companyIndustry = company.find("span",{'data-test':'employer-industry'}).text
                    companyGlobalSize = company.find("span",{'data-test':'employer-size'}).text
                    companyLocation = 'Singapore'

                    # Converting relevant company details from string to integer format. E.g. '10k' -> 10000
                    companyRating = float(company.find(class_='pr-xsm ratingsWidget__RatingsWidgetStyles__rating').text)
                    companyReviews = convert_to_int(company.find("h3", {'data-test':'cell-Reviews-count'}).text)
                    companySalaryNo = convert_to_int(company.find("h3", {'data-test':'cell-Salaries-count'}).text)
                    companyJobs = convert_to_int(company.find("h3", {'data-test':'cell-Jobs-count'}).text)
                    companyDescription = company.find("p", {"class": 'css-1sj9xzx css-56kyx5'}).text if company.find("p", {"class": 'css-1sj9xzx css-56kyx5'}) is not None else "None"
                    
                    #Obtaining reviews
                    companyURL = company.find("a", {'data-test':'cell-Reviews-url'}).get('href')
                    newurl = f"https://www.glassdoor.com{companyURL}"
                    payload = {'api_key': apiKey, 'url': newurl}
                    newr = requests.get('http://api.scraperapi.com', params=payload)
                    newsoup = BeautifulSoup(newr.text.encode('utf-8'), "html.parser")
                    companyCons = []
                    companyPros = []
                    if newsoup.body is not None:
                        pros = newsoup.body.find_all('li', {'data-term-type': 'PRO'})
                        cons = newsoup.body.find_all('li', {'data-term-type': 'CON'})
                        # Append pros and cons into an initial list
                        for pro in pros:
                            currentPros = pro.find("a", {'class':'css-1vg6q84 e108tx6a4'}).text
                            if currentPros:    
                                companyPros.append(currentPros)
                        for con in cons:
                            currentCons = con.find("a", {'class':'css-1vg6q84 e108tx6a4'}).text
                            if currentCons:
                                companyCons.append(currentCons)

                    companies.append([companyName, companyIndustry, companyGlobalSize, companyLocation, companyRating, companyReviews, companySalaryNo, companyJobs, companyDescription, companyPros, companyCons])
                sleep(2)
                page_counter += 1
            return companies
        except:
            return companies

    allcompanies = scrapingcompanies(25)
    headers = ["Company Name", "Industry", "Global Size", "Location", "Rating", "No. of Reviews", "No. of Salary", "No. of Jobs", "Description", "Pros", "Cons"]
    companyList = pd.DataFrame(allcompanies, columns = headers)
    return companyList

def sentiment_company_reviews(df):
    """
    PARAMETERS:
    dataframe with company reviews scraped from Glassdoor
    
    Output:
    emotions appended to the dataframe
    """
    #combine pros and cons of company into a string
    df['reviews'] = df['Pros'] + df['Cons']
    df['reviews'] = df['reviews'].apply(lambda x: str(x))
    
    #helper function to remove brackets
    def list_string(lst):
        str_with_quotes = str(lst)
        str_without_quotes = str_with_quotes.replace('[','').replace(']','').replace("'","").replace('"', '').strip()
        return str_without_quotes

    # Convert previous lists to String. Also, removes " and ' characters to sanitize the SQL Inputs.
    df['reviews'] = df['reviews'].apply(list_string)
    df['Pros'] = df['Pros'].apply(list_string)
    df['Cons'] = df['Cons'].apply(list_string)
    df['Description'] = df['Description'].apply(list_string)
    
    count = lambda x: len(NRCLex(x).words)
    df['word_count'] = df['reviews'].apply(count)
    
    emo_score = lambda x: NRCLex(x).raw_emotion_scores
    df['score'] = df['reviews'].apply(emo_score)
    emotion_data = list(df['score'])
    emotion_value = pd.DataFrame(emotion_data)

    emotion_value = emotion_value.fillna(0)
    emotion_value = emotion_value.astype(int)
    
    #scale with MinMaxScaler
    emotion_value=pd.DataFrame(MinMaxScaler().fit_transform(emotion_value.T).T,columns=emotion_value.columns)
    emotion_value = emotion_value.round(2)

    #append the emotions table
    df = pd.concat([df, emotion_value], axis=1)
    
    return df

def scrape_data(**context):
    companies = scrape_company_data()
    print(companies)
    data_dict = companies.to_dict(orient='records')
    json_data = json.dumps(data_dict)
    context['ti'].xcom_push(key='company_data', value = json_data)
    return "Data pushed to xcom."

def sentiment_analysis(**context):
    json_data = context['ti'].xcom_pull(key='company_data')
    data_dict = json.loads(json_data)
    companies = pd.DataFrame.from_dict(data_dict)
    companies_with_sentiment = sentiment_company_reviews(companies)
    data_dict = companies_with_sentiment.to_dict(orient='records')
    json_data = json.dumps(data_dict)
    context['ti'].xcom_push(key='company_data_with_sentiment', value = json_data)

def remove_quotes(s):
    if isinstance(s, str):
        return s.replace('\'', '').replace('\"', '')
    return s

def push_to_database(**context):
    host = "db4free.net"
    port = 3306
    user = "rootjingen"
    password = "password"
    database = "is3107"
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    print("Connected")
    json_data = context['ti'].xcom_pull(key='company_data_with_sentiment')
    data_dict = json.loads(json_data)
    companies = pd.DataFrame.from_dict(data_dict)
    companies.replace({np.nan: 0}, inplace=True)

    # For each row in company data, insert into database. If the company name exists, update the values.
    for index, row in companies.iterrows():
        insert_query = """INSERT INTO company_table (company_name, industry, global_size, location, rating,
                            review_count, salary_count, job_count, description, pros, cons, 
                            positive, anticipation, joy, surprise, trust, anger, disgust, fear, negative, sadness)
                            VALUES ("{0}", "{1}", "{2}", "{3}", "{4}", "{5}", "{6}", "{7}", "{8}", "{9}", "{10}", {11}, {12},
                            {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20})
                            ON DUPLICATE KEY UPDATE
                            company_name = VALUES(company_name),
                            industry = VALUES(industry),
                            global_size = VALUES(global_size),
                            location = VALUES(location),
                            rating = VALUES(rating),
                            review_count = VALUES(review_count),
                            salary_count = VALUES(salary_count),
                            job_count = VALUES(job_count),
                            description = VALUES(description),
                            pros = VALUES(pros),
                            cons = VALUES(cons),
                            positive = VALUES(positive),
                            anticipation = VALUES(anticipation),
                            joy = VALUES(joy),
                            surprise = VALUES(surprise),
                            trust = VALUES(trust),
                            anger = VALUES(anger),
                            disgust = VALUES(disgust),
                            fear = VALUES(fear),
                            negative = VALUES(negative),
                            sadness = VALUES(sadness)""".format(
                            row['Company Name'], row['Industry'], row['Global Size'], row['Location'], row['Rating'],
                            row['No. of Reviews'], row['No. of Salary'], row['No. of Jobs'], row['Description'], 
                            row['Pros'], row['Cons'], row['positive'], row['anticipation'], row['joy'], row['surprise'],
                            row['trust'], row['anger'], row['disgust'], row['fear'], row['negative'], row['sadness'])
        
        print(insert_query)

        # execute the insert query
        cursor.execute(insert_query)

    # commit the changes to your AWS database
    conn.commit()

    # Close the connection
    conn.close()
    return "Pushed to Database."
    
# Define a PythonOperator to run the scrape_data() function
scrape_task = PythonOperator(
    task_id='glassdoor_company_webscraping',    
    python_callable=scrape_data,
    provide_context = True,
    dag=dag
)

def validate():
    host = "db4free.net"
    port = 3306
    user = "rootjingen"
    password = "password"
    database = "is3107"
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()

    # Validates that a table exists. If not, create the table.
    create_query = """CREATE TABLE IF NOT EXISTS company_table (
        company_name VARCHAR(255),
        industry VARCHAR(255),
        global_size VARCHAR(255),
        location VARCHAR(255),
        rating FLOAT,
        review_count INTEGER,
        salary_count INTEGER,
        job_count INTEGER,
        description VARCHAR(5000),
        pros VARCHAR(5000),
        cons VARCHAR(5000),
        positive FLOAT,
        anticipation FLOAT,
        joy FLOAT,
        surprise FLOAT,
        trust FLOAT,
        anger FLOAT,
        disgust FLOAT,
        fear FLOAT,
        negative FLOAT,
        sadness FLOAT,
        PRIMARY KEY (company_name)
    );"""
    cursor.execute(create_query)
    conn.commit()
    conn.close()
    return "Test Run Finished"

# Define the DAG dependencies
# scrape_task

with dag as glassdoor_company_insights_dag:
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        provide_context=True,
    )

    analyse_sentiment = PythonOperator(
        task_id="analyse_sentiment",
        python_callable=sentiment_analysis,
        provide_context=True,
    )

    upload = PythonOperator(
        task_id='upload_data',    
        python_callable=push_to_database,
        provide_context = True,
        dag=dag
    )
    validate >> scrape_task >> analyse_sentiment >> upload
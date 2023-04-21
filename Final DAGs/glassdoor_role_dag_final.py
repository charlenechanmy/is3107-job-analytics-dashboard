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
    'start_date': datetime(2023, 4, 12),
    'retries': 1,    
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG with a unique ID and default arguments
dag = DAG(
    'glassdoor_role_dag',
    default_args=default_args,
    schedule_interval='@weekly'
    # run the DAG once a week
)

def scrapeRolesSentiment_company_review(apiKey):
    # Scrape the company reviews for the role.

    def scraper(apiKey):
        jobSearches = [
            "software-engineer",
            "data-analyst",
            "data-engineer",
            "data-scientist",
            "product-manager",
            "systems-analyst",
            "it-security-analyst",
            "devops-engineer",
            "cloud-architect",
            "app-developer",
            "web-developer"
        ]
        jobReviewList = []
        for search in jobSearches:
            url = f"https://www.glassdoor.sg/Career/{search}-career_KO0,{len(search)}.htm"
            payload = {'api_key': apiKey, 'url': url}
            r = requests.get('http://api.scraperapi.com', params=payload)
            soup = BeautifulSoup(r.text.encode('utf-8'), "html.parser")

            #Reviews
            reviews = soup.find(class_ ="css-1s4z4nm")      
            reviewCompanies = [i.text for i in reviews.findAll(class_ = "ml-xsm")]

            #Find every even index and extract the text within the "" brackets, to extract the relevant company reviews
            reviewList = [re.findall('“(.*?)”', reviews.findAll(class_ = "p-std")[i * 2 - 1].text)[0] for i in range(len(reviews.findAll(class_ = "p-std")) // 2)]
            companyReviews = {}
            for i in range(len(reviewCompanies)):
                companyReviews[reviewCompanies[i]] = reviewList[i]

            jobReviewList.append({'Company Reviews': companyReviews})
        return jobReviewList
    
    result = scraper(apiKey)
    
    """COMPANY REVIEW"""
    df_company_review = pd.DataFrame()
    df_result = pd.DataFrame()
    jobSearches = [
            "software-engineer",
            "data-analyst",
            "data-engineer",
            "data-scientist",
            "product-manager",
            "systems-analyst",
            "it-security-analyst",
            "devops-engineer",
            "cloud-architect",
            "app-developer",
            "web-developer"
        ]
    for i in range(0,11):
        df_temp = pd.DataFrame.from_dict(result[i]['Company Reviews'],orient='index',columns=['Review'])
        df_temp.index.name = 'Company'
        df_temp['role'] = jobSearches[i]
        df_result = pd.concat([df_result, df_temp], axis = 0)

    df_company_review = df_result.rename(index={0: "Company", 1: "Review", 2: "Job Title"})
    df_company_review = df_company_review.reset_index()        
    return df_company_review

def scrapeRolesSentiment_additional_insights(apiKey):
    # Scrape the additional insights for the role. We used the day to day insights for the most accurate representation.
    def scraper(apiKey):
        jobSearches = [
            "software-engineer",
            "data-analyst",
            "data-engineer",
            "data-scientist",
            "product-manager",
            "systems-analyst",
            "it-security-analyst",
            "devops-engineer",
            "cloud-architect",
            "app-developer",
            "web-developer"
        ]
        jobReviewList = []
        for search in jobSearches:
            url2 = f"https://www.glassdoor.sg/Career/{search}-day-to-day-insights_KO0,{len(search)}_KC{len(search) + 1},{len(search) + 14}.htm"

            # Obtaining day to day reviews about the role
            payload2 = {'api_key': apiKey, 'url': url2}
            r2 = requests.get('http://api.scraperapi.com', params=payload2)
            soup = BeautifulSoup(r2.text.encode('utf-8'), "html.parser")
            dayToDay = soup.select('[class^="mb-sm-sm p-std css-poeuz4 css-1wh1oc8 "]')
            dayToDay = [re.findall('“(.*?)”',i.text)[0] for i in dayToDay]
            jobReviewList.append({'Additional Insights': dayToDay})

            for count in range(2, 7):
                # Scrape 5 pages' worth of day to day insights. In the event that there is no page (i.e. no further reviews), break the loop.
                try:
                    url3 = f"https://www.glassdoor.sg/Career/{search}-day-to-day-insights_KO0,{len(search)}_KC{len(search) + 1},{len(search) + 14}_P{count}.htm"
                    r3 = requests.get('http://api.scraperapi.com', params={'api_key': apiKey, 'url': url3})
                    soup = BeautifulSoup(r2.text.encode('utf-8'), "html.parser")
                    dayToDayAdditional = soup.select('[class^="mb-sm-sm p-std css-poeuz4 css-1wh1oc8 "]')
                    dayToDayAdditional = [re.findall('“(.*?)”',i.text)[0] for i in dayToDayAdditional]
                    jobReviewList[0]['Additional Insights'].extend(dayToDayAdditional)
                except:
                    break
        return jobReviewList
    
    result = scraper(apiKey)
    
    """ADDITIONAL INSIGHTS"""
    jobSearches = [
            "software-engineer",
            "data-analyst",
            "data-engineer",
            "data-scientist",
            "product-manager",
            "systems-analyst",
            "it-security-analyst",
            "devops-engineer",
            "cloud-architect",
            "app-developer",
            "web-developer"
        ]   
    temp = []
    for i in range(0,11):
        temp.append(result[i]['Additional Insights'])
            
    df_additional_insights = pd.DataFrame()
    df_additional_insights['Insights'] = temp
    df_additional_insights['Role'] = jobSearches
    df_additional_insights
       
    return df_additional_insights

def sentiment_role_reviews(df_additional_insights, df_company_review):
    """
    PARAMETERS:
    1) dataframe with additional insights for each role
    2) dataframe with company review for each role
    
    Output:
    emotions appended to the dataframe
    """
    #helper function to remove brackets
    def list_string(lst):
        str_with_quotes = str(lst)
        str_without_quotes = str_with_quotes.replace('[','').replace(']','').replace("'","").replace('"', '').strip()
        return str_without_quotes
    
    #groupby role
    df_grouped = df_company_review.groupby('role')['Review'].apply(lambda x: ' '.join(x)).reset_index()
    df_grouped = df_grouped.rename(columns={'role': 'Role'})
    
    df_additional_insights['Insights'] = df_additional_insights['Insights'].apply(lambda x: ' '.join(x))
    
    #merge 2 dataset together
    df_final_review = df_grouped.merge(df_additional_insights, on='Role', how='left')
    
    #combine reviews and insights together
    df_final_review['Review'] = df_final_review['Review'].apply(list_string)
    df_final_review['Insights'] = df_final_review['Insights'].apply(list_string)
    df_final_review['Consolidated_Review'] = df_final_review['Review'] + df_final_review['Insights']

    
    count = lambda x: len(NRCLex(x).words)
    df_final_review['score'] = df_final_review['Consolidated_Review'].apply(count)
    
    emo_score = lambda x: NRCLex(x).raw_emotion_scores
    df_final_review['score'] = df_final_review['Consolidated_Review'].apply(emo_score)
    emotion_data = list(df_final_review['score'])
    
    emotion_value = pd.DataFrame(emotion_data)

    emotion_value = emotion_value.fillna(0)

    emotion_value = emotion_value.astype(int)
    
    #scale using MinMax
    emotion_value=pd.DataFrame(MinMaxScaler().fit_transform(emotion_value.T).T,columns=emotion_value.columns)
    emotion_value = emotion_value.round(2)

    #append emotions table to final dataframe
    df_final_review = pd.concat([df_final_review, emotion_value], axis=1)
    df_final_review['Role'] = df_final_review['Role'].apply(lambda x: " ".join(x.split("-")).title())
    df_final_review = df_final_review.drop('score', axis = 1)
    
    return df_final_review

def scrape_role_data_by_company(**context):
    role_data_company_review = scrapeRolesSentiment_company_review(apiKey)
    print("Scraped Data")
    data_dict = role_data_company_review.to_dict(orient='records')
    json_data = json.dumps(data_dict)
    context['ti'].xcom_push(key='role_company_review', value = json_data)
    return "Data pushed to xcom."

def scrape_role_data_additional_insights(**context):
    role_data_additional_insights = scrapeRolesSentiment_additional_insights(apiKey)
    print("Scraped Data")
    data_dict = role_data_additional_insights.to_dict(orient='records')
    json_data = json.dumps(data_dict)
    context['ti'].xcom_push(key='role_additional_insights', value = json_data)
    return "Data pushed to xcom."

def sentiment_analysis(**context):
    role_company_json = context['ti'].xcom_pull(key='role_company_review')
    role_company_data_dict = json.loads(role_company_json)
    role_company_df = pd.DataFrame.from_dict(role_company_data_dict)
    role_additional_insights_json = context['ti'].xcom_pull(key='role_additional_insights')
    role_additional_insights_data_dict = json.loads(role_additional_insights_json)
    role_additional_insights_df = pd.DataFrame.from_dict(role_additional_insights_data_dict)

    role_df = sentiment_role_reviews(role_additional_insights_df, role_company_df)
    data_dict = role_df.to_dict(orient='records')
    json_data = json.dumps(data_dict)
    context['ti'].xcom_push(key='role_data_with_sentiment', value = json_data)

def push_to_database(**context):
    host = "db4free.net"
    port = 3306
    user = "rootjingen"
    password = "password"
    database = "is3107"
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    print("Connected")
    json_data = context['ti'].xcom_pull(key='role_data_with_sentiment')
    data_dict = json.loads(json_data)
    role_data = pd.DataFrame.from_dict(data_dict)
    role_data.replace({np.nan: 0}, inplace=True)

    # Insert query for each row in role_data. Replace value if there is a duplicate Role to ensure updated data.
    for index, row in role_data.iterrows():
        insert_query = """INSERT INTO role_table (role, review, consolidated_review, insights, 
                            positive, anticipation, joy, surprise, trust, anger, disgust, fear, negative, sadness)
                            VALUES ("{0}", "{1}", "{2}", "{3}", {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12},
                            {13})
                            ON DUPLICATE KEY UPDATE
                            role = VALUES(Role),
                            review = VALUES(Review),
                            consolidated_review = VALUES(Consolidated_Review),
                            insights = VALUES(Insights),
                            positive = VALUES(positive),
                            anticipation = VALUES(anticipation),
                            joy = VALUES(joy),
                            surprise = VALUES(surprise),
                            trust = VALUES(trust),
                            anger = VALUES(anger),
                            disgust = VALUES(disgust),
                            fear = VALUES(fear),
                            negative = VALUES(negative),
                            sadness = VALUES(sadness)""".format(row['Role'], row['Review'], row['Consolidated_Review'], row['Insights'], row['positive'], row['anticipation'], row['joy'], row['surprise'],
                            row['trust'], row['anger'], row['disgust'], row['fear'], row['negative'], row['sadness'])

        print(insert_query)
        # execute the insert query
        cursor.execute(insert_query)

    # commit the changes to your AWS database
    conn.commit()

    # Close the connection
    conn.close()
    return "Pushed to Database."

def validate():
    host = "db4free.net"
    port = 3306
    user = "rootjingen"
    password = "password"
    database = "is3107"
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    create_query = """CREATE TABLE IF NOT EXISTS role_table (
        role VARCHAR(500),
        review VARCHAR(1000),
        consolidated_review VARCHAR(2000),
        insights VARCHAR(2000),
        anticipation FLOAT,
        positive FLOAT,
        joy FLOAT,
        trust FLOAT,
        surprise FLOAT,
        disgust FLOAT,
        negative FLOAT,
        fear FLOAT,
        sadness FLOAT,  
        anger FLOAT,
        PRIMARY KEY (Role)
    );"""
    cursor.execute(create_query)
    conn.commit()
    conn.close()
    return "Test Run Finished"

# Define the DAG dependencies
# scrape_task

with dag as glassdoor_company_dag:
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        provide_context=True,
    )

    scrape_role_additional_insights = PythonOperator(
        task_id="scrape_role_additional_insights",
        python_callable=scrape_role_data_additional_insights,
        provide_context=True,
    )

    scrape_role_company_reviews = PythonOperator(
        task_id="scrape_role_company_reviews",
        python_callable=scrape_role_data_by_company,
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

    validate >> scrape_role_additional_insights
    validate >> scrape_role_company_reviews
    scrape_role_additional_insights >> analyse_sentiment
    scrape_role_company_reviews >> analyse_sentiment
    analyse_sentiment >> upload
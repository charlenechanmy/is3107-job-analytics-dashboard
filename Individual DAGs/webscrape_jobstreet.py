from datetime import datetime, timedelta
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'xuanming',    
    'start_date': datetime(2023, 4, 11),
    'retries': 1,    
    'retry_delay': timedelta(minutes=5)
}
# Define the DAG with a unique ID and default arguments
dag = DAG(
    'webscrape_jobstreet',
    default_args=default_args,
    schedule_interval='@weekly'
    # run the DAG once a week
)

def scrape_raw_jobstreet(**kwargs):
    job_roles = ["data-analyst", "data-scientist","data-engineer","product-manager",
                              "systems-analyst","IT-Security-Analyst","DevOps-Engineer","Cloud-Architect",
                              "Mobile-App-Developer","Web-Developer"]
    df = pd.DataFrame(columns=['Title', 'URL', 'Company', 'Salary', 'Job Type', 'Searched-Title'])
    count = 0
    for job in job_roles:
        print("Scraping for " + job)
        url = "https://www.jobstreet.com.sg/" + job + "-jobs"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        find = soup.find('div', {"id": "jobList"})
        # need to change class name because this changes after certain time period
        # Old class name
        n_pages = find.find('span', {"class": 'z1s6m00 _1hbhsw64y y44q7i0 y44q7i1 y44q7i21 _1d0g9qk4 y44q7i7'})
        
        n_pages = int(n_pages.text.split(" ")[2].replace(',', '')) // 30 + 1
        for p in range(1, n_pages):
            if p % 10 == 0:
                print("Page: " + str(p))
            url = "https://www.jobstreet.com.sg/" + job + "-jobs?pg=" + str(p)

            response = requests.get(url)

            soup = BeautifulSoup(response.content, "html.parser")

            job_listings = soup.find_all('article')
            for j in range(len(job_listings)):
                jobs = []
                jobs.append(job_listings[j].find('h1').text)
                job_description = "https://www.jobstreet.com.sg" + str(job_listings[j].find('a', href=True)['href'])
                jobs.append(job_description)
                
                # need to change class name because this changes after certain time period
                company = job_listings[j].find("span", {"class": "z1s6m00 _17dyj7u1 _1hbhsw64y _1hbhsw60 _1hbhsw6r"})
                
                if company != None:
                    jobs.append(company.text)
                # location_salary = job_listings[j].find_all("span", {"class": "z1s6m00 _1hbhsw64u y44q7i0 y44q7i3 y44q7i21 y44q7ih"})
                location_salary = job_listings[j].find_all("span", {"class": "z1s6m00 _1hbhsw64y y44q7i0 y44q7i3 y44q7i21 y44q7ih"})
                if len(location_salary) > 1:
                    jobs.append(location_salary[1].text)
                else: 
                    continue
                jobs.append(job_listings[j].find('dl').find_all('a')[-1].text)
                jobs.append(job)
                if len(jobs) == 6:
                    df.loc[count] = jobs
                    count += 1
    kwargs["ti"].xcom_push(key="jobstreet_raw", value=df)

# converting lower_bound and upper_bound to correct format and type
def convert_lower_bound(string):
    try:
        success = True
        if string[-1] == 'K':
            a = string[:-1]
            return float(a) * 1000
        elif string[-1] == 'M':
            a = string[:-1]
            return float(a) * 1000000
        else:
            return float(string)
    except (ValueError, IndexError):
        return 0

def data_processing(**kwargs):
    df = kwargs["ti"].xcom_pull(key="jobstreet_raw")
    print("Scraping Done -> Starting to clean data")
    # Cleaning of df
    # Adding Currency and Lower_Bound Frequency and Upper_Bound Columns
    df['Currency'] = df['Salary'].str.split(' ').str[0:3]
    df['Lower_Bound'] = df['Salary'].str.split(' - ').str[0].str.split(' ').str[-1]
    df['Frequency'] = df['Salary'].str.split(' - ').str[1].str.split(' ').str[1].str.replace(',','')
    df['Upper_Bound'] = df['Salary'].str.split(' ').str[-2]
    
    # Making Currency in correct format
    df['Currency'] = df['Currency'].apply(lambda x: x[0])
    df['Currency'] = df['Currency'].str[:3]
    
    df['Lower_Bound'] = df['Lower_Bound'].str[4:]
    df['Lower_Bound'] = df['Lower_Bound'].apply(lambda x: convert_lower_bound(x))
    df['Upper_Bound'] = df['Upper_Bound'].apply(lambda x: float(x.replace(',', "")))
    
    # Adding Website column to all rows and dropping all foreign rows with foreign currencies
    df['Website'] = ['JobStreet'] * len(df)
    df = df[df['Currency'] == "SGD"]
    
    # Sorting Columns in specified order
    df = df[['Title', 'Searched-Title', 'Company', 'Lower_Bound', 'Upper_Bound', 'Frequency', 'Job Type', 'Website', 'URL']]
    
    # Capitalising Frequency Column
    df['Frequency'] = df['Frequency'].str.capitalize()
    
    # Mapping Searched Title to specified titles
    job_roles_dict = {"data-analyst": "Data Analyst", "data-scientist": "Data Scientist", "data-engineer": "Data Engineer", "product-manager": "Product Manager",
                              "systems-analyst": "Systems Analyst", "IT-Security-Analyst": "IT Security Analyst", "DevOps-Engineer": "DevOps Engineer",
             "Cloud-Architect": "Cloud Architect", "Mobile-App-Developer": "Mobile App Developer", "Web-Developer": "Web Developer"}
    df['Searched-Title'] = df['Searched-Title'].map(job_roles_dict)
    
    # Renaming the Columns
    df = df.set_axis(['Job Title', 'Searched Job Title', 'Company Name', 'Salary Lower Bound',
                 'Salary Upper Bound', 'Salary Frequency', 'Job Type', 'Website', 'URL'], axis=1)
    
    kwargs["ti"].xcom_push(key="jobstreet_clean", value=df)


t1 = PythonOperator(
    task_id='scrape_raw_jobstreet',
    python_callable=scrape_raw_jobstreet,
    dag=dag
)

t2 = PythonOperator(
    task_id='jobstreet_data_processing',
    python_callable=data_processing,
    dag=dag
)

t1 >> t2
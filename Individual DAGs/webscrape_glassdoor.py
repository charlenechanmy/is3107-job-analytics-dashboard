from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import re
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
    'webscrape_glassdoor',
    default_args=default_args,
    schedule_interval='@weekly'
    # run the DAG once a week
)

def jobTypeFormatter(_type):
    if _type == "fulltime":
        return "Full-Time"
    elif _type == "parttime":
        return "Part-Time"
    elif _type == "contract":
        return "Contract"
    else:
        return "Internship"

def jobSearchFormatter(search):
    return " ".join([i.capitalize() for i in search.split("-")])

def scrape_raw_glassdoor(**kwargs):
    apiKey = "9edb1daaf83df2801254221d6202b156"
    def remove_non_numbers(s):
        return re.sub(r'\D+', '', s)

    def convert_pay_to_range(pay_string):
        whatWeNeed = pay_string.split()[0]
        if "−" in whatWeNeed:
            ranges = [float(remove_non_numbers(i)) * 1000 for i in whatWeNeed.split("−")]
            return ranges
        else:
            return [float(i) * 1000 for i in [remove_non_numbers(whatWeNeed), remove_non_numbers(whatWeNeed)]]
    
    types = [
        "fulltime",
        "parttime",
        "contract",
        "internship"
    ]
    
    seniorityTypes = [
        "internship",
        "entrylevel",
        "midseniorlevel",
        "director",
        "executive"
    ]
    
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
    counter = 1
    jobs = []
    for search in jobSearches:
        print("Beginning search ", counter)
        counter += 1
        for seniority in seniorityTypes:
            for _type in types:
                url = f"https://www.glassdoor.sg/Job/{search}-jobs-SRCH_KO0,{len(search)}.htm?jobType={_type}?fromAge=30&seniorityType={seniority}"
                payload = {'api_key': apiKey, 'url': url}
                r = requests.get('http://api.scraperapi.com', params=payload)
                soup = BeautifulSoup(r.text.encode('utf-8'), "html.parser")
                job_listings = soup.find_all(
                    'li', {'class': lambda x: x and x.startswith('react-job-listing')})
                job_listings = soup.find_all('li', {'class': lambda x: x and x.startswith('react-job-listing')})
                for job in job_listings:
                    companyName = job.find(class_="css-l2wjgv e1n63ojh0 jobLink").text
                    jobTitle = job.find(class_="jobLink css-1rd3saf eigr9kq2").text
                    location = "Singapore"
                    #location = job.find(class_="d-flex flex-wrap css-11d3uq0 e1rrn5ka2").text
                    pay = job.find(class_="css-1xe2xww e1wijj242")
                    salaryFrequency = "Monthly"
                    if pay:
                        pay = pay.text
                        salaryLowerbound, salaryUpperbound = convert_pay_to_range(pay)
                        if salaryLowerbound > 20000 or salaryUpperbound > 20000:
                            salaryFrequency = "Yearly"
                    else:
                        pay = "Not disclosed"
                        continue
                    # jobs.append([jobTitle, jobSearchFormatter(search), companyName, salaryLowerbound, salaryUpperbound, salaryFrequency,
                                 # jobTypeFormatter(_type), "Glassdoor", url])
                    
                    jobs.append([jobTitle, search, companyName, salaryLowerbound, salaryUpperbound, salaryFrequency,
                                 _type, "Glassdoor", url])
                    
                    header = ["Job Title", "Searched Job Title", "Company Name", "Salary Lower Bound", "Salary Upper Bound", "Salary Frequency", "Job Type", "Website", "URL"]
                    df = pd.DataFrame(jobs, columns = header)
                time.sleep(2)
                
    kwargs["ti"].xcom_push(key="glassdoor_raw", value=df)

def data_processing(**kwargs):
    df = kwargs["ti"].xcom_pull(key="glassdoor_raw")
    
    df['Searched Job Title'] = df['Searched Job Title'].apply(jobSearchFormatter)
    df['Job Type'] = df['Job Type'].apply(jobTypeFormatter)
    kwargs["ti"].xcom_push(key="glassdoor_clean", value=df)

t5 = PythonOperator(
    task_id='scrape_raw_glassdoor',
    python_callable=scrape_raw_glassdoor,
    dag=dag
)

t6 = PythonOperator(
    task_id='glassdoor_data_processing',
    python_callable=data_processing,
    dag=dag
)

t5 >> t6
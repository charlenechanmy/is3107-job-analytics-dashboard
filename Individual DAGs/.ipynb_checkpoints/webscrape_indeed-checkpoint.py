from datetime import datetime, timedelta
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import re

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

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
    'webscrape_indeed',
    default_args=default_args,
    schedule_interval='@weekly'
    # run the DAG once a week
)

def generate_urls():
    original_job_titles = ["Data Analyst",  # 1357 jobs
                "Data Scientist",  # 809 jobs
                "Data Engineer",  # 9,492 jobs
                "Product Manager",
                "Systems Analyst",
                "IT Security Analyst",
                "DevOps Engineer",
                "Cloud Architect",
                "Mobile App Developer",
                "Web Developer"]
    formatted_job_titles = [title.lower().replace(' ', '+')
                                        for title in original_job_titles]
    data = {}
    for i in range(len(formatted_job_titles)):
        job_title = formatted_job_titles[i]
        job_title_urls = [] 
        for page in range(0, 301, 10):
            job_title_urls.append(
                "https://sg.indeed.com/jobs?q="+job_title+"&l=Any&start="+str(page)+"&fromage=30")
            data[original_job_titles[i]] = job_title_urls
    return data


def scrape_one_page(searched_job_title, url):
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.binary_location = '/usr/bin/firefox'
    driver = webdriver.Firefox(options=options)
    driver.get(url)
            # Wait for the page to fully load
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")    
    job_listings = soup.find_all("div", class_="cardOutline")
    jobs_data = []
    for job_listing in job_listings: 
        try:
            job_title = job_listing.find("span").text.strip()
            company_name = job_listing.find(
                "span", {"class": "companyName"}).text.strip()
            salary_range = job_listing.find(
                "svg", attrs={"aria-label": "Salary"}).parent.text.strip()
            job_type = job_listing.find(
                "svg", attrs={"aria-label": "Job type"}).parent.text.strip()
            url = job_listing.find("a", {"class": "jcs-JobTitle"}).get("href") 
            if not url.startswith("https://sg.indeed.com"):
                url = "https://sg.indeed.com" + url
        except:
            continue
        # print(url)
        # print("Job Title:", job_title)        # print("Company Name:", company_name)
        # print("Salary Range:", salary_range)        # print("Job Type:", job_type)
        job_data = {
            "Job Title": job_title,            
            "Searched Job Title": searched_job_title,
            "Company Name": company_name,            
            "Salary Range": salary_range,
            "Job Type": job_type,            
            "Website": "Indeed",
            "URL": url        }
        jobs_data.append(job_data)
    driver.quit()
    df = pd.DataFrame(jobs_data)
    return df

def scrape_raw_indeed(**kwargs):
    data = generate_urls()
    page_counter = 1
    df = pd.DataFrame()    # return df
    for searched_job_title in data.keys():        
        urls = data[searched_job_title]
        for url in urls:            
            df_single = scrape_one_page(searched_job_title, url)
            df = pd.concat([df,df_single], ignore_index=True)            
            print("Number of pages scraped:", page_counter)
            page_counter += 1        
    kwargs["ti"].xcom_push(key="indeed_raw", value=df)

def data_processing(**kwargs):
    df = kwargs["ti"].xcom_pull(key="indeed_raw")
    # df = scrape_all_pages(generate_urls())    # XUAN MING REMOVE THIS!!!    
    
    df[['Salary Lower Bound', 'Salary Upper Bound', 'Salary Frequency']] = df['Salary Range'].str.extract(r'\$?(\d[\d,]*)?(?:\s*(?:−|-|\sto\s)\s*\$?(\d[\d,]*))?\s*(.*)')
    df['Salary Lower Bound'] = df['Salary Lower Bound'].str.replace(',', '', regex=True).str.replace('$', '', regex=True).astype(float)
    df['Salary Upper Bound'] = df['Salary Upper Bound'].str.replace(',', '', regex=True).str.replace('$', '', regex=True).astype(float)    
    month_regex = r'month'
    year_regex = r'year'    
    week_regex = r'week'
    # Define a function to check for the regex pattern in the salary range
    def check_salary_frequency(salary_range):        
        if re.search(month_regex, salary_range, re.IGNORECASE):
            return 'Monthly'        
        elif re.search(year_regex, salary_range, re.IGNORECASE):
            return 'Yearly'        
        elif re.search(week_regex, salary_range, re.IGNORECASE):
            return 'Weekly'        
        else:
            return None
    df['Salary Frequency'] = df['Salary Frequency'].apply(check_salary_frequency)    
    df.drop("Salary Range", axis=1, inplace=True)
    # Fixing Job Type
    pattern = r'(\w.*)\s+\d+$'
    
    def extract_text(input_string):
        formatted_string = re.sub(r'[^a-zA-Z]+', '', input_string)        
        job_types = ["Fulltime", "Parttime", "Permanent", "Contract"]
        new_job_types = ["Full-Time", "Part-Time", "Permanent", "Contract"]        
        if formatted_string in job_types:
            return new_job_types[job_types.index(formatted_string)]        
        return input_string
    df['Job Type'] = df['Job Type'].apply(extract_text)
        # Reorder the df
    new_order = ['Job Title', 'Searched Job Title', 'Company Name', 'Salary Lower Bound',                 
    'Salary Upper Bound', 'Salary Frequency', 'Job Type', 'Website', 'URL']
    df = df.reindex(columns=new_order)   
    
    kwargs["ti"].xcom_push(key="indeed_clean", value=df)

t3 = PythonOperator(
    task_id='scrape_raw_indeed',
    python_callable=scrape_raw_indeed,
    dag=dag
)

t4 = PythonOperator(
    task_id='indeed_data_processing',
    python_callable=data_processing,
    dag=dag
)

t3 >> t4

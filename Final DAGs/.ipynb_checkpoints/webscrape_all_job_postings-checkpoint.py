from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import re
from bs4 import BeautifulSoup
import pymysql

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

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
    'webscrape_all_job_postings',
    default_args=default_args,
    schedule_interval='@weekly'
    # run the DAG once a week
)

# Beginning of Jobstreet
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
            time.sleep(0.3)
            
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

def jobstreet_data_processing(**kwargs):
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

# Beginning of Indeed

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

def indeed_data_processing(**kwargs):
    df = kwargs["ti"].xcom_pull(key="indeed_raw")
       
    df[['Salary Lower Bound', 'Salary Upper Bound', 'Salary Frequency']] = df['Salary Range'].str.extract(r'\$?(\d[\d,]*)?(?:\s*(?:âˆ’|-|\sto\s)\s*\$?(\d[\d,]*))?\s*(.*)')
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

# Beginning of Glassdoor

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
    apiKey = "cfd0d3475f3d2cd5a9f6e13f9189c52a"
    def remove_non_numbers(s):
        return re.sub(r'\D+', '', s)

    def convert_pay_to_range(pay_string):
        whatWeNeed = pay_string.split()[0]
        if "âˆ’" in whatWeNeed:
            ranges = [float(remove_non_numbers(i)) * 1000 for i in whatWeNeed.split("âˆ’")]
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
                    jobs.append([jobTitle, search, companyName, salaryLowerbound, salaryUpperbound, salaryFrequency,
                                 _type, "Glassdoor", url])
                time.sleep(2)         
    header = ["Job Title", "Searched Job Title", "Company Name", "Salary Lower Bound", "Salary Upper Bound", "Salary Frequency", "Job Type", "Website", "URL"]
    df = pd.DataFrame(jobs, columns = header)
    kwargs["ti"].xcom_push(key="glassdoor_raw", value=df)

def glassdoor_data_processing(**kwargs):
    df = kwargs["ti"].xcom_pull(key="glassdoor_raw")
    
    df['Searched Job Title'] = df['Searched Job Title'].apply(jobSearchFormatter)
    df['Job Type'] = df['Job Type'].apply(jobTypeFormatter)
    kwargs["ti"].xcom_push(key="glassdoor_clean", value=df)
    
# Define Merge Task
def merge_dataset(**kwargs):
    jobstreet = kwargs["ti"].xcom_pull(key="jobstreet_clean")
    indeed = kwargs["ti"].xcom_pull(key="indeed_clean")
    glassdoor = kwargs["ti"].xcom_pull(key="glassdoor_clean")
    merged_df = pd.concat([jobstreet, indeed, glassdoor])
    kwargs["ti"].xcom_push(key="merged_dataset", value=merged_df)
    
# Define remove_duplicates
def remove_duplicates(**kwargs):
    df = kwargs["ti"].xcom_pull(key="merged_dataset")
    before = len(df)
    # First Layer: Removing using URL
    df = df.drop_duplicates(subset="URL", keep="first")
    # Second Layer: Removing using drop_duplicates
    cols_to_check = ['Job Title', 'Company Name']
    df = df.drop_duplicates(subset=cols_to_check, keep="first")
    # Third Layer: Removing using cosine similarity
    new_df = df.copy()
    columns_to_concat = ['Job Title', 'Company Name', 'Salary Lower Bound', 'Salary Upper Bound', 'Salary Frequency', 'Job Type']
    new_df = df[columns_to_concat].copy()
    new_df['Combined'] = new_df.apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    vectorizer = CountVectorizer().fit_transform(new_df['Combined'])
    cosine_sim = cosine_similarity(vectorizer)
    threshold_value = 0.90
    duplicates = []
    for i in range(len(cosine_sim)):
        for j in range(i+1, len(cosine_sim)):
            if cosine_sim[i][j] >= threshold_value and \
              (df.iloc[i]["Salary Lower Bound"] == df.iloc[j]["Salary Lower Bound"]) and \
              (df.iloc[i]["Salary Upper Bound"] == df.iloc[j]["Salary Upper Bound"]):
                duplicates.append(j)
    df = df.drop(df.index[duplicates])
    after = len(df)
    print("Length of df reduced from {} to {}.".format(before, after))
    kwargs["ti"].xcom_push(key="duplicates_removed_dataset", value=df)

# Define test write to database
def test_write_to_database():
    host = "db4free.net"
    port = 3306
    user = "rootjingen"
    password = "password"
    database = "is3107"
    
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = conn.cursor()
    create_query = """CREATE TABLE IF NOT EXISTS job_database (
        job_title VARCHAR(255),
        searched_job_title VARCHAR(255),
        company_name VARCHAR(255),
        salary_lower_bound FLOAT(10,2),
        salary_upper_bound FLOAT(10,2),
        salary_frequency VARCHAR(50),
        job_type VARCHAR(50),
        website VARCHAR(255),
        url VARCHAR(255),
        PRIMARY KEY (job_title, searched_job_title, company_name)
    );"""
    cursor.execute(create_query)
    conn.commit()
    conn.close()
    print("Test Run Finished")
    
# Define write to database
def write_to_database(**kwargs):
    df = kwargs["ti"].xcom_pull(key="duplicates_removed_dataset")
    host = "db4free.net"
    port = 3306
    user = "rootjingen"
    password = "password"
    database = "is3107"
    
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    
    # Create table if not exists
    cursor = conn.cursor()

    for index, row in df.iterrows():
        insert_query = """INSERT IGNORE INTO {0} (job_title, searched_job_title, company_name, 
                            salary_lower_bound, salary_upper_bound, salary_frequency, 
                            job_type, website, url)
                            VALUES ("{1}", "{2}", "{3}", "{4}", "{5}", "{6}", "{7}", "{8}", "{9}")
                            ON DUPLICATE KEY UPDATE
                            job_title="{1}", searched_job_title="{2}", company_name="{3}",
                            salary_lower_bound="{4}", salary_upper_bound="{5}",
                            salary_frequency="{6}", job_type="{7}", website="{8}", url="{9}"
                            """.format("job_database", row["Job Title"], row["Searched Job Title"], row["Company Name"], 
                                    row["Salary Lower Bound"], row["Salary Upper Bound"], row["Salary Frequency"], 
                                    row["Job Type"], row["Website"], row["URL"])
        cursor.execute(insert_query)
    conn.commit()
    conn.close()
    print("Task Successful.")    
    
# Define Tasks
    
t1 = PythonOperator(
    task_id='scrape_raw_jobstreet',
    python_callable=scrape_raw_jobstreet,
    dag=dag
)

t2 = PythonOperator(
    task_id='jobstreet_data_processing',
    python_callable=jobstreet_data_processing,
    dag=dag
)

t3 = PythonOperator(
    task_id='scrape_raw_indeed',
    python_callable=scrape_raw_indeed,
    dag=dag
)

t4 = PythonOperator(
    task_id='indeed_data_processing',
    python_callable=indeed_data_processing,
    dag=dag
)
    
t5 = PythonOperator(
    task_id='scrape_raw_glassdoor',
    python_callable=scrape_raw_glassdoor,
    dag=dag
)

t6 = PythonOperator(
    task_id='glassdoor_data_processing',
    python_callable=glassdoor_data_processing,
    dag=dag
)

t7 = PythonOperator(
    task_id='merge_dataset',
    python_callable=merge_dataset,
    dag=dag
)

t8 = PythonOperator(
    task_id='remove_duplicates',
    python_callable=remove_duplicates,
    dag=dag
)

t9 = PythonOperator(
    task_id='test_write_to_database',
    python_callable=test_write_to_database,
    dag=dag
)

t10 = PythonOperator(
    task_id='write_to_database',
    python_callable=write_to_database,
    dag=dag
)

t1 >> t2 
t3 >> t4
t5 >> t6

t2 >> t7
t4 >> t7
t6 >> t7

t7 >> t8
t8 >> t9
t9 >> t10
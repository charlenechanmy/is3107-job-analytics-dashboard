# IS3107 Final Repository

### Architecture & Structure 🧱

This repository is split into three folders:

```
IS3107_Repository
├───Final DAGs
│    │─── glassdoor_company_dag_final.py
│    │─── glassdoor_role_dag_final.py
│    └─── webscrape_all_job_postings.py
│
├───Individual DAGs
│    │─── webscrape_glassdoor.py
│    │─── webscrape_indeed.py
│    └─── webscrape_jobstreet.py
│
└───Individual IPYNBs
     │─── glassdoor.ipynb
     │─── indeed.ipynb
     │─── jobstreet.ipynb
     └─── removing_duplicates.ipynb

```

`Final DAGs` contains the Python DAGs that are run on Airflow.

`Individual DAGS` contains the testing DAGs that webscrape data from individual job portals.

`Individual IPYNBs` contains the Jupyter Notebooks used to test the Python code for webscraping.

### Background 📝

As evidenced by the fact that 1 in 2 job seekers are frustrated with the job search process, there are a multitude of challenges in the job-seeking process. With numerous channels of information, job seekers are uncertain of the latest job requirements and compensation packages. They may also feel confused about a company's or job role’s overall reputation and perception due to conflicting reviews. This can make it difficult for them to form an opinion regarding which company and role to pursue. This pressing issue needs to be solved to bridge the gap between employees and employers, in order to build a stronger workforce.

### Motivation 🤔

Based on our own struggles in applying for internships, we identified a lack of transparency in the job market. To address this, we intend to create a data pipeline that will gather information from multiple job portals, consolidate it, and present pertinent insights through a dashboard. This dashboard will offer job seekers valuable information about the current job market, including up-to-date trends, expected salaries, and job sentiment, which can help guide their decision-making process.

Furthermore, the dashboard will also be valuable for the HR industry by providing a better understanding of the competitive landscape. This will enable HR professionals to make better-informed decisions about benefit packages and recruitment strategies.

### Pipeline 🏗

![image](https://user-images.githubusercontent.com/74229021/233560069-440680df-f57b-43b7-8a03-5531659a0acd.png)

Our pipeline scrapes data from job portals (Glassdoor, Jobstreet, Indeed), analyses it using sentiment analysis and Pandas, and uploads to MySQL. Automation is achieved with Airflow and Google Cloud Composer, with Tableau for visualisation. 

Here's the link to our report on this project: [IS3107 Report.pdf](https://github.com/charlenechanmy/is3107-job-analytics-dashboard/files/12503355/IS3107.Report.pdf)


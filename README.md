# indeed_project
## This project is about the scraping of indeed jobs for some professions in certain countries and analysing the data for most required skills, the locations with higher number of oppertunities, the frequency of posting of the job, and more.
### the project consist of 2 main parts:
#### 1ST is the collecton of the data -this is done in 2 stages- 
> **First the initial scraping of the posted jobs**: this was the first step i scraped all the data i could (the intial scraping had about 90K job entries) and inserted that data into a Google Cloud Platform SQL Database but after a week they started charging me money so i switched to an Oracle free teir Database you will find me use it in the daily scrapper script. 
</br>

> **Second is the daily scraping of the newly posted data** which is script schaduled to run daily on a heroku server then sends a daily confirmation email with the updates to the database. <a><img src="https://i.ibb.co/0rdDwZJ/oracledb-updata-confirmation-mail.png"/></a>
#### 2ND is the visualization of the data -this is done in 2 ways:
  > **First Visualization is with a dash webapp** which is python webapp build with plotly and dash and also deployed to heroku : [Check It Out Here](https://indeed-jobs-analysis.herokuapp.com/).
  <a><img src="https://i.ibb.co/p0HpjXw/Indeed-Jobs-Analysis-desktop.png"/></a>
</br>

  > **Second Visualization is with PowerBI** : [Check It Out Here](https://app.powerbi.com/view?r=eyJrIjoiNDdhYzFlNmEtN2NkOS00NjA2LWE0ZDAtNWI2OWE5MzllNmZhIiwidCI6ImJmZTEyNDVlLTA2YWItNGQ0Yi1hMzc2LThkMDgwMzJjN2EyMCJ9&pageName=ReportSection).
<a><img src="https://i.ibb.co/hBBxGJT/Power-BI-Report.png"/></a>
</br>

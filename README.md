# Movies-ETL
Amazing Prime Video, the worlds largest streaming video service would like to develop an algorithm to determine which low buidget movie will become popular. The company will host a hackathon of teams of developers to produce the algorithm. Our mission is to prepare the dataset the developers will use during the event and prodcue an ETL pipeline to update the data daily. 

## Entity Relationship Diagram
![Movies Database ERD](./movies-etl.png)

## resources
* ![Wikipedia Data](https://courses.bootcampspot.com/courses/137/files/14478/download?wrap=1)
* ![Kaggle Data](https://www.kaggle.com/account/login?returnUrl=%2Frounakbanik%2Fthe-movies-dataset%2Fdata)
* log file
extract the Wikipedia and Kaggle data from their respective files, transform the datasets by cleaning them up and joining them together, and load the cleaned dataset into a SQL database.


## Data Assumpitons
* Data from all sources remains in the same format
* Kaggle data is more complete than wiki, filled in kaggle with Wiki
* Movies in the Adult category will not be use in the hackaton
* user will create a local instance of Postgres with a database named:
* data files will be downloaded to the runtime directory prior to execution


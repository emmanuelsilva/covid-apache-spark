# Apache Spark COVID analyzer

Simple Spark + Scala application to analyze a public dataset which contains data from COVID around the world. 
You can find the dataset at this repository: https://github.com/owid/covid-19-data/tree/master/public/data

## Goal

Introduce how to use the Apache Spark libraries to create an ETL analyzer by grouping the dataframe per Continent.

* E -> Extract the data from the raw data frame (CSV file).
* T -> Clear the data and group the data by Continent.
* L -> Save the grouped data into a another one CSV file.

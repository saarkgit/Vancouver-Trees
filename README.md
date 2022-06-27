# Java-Mongo Project

This is a personal project I worked on to review and improve my backend and database skills. For this project I used the 'Street Trees' data from City of Vancouver [Open Data](https://opendata.vancouver.ca/pages/home/) that has

<p>&nbsp "a listing of public trees on boulevards in the City of Vancouver and provides data on tree coordinates, species and other related characteristics." </p> 

With the tree data I put it in a mongoDB database and then applied the ETL (Extract, Transform, Load) process to the data by moving up the sub-document with the important fields as primary keys and generated a `friendly_names`, key to the data using info from relevant values...
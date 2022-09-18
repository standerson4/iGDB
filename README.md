# iGDB

## About
This repository contains the code and instructions to create the iGDB database initially described in:

Scott Anderson, Loqman Salamatian, Zachary S. Bischof, Alberto Dainotti, and Paul Barford. 2022.
*iGDB: Connecting the Physical and Logical Layers of the Internet.*
In ACM Internet Measurement Conference (IMC ’22), October 25–27, 2022, Nice, France. ACM, New York, NY, USA, 16 pages.

Please reference the manuscript for a full description of the research that led to the creation of iGDB.

## The script iGDB.py is designed to accomplish these tasks
* Collect (update) raw Internet topology data for input
* Process raw Internet topology data into csv files that can be loaded into a database
* Create a database of physical and logical Internet topology information
* Query the database (example query listed below)

## Quickstart
* The entry point for all tasks is code/iGDB.py
* Run all code from the *code* directory
* *python3 iGDB.py* will display a complete help menu
* Existing processed data is included in the repo
	- You may create a new version of the DB from the existing processed data
	- You may query the DB after creating it from the processed data
* All of the unprocessed data is included in the .gitignore file and NOT in the repo
	- Therefore, you may run the script in this order to locally collect the raw data:
	- iGDB.py -u <LOCATION>
	- iGDB.py -p
	- iGDB.py -c database_name.db
	- iGDB.py -q "SELECT * FROM asn_loc LIMIT 10;"
* The SQLite database is created in the *database* folder and may be viewed using your database viewer of choice.
* The SQLite database may be dumped and loaded into PostgreSQL for use with a Geographic Information System (GIS), such as ArcGIS.
  - All visualizations in the manuscript were created using ArcGIS.

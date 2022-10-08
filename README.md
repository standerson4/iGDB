# iGDB

## About
This repository contains the code and instructions to create the iGDB database initially described in:

Scott Anderson, Loqman Salamatian, Zachary S. Bischof, Alberto Dainotti, and Paul Barford. 2022.
*iGDB: Connecting the Physical and Logical Layers of the Internet.*
In ACM Internet Measurement Conference (IMC ’22), October 25–27, 2022, Nice, France. ACM, New York, NY, USA, 16 pages.

Please cite the manuscript when using iGDB to enable future research. Please reference the manuscript for a full description of the research that led to the creation of iGDB.

## The script iGDB.py is designed to accomplish these tasks
* Collect (update) raw Internet topology data
* Process raw Internet topology data into csv files that can be loaded into a database
* Create a database of physical and logical Internet topology information
* Query the database (example query listed below)

## Quickstart
* iGDB is written in Python3 (version 3.8 or higher).
* The entry point for all tasks is code/iGDB.py
* Run all code from the *code* directory.
* From the *code* directory, run *python3 iGDB.py* to display a complete help menu.
* Existing processed data is included in the repo.
	- You may create a new version of the DB from the existing processed data, by running *python3 iGDB.py -c database_name.db*
	- You may query the DB after creating it from the processed data, by running *python3 iGDB.py -q "<SQL QUERY>"*
* All of the unprocessed data is included in the .gitignore file and therefore NOT in the repo.
	- Therefore, you may run the script in this order to locally collect the raw data:
	- iGDB.py -u <LOCATION>
	- iGDB.py -p
	- iGDB.py -c database_name.db
	- iGDB.py -q "SELECT * FROM asn_loc LIMIT 10;"
* The SQLite database is created in the *database* folder and may be viewed using your database viewer of choice.
* The SQLite database may be dumped and loaded into a PostgreSQL spatial database for use with a Geographic Information System (GIS), such as ArcGIS.
  - All visualizations in the manuscript were created using ArcGIS.
* Reference the help menu by running *python3 iGDB.py* to display a complete list of options.

## Example SQL queries
* To determine the number of ASNs in Atlanta, GA:
  - python3 iGDB.py -q 'SELECT COUNT(\*) FROM asn_loc al WHERE al.standard_city == "Atlanta" AND al.standard_state == "Georgia" AND al.source == "PeeringDB";'
* To determine the location, ASN, and RDNS of an IP address:
  - python3 iGDB.py -q 'SELECT iad.ip_addr, iad.rdns, iad.asn, c.city_name, c.state_province, c.country_code, c.city_latitude, c.city_longitude FROM ip_asn_dns iad, city_points c WHERE iad.ip_addr == "37.49.232.7" AND c.city_name == iad.standard_city AND c.state_province == iad.standard_state AND c.country_code == iad.standard_country;'

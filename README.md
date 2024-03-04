# Integrating-Consumer-Data-into-SMART360
A python script to extract consumer data from ABC utility company’s databases assuming that they are using MySQL databases. The script will map, transform and load the extracted data from the MySQL database to its corresponding SMART360 platform, the script will automatically run at the scheduled time 8 am.

Make sure you have downloaded: 
1.MySQL pip instpip install mysqlx-connector-pythonall mysqlx-connector-python
2.Requests pip install requests
3.Schedule pip install schedule

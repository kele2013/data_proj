
1.The first is a performance appraisal dashboard. The front end scrapes data into an Excel spreadsheet, the script cleans the data from the Excel spreadsheet, and then imports it into Starrocks via streamload.

2.The second one synchronizes from MySQL to Starrocks via DataX.
3.The third is a plugin for the NiFi open-source project that uses streamload to import NiFi data streams into Starrocks in real time. Stream Load is a commonly used streaming data import method in StarRocks real-time data warehouses.

1.After all the data was imported into Starrocks via NiFi, it was
modeled hierarchically by topic. The DWD layer consists of wide tables
for accounts receivable and actual receipts, respectively, while the
DWS layer contains atomic correlation indicators generated daily.

2.For some real-time metrics, data is directly written to StarRocks after being correlated with dimension tables using Flink.

3.Scheduling is performed via Airflow, with minute-level and day-level scheduling, and real-time monitoring of the scheduling scripts sending email alerts.

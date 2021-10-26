# Project Summary
Analyze data on songs and user activity on a music streaming app to understand what music users are listening to on the service. Design a database schema to facilitate the analysis. User activity and metadata on songs are recorded in separate JSON files which are kept in separate directories in an Amazon S3 bucket. Create a Redshift cluster on AWS and connect to it. Create an ETL pipeline to populate the new Redshift database by copying data from the bucket into staging tables. The database uses a star schema with a fact table and several dimension tables. The data is suitably transformed while populating the various tables.

# Execution
Clone the project and open the folder named `sparkifydb_rs`.

1. Open and run the Jupyter notebook `create_cluster.ipynb` to create a new Redshift cluster and connect to it. Alternatively, run `python create_cluster.py` in the terminal.
2. Cluster creation will take some time. Once it has been created and a connection established, the endpoint (HOST) and the Amazon Resource Name (ARN) will be printed. Copy these and fill out the relevant fields in the config file `dwh.cfg`, namely ARN and DWH_HOST.
3. In the terminal, run `python create_tables.py` to connect to the database and create tables. These include two **staging** tables (for events and songs), four dimension tables (**users**, **artists**, **songs**, **time**) and one fact table (**songplays**).
4. To insert data into the various tables, run `python etl.py`. The two staging tables will be populated first. Copying song data from the S3 bucket into the staging table for songs may take a considerable amount of time. Once the staging tables are ready, data will be inserted into the dimension and fact tables.
5. Query the database.
6. Delete the cluster by running the last cell in the notebook `create_cluster.ipynb`.

# Files
- `dwh.cfg`: Config file with various parameters.
- `sql_queries.py`: Contains all relevant queries that are used for creating tables and inserting data.
- `create_cluster.ipynb`: Creates a Redshift cluster and connect to it. Alternativel, use `create_cluster.py`.
- `create_tables.py`: Creates all tables in the database including staging tables.
- `etl.py`: Copies data from S3 bucket into staging tables. Transforms data during insertion into fact and dimension tables from staging tables.

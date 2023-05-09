# f1-exercise
This is my local solution.
here is a flow chart to describe the flow:
1. is implemented in python_code/pit-stops-producer
2. is implemented in python_code/spark/pit-stops-normalizer (dedup) + python_code/setup/config/DDL.sql (I used ksqldb for RT enrichment)
3. is implemented in python_code/spark/f1-daily-report-generator
* for the offline report I used `local-files-system` & `csv format` which will be replaced with `S3` & `parquet` in production. 
![img.png](img.png)

in order to run the solution follow those steps:
- set up env: `docker compose up -d`
- create venv: `python3 -m venv venv` 
- activate venv: `source venv/bin/activate`
- install requirements: `pip3 install -r requirements.txt`
- set env var: `export F1_ENV=local`
- set up kafka & ksaql-db:  `python3 python_code/setup/main.py`
- run pit-stops-producer: `python3 python_code/pit-stops-producer/main.py`
- run pit-stops-producer: `python3 python_code/pit-stops-producer/main.py`
- run pit-stops-normalizer: `python3 python_code/spark/pit-stops-normalizer/main.py` -- 
  - results will be saved to ksqldb where the RT enrichment will happen, to check the results use:
    - connect to ksql-db: `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`
    - run the following query: `SELECT * FROM enriched_drivers_table EMIT CHANGES;` 
- run f1-daily-report-generator- `python3 python_code/spark/f1-daily-report-generator/main.py`- 
  - the output will be saved to [f1-daily-report](local_data%2Foutput%2Ff1-daily-report)

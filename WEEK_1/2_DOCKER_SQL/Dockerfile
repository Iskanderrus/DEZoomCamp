FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app

COPY csv_ingest_data.py csv_ingest_data.py

ENTRYPOINT [ "python", "csv_ingest_data.py" ]
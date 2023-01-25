FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY engest_data.py engest_data.py

ENTRYPOINT ["python", "engest_data.py"]
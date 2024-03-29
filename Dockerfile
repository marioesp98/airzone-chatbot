# Dockerfile for building the image used in the Amazon EC2 instance

FROM python:3.11

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY ../.. .

CMD [ "python", "main_scraper.py"]
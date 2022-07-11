FROM python:3.8.13-slim-bullseye
ENV DIR=app/files
RUN mkdir -p ${DIR}
COPY crawler.py requirements.txt app/
RUN pip install -r app/requirements.txt
CMD python app/crawler.py

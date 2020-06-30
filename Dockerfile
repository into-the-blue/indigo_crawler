FROM python:3.7


COPY ./requirements.txt .


RUN pip install -r requirements.txt


RUN rm requirements.txt

RUN mkdir -p /apps/crawler

WORKDIR /apps/crawler

COPY ./src .


EXPOSE 5001
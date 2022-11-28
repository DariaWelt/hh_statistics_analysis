# build from parent dir
FROM python:3.8

WORKDIR /code

COPY . ./hh_analyzer

RUN mkdir /log
RUN pip install -r ./hh_analyzer/ExtractorService/requirements.txt
CMD bash ./hh_analyzer/ExtractorService/run.sh
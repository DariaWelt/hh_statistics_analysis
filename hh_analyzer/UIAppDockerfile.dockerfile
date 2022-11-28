# build from parent dir
FROM python:3.8

WORKDIR /code

COPY . ./hh_analyzer

RUN mkdir /log
RUN pip install -r ./hh_analyzer/UserApp/requirements.txt

CMD python -m hh_analyzer.UserApp.UserInterfaceService
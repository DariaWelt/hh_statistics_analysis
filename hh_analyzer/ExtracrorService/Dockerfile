FROM python:3.8

WORKDIR /code

COPY requirements.txt .

RUN pip install -r requirements

COPY ../ExtracrorService .
COPY ../ServisesUtils .

RUN nohup python -m ExtractorService.LazyExtractor -w 5  > /log/lazy_extractor.log &
RUN python -m ExtractorService.MonthExtractor -w 5 > /log/month_extractor.log &
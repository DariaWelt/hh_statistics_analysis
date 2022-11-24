import typing as tp
from pyspark import sql

import handlers as hls
import filtration as flt

class ProcSession:

    def __init__(self, db_url: str = "mongodb://127.0.0.1:9200", 
        db_name: str = "hh_vacations",
        collection: str = "hh_vacancies_RAW") -> None:
        
        self.spark = sql.SparkSession \
        .builder \
        .appName("handler") \
        .config("spark.mongodb.read.connection.uri", f"{db_url}/{db_name}.{collection}") \
        .config("spark.mongodb.write.connection.uri", f"{db_url}/{db_name}.{collection}") \
        .config(f"spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.4") \
        .config('spark.submit.pyFiles', './m.zip')\
        .getOrCreate()

        # load data
        self.df = self.spark.read.format("mongodb").load()

    


class ProcPipLine:
    
    text_cols = ["snippet.requirement", "snippet.responsibility"]
    desc_col = "desc_col"

    def __init__(self, 
            session: ProcSession = None,
            mfilter: flt.FilterBase = None, 
            handlers: tp.Dict[str, hls.HandlerBase] = {}) -> None:
        
        self.session = session
        self.mfilter = mfilter
        self.handlers = handlers

    def set_filter(self, mfilter: flt.FilterBase):
        self.mfilter = mfilter

    def set_session(self, session: ProcSession):
        self.session = session

    def add_handler(self, name: str, handler: hls.HandlerBase):
        self.handlers[name] = handler

    def run(self, sentences: tp.List[str]):
        df = self.session.df
        filt_df = self.mfilter.transform(df, ProcPipLine.text_cols, ProcPipLine.desc_col)

        out_stat = []
        for name, handler in self.handlers.items():
            res = handler.proc(filt_df, ProcPipLine.desc_col, sentences)
            res["title"] = name
            out_stat.append(res)
        
        return {"data": out_stat} 



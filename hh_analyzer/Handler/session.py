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
        .config('spark.submit.pyFiles', '/opt/spark/work-dir/app.zip')\
        .getOrCreate()

        # load data
        self.df = self.spark.read.format("mongodb").load()

    


class ProcPipLine:
    sal_col = "salary"
    text_col = "description"
    desc_col = "desc_col"

    def __init__(self, 
            session: ProcSession,
            mfilter: flt.FilterBase, 
            handlers: tp.Dict[str, hls.HandlerBase] = {}) -> None:
        
        self.session = session
        self.mfilter = mfilter
        self.handlers = handlers

        df = self.session.df.select([self.sal_col, self.text_col])
        self.filt_df = self.mfilter.transform(df, ProcPipLine.text_col, ProcPipLine.desc_col)



    def set_filter(self, mfilter: flt.FilterBase):
        self.mfilter = mfilter

    def set_session(self, session: ProcSession):
        self.session = session

    def add_handler(self, name: str, handler: hls.HandlerBase):
        self.handlers[name] = handler

    def run(self, sentences: tp.List[str]):
        
        out_stat = []
        for name, handler in self.handlers.items():
            res = handler.proc(self.filt_df, ProcPipLine.desc_col, sentences)
            res["title"] = name
            out_stat.append(res)
        
        return {"data": out_stat} 



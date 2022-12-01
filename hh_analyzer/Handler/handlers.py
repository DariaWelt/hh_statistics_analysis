import typing as tp
from abc import abstractmethod
from pyspark import sql
from pyspark.sql import functions as sqlF, types as stp
import re

class HandlerBase:

    @abstractmethod
    def proc(self, df: sql.DataFrame, inputCol: str, **kwargs) -> tp.Dict:
        pass


class TechCount(HandlerBase):

    word_count_col: str = "word_count"

    def __init__(self) -> None:
        super().__init__()

        self.spliter = re.compile("[^a-zA-Zа-яА-Я0-9+]")

    def proc(self, df: sql.DataFrame, 
            inputCol: str, 
            sentences: tp.List[str], **kwargs) -> tp.Dict:

        out_stat = []
        df_count = df.count()

        for sentence in sentences:
            wcount_df = self._get_wcdf(df, inputCol, sentence)
            out_stat.append({
                "name": sentence,
                "value": wcount_df.count() / df_count * 100
            })

        return {"units": "%", "technology_data": out_stat}

    def _get_wcdf(self, df: sql.DataFrame, inputCol: str,  sentence: str) -> sql.DataFrame:
        """
        Get word count data frame
        """
        def is_sublist(l: list, subl: list):
            if len(subl) == 0:
                return True
            
            if not subl[0] in l:
                return False
                
            i = l.index(subl[0])
            if len(l) - i <= len(subl):
                return False

            return l[i:i+len(subl)] == subl
            
        clean_sentence = [sent for sent in self.spliter.split(sentence.lower()) if sent != ""]
        cout_df = df.withColumn(TechCount.word_count_col, 
            sqlF.udf(lambda s: int(is_sublist(s, clean_sentence)), 
            stp.IntegerType())(sqlF.col(inputCol)))

        return cout_df\
            .filter(cout_df[TechCount.word_count_col] > 0)\
            .drop(TechCount.word_count_col)


class TechCorr(TechCount):

    def __init__(self) -> None:
        super().__init__()

    def proc(self, df: sql.DataFrame, 
            inputCol: str, 
            sentences: tp.List[str], **kwargs) -> tp.Dict:
        out_stat = []

        for sent in sentences:
            out_stat.append({
                    "name": sent,
                    "value": {}
                })

        for i, sent1 in enumerate(sentences[:-1]):
            for j, sent2 in enumerate(sentences[i+1:], i+1):
                
                wcount_df1 = self._get_wcdf(df, inputCol, sent1)
                wcount_df2 = self._get_wcdf(df, inputCol, sent2)
                gen_wc_df = wcount_df1.intersect(wcount_df2)
                
                cond_prob = lambda x, y: x / y if y != 0 else 0
                out_stat[i]["value"][sent2] = cond_prob(gen_wc_df.count(), wcount_df1.count())
                out_stat[j]["value"][sent1] = cond_prob(gen_wc_df.count(), wcount_df2.count())

        return {"units": "", "technology_data": out_stat}

class MeanSalaryByTech(TechCount):

    currences: tp.Dict = {
    "USD": 60.,
    "EUR": 63.,
    "KZT": 0.13,
    "UZS": 0.0054,
    "KGS": 0.72,
    "BYR": 24.17
}

    def __init__(self) -> None:
        super().__init__()

    def proc(self, df: sql.DataFrame, 
            inputCol: str, 
            sentences: tp.List[str], 
            salCol: str = "salary", **kwargs) -> tp.Dict:
        
        out_stat = []

        for sent in sentences:
            wcount_df = self._get_wcdf(df, inputCol, sent)
            mean_salary = self._get_mean_sal(wcount_df, salCol)

            out_stat.append({
                "name": sent,
                "value": mean_salary
            })

        return  {"units": ".руб", "technology_data": out_stat}

    def _get_mean_sal(self, df: sql.DataFrame, inputCol: str) -> float:
        
        @sqlF.udf(stp.DoubleType())
        def get_salary(sal):
            coef = 0.87 if sal["gross"] else 1.
            coef *= MeanSalaryByTech.currences.get(sal["currency"], 1.)
            
            if sal["from"] is None:
                res = sal["to"] * coef
            elif sal["to"] is None:
                res = sal["from"] * coef
            else:
                res = (sal["from"] + sal["to"]) / 2 * coef

            return res
        
        clean_df = df.na.drop(subset=[inputCol, f"{inputCol}.currency"])\
            .na.drop(subset=[f"{inputCol}.from", f"{inputCol}.to"])
        clean_df = clean_df.withColumn(f"mean_{inputCol}", 
            get_salary(clean_df[inputCol]))
        col_mean = clean_df.select(sqlF.avg(f"mean_{inputCol}").alias("mean")).collect()[0]["mean"]

        return col_mean
        

class TechInfluence(MeanSalaryByTech):

    def __init__(self) -> None:
        super().__init__()

    
    def proc(self, df: sql.DataFrame, 
            inputCol: str, 
            sentences: tp.List[str], 
            salCol: str = "salary", **kwargs) -> tp.Dict:
        
        out_stat = []

        for sent in sentences:
            wcount_df = self._get_wcdf(df, inputCol, sent)
            nowcount_df = df.subtract(wcount_df)
            wmean_salary = self._get_mean_sal(wcount_df, salCol)
            nowmean_salary = self._get_mean_sal(nowcount_df, salCol)

            if wmean_salary is None or nowmean_salary is None:
                rate = None
            else:
                rate = nowmean_salary - wmean_salary

            out_stat.append({
                "name": sent,
                "value": rate
            }) 
        
        return  {"units": "", "technology_data": out_stat}







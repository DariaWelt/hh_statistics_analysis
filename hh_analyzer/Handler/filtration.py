import typing as tp
from abc import abstractmethod
from pyspark import sql
from pyspark.sql import functions as sqlF
from pyspark.ml import feature as mlf
from pyspark.sql import functions as sqlF, types as stp
import bs4



class FilterBase:

    @abstractmethod
    def transform(self, df: sql.DataFrame, **kwargs) -> sql.DataFrame:
        pass

    @abstractmethod
    def get_target_col():
        pass


class StringFilter(FilterBase):

    def __init__(self) -> None:
        super().__init__()

        self.tkzer = mlf.RegexTokenizer(
            pattern="[^a-zA-Zа-яА-Я0-9+]")
        
        self.sw_remover = mlf.StopWordsRemover(
            stopWords=mlf.StopWordsRemover.loadDefaultStopWords("russian")
        )
    
    def transform(self, df: sql.DataFrame, inputCol: str, outputCol: str, **kwargs) -> sql.DataFrame:
        
        tkz_col = "tkz_col"
        self.tkzer.setInputCol(inputCol)
        self.tkzer.setOutputCol(tkz_col)
        self.sw_remover.setInputCol(tkz_col)
        self.sw_remover.setOutputCol(outputCol)
        
        out_df = self.tkzer.transform(df)
        out_df = self.sw_remover.transform(out_df).drop(tkz_col)

        return out_df


class GeneralFilter(StringFilter):

    

    def __init__(self) -> None:
        super().__init__()

        @sqlF.udf(stp.StringType())
        def html_parser(html_desc: str) -> str:
            soup = bs4.BeautifulSoup(html_desc, "html.parser")
            
            return soup.get_text()

        self.html_parser = html_parser


    def transform(self, df: sql.DataFrame, inputCol: str, outputCol: str, **kwargs) -> sql.DataFrame:

        out_df = df.na.drop(subset=[inputCol])

        text_general_col = "text_general_col"
        out_df = out_df.select(
            self.html_parser(inputCol).alias(text_general_col),
            *out_df.columns
        )

        out_df = super().transform(out_df, 
            inputCol=text_general_col,
            outputCol=outputCol).drop(text_general_col)

        return out_df


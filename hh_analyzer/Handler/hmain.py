import os
import typing as tp
import kafka
import json

import session
import handlers
import filtration


def pipline_init(params: tp.Dict) -> session.ProcPipLine:
    msession = session.ProcSession(
        db_url=params["DB_URL"],
        db_name=params["DB_NAME"],
        collection=["DB_COLLECTION"]
    )
    mpipline = session.ProcPipLine()

    mpipline.set_session(msession)
    mpipline.set_filter(filtration.GeneralFilter())

    mpipline.add_handler("Доля вакансий с технологией", 
        handlers.TechCount())
    mpipline.add_handler("Корреляция технологий", 
        handlers.TechCorr())
    mpipline.add_handler("Средняя зарплата с технологией", 
        handlers.MeanSalaryByTech())
    mpipline.add_handler("Влияние технологии на зарплату", 
        handlers.TechInfluence())
    
    return mpipline

def kafka_init(params: tp.Dict):

    concumer = kafka.KafkaConsumer(
        bootstrap_servers=[params["KAFKA_DOM"]],
        value_serializer=lambda x:
        json.loads(x))

    producer = kafka.KafkaProducer(
        os.environ["SEND_TOPIC"],
        bootstrap_servers=[params["KAFKA_DOM"]],
        value_serializer=lambda x:
        json.dumps(x).encode('utf-8'))

    return producer, concumer

def proc_loop(mpipline: session.ProcPipLine, 
            producer: kafka.KafkaProducer, 
            concumer: kafka.KafkaConsumer, 
            p_topic: str):
    
    while(True):
        msg = next(concumer)
        in_data = msg.get("data")
        out_data = mpipline.run(in_data)
        producer.send(p_topic, value=out_data)


def main():
    mpipline = pipline_init(os.environ)
    
    p_topic = os.environ["RESP_SEND_TOPIC"]
    producer, concumer = kafka_init(os.environ)

    proc_loop(mpipline, producer, concumer, p_topic)



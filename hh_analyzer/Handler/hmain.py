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
        collection=params["DB_COLLECTION"]
    )
    mpipline = session.ProcPipLine(msession, filtration.GeneralFilter())

    mpipline.set_session(msession)

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
        (params["SEND_TOPIC"]),
        bootstrap_servers=[params["KAFKA_DOM"]],
        value_deserializer=lambda x:
        json.loads(x))
    #concumer.subscribe([params["SEND_TOPIC"]])

    producer = kafka.KafkaProducer(
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
        in_data = msg.value.get("data")
        out_data = mpipline.run(in_data)
        producer.send(p_topic, value=out_data)


def main():
    mpipline = pipline_init(os.environ)
    
    p_topic = os.environ["RESP_SEND_TOPIC"]
    producer, concumer = kafka_init(os.environ)

    proc_loop(mpipline, producer, concumer, p_topic)

if __name__ == "__main__":
    main()


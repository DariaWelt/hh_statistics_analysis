import asyncio
import os
from argparse import Namespace, ArgumentParser
from asyncio import gather
from datetime import datetime, timedelta
from math import ceil
from os import environ as env
from typing import List, Dict

import aiohttp
from pymongo import MongoClient
from pymongo.database import Database
from kafka import KafkaProducer, KafkaConsumer

from hh_analyzer.ServisesUtils import DB_URI_STR, DB_NAME_STR, EXTRACTOR_THEME_STR, KAFKA_PORT_STR
from hh_analyzer.ServisesUtils.service_core import HHService
from hh_analyzer.ServisesUtils.utils import load_config, matched_specializations


class MonthExtractor(HHService):
    _mongodb: Database
    _collection_name: str = 'hh_vacancies_RAW'
    _sourceUrl: str = 'https://api.hh.ru/vacancies'
    _workers: int
    _kafka_theme: str
    _kafka_port: str

    def __init__(self):
        super(MonthExtractor, self).__init__('hh_month_extractor')
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._kafka_port, api_version=(0, 10))
        self._kafka_consumer = KafkaConsumer(self._kafka_theme, bootstrap_servers=self._kafka_port,
                                             api_version=(0, 10))

    def parse_args(self):
        namespace = super(MonthExtractor, self).parse_args()
        self._workers = max(1, namespace.workers)

    def run(self):
        asyncio.run(self.listen_messages())

    async def listen_messages(self):
        t = 3
        self._logger.info(f"listen to topic '{self._kafka_theme}'")
        last_updated = datetime.now() - timedelta(hours=12)
        for message in self._kafka_consumer:
            self._logger.info(f"got {message.topic}: message '{message.value}'")

            if datetime.now() - last_updated <= timedelta(minutes=5):
                self._logger.info("fresh data is already stored in database, skipping extraction")
                self._kafka_producer.send(f'resp_{self._kafka_theme}', b'ok_' + message.value)
                break

            for i in range(t):
                try:
                    await asyncio.create_task(self._extract_monthly_records())
                    self._kafka_producer.send(f'resp_{self._kafka_theme}', b'ok_' + message.value)
                    self._logger.info("data is updated, ok response sent")
                    break
                except Exception as err:
                    if i + 1 < t:
                        self._logger.warning(f"got exception '{err}' during run. retrying...({i + 1}/{t})")
                    else:
                        self._logger.error(f"run failed: got exception '{err}'")
                        self._kafka_producer.send(f'resp_{self._kafka_theme}', b'failed_' + message.value)
                    await asyncio.sleep(10)
            last_updated = datetime.now()

    async def _extract_monthly_records(self):
        ids = await self._get_specialities_ids()
        spec_num = len(ids)
        batch_size = ceil(len(ids) / self._workers)
        batches = [[
            ids[j] for j in range(i * batch_size, (i + 1) * batch_size) if j < spec_num
            ] for i in range(self._workers) if i * batch_size < spec_num
        ]
        await gather(*(self._extract_speciality_records(ids) for ids in batches))

    def _check_vacancy_exist(self, vac_id) -> bool:
        records = self._mongodb[self._collection_name].find({"id": vac_id})
        try:
            records.next()
            return True
        except:
            return False

    @staticmethod
    async def _get_request(url: str, params: Dict) -> Dict:
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(url, params=params) as resp:
                if resp.ok:
                    return await resp.json()
        return {}

    async def _extract_speciality_records(self, speciality_ids: List[str]):
        timestamp = datetime.now()
        time_readed = timestamp
        interval = timedelta(hours=12)
        for speciality_id in speciality_ids:
            time_readed = timestamp
            while time_readed > timestamp - timedelta(days=31):
                params = {
                    "specialization": speciality_id,
                    "date_to": time_readed.strftime('%Y-%m-%dT%H:%M:%S'),
                    "date_from": (time_readed - interval).strftime('%Y-%m-%dT%H:%M:%S')
                }
                while True:
                    data = await self._get_request(self._sourceUrl, params)
                    if not data:
                        break
                    to_add = list(filter(lambda vac: (not self._check_vacancy_exist(vac["id"])), data["items"]))
                    if len(to_add) > 0:
                        self._mongodb[self._collection_name].insert_many(to_add)
                        self._logger.info(f"added {len(to_add)} vacancies")

                    params["page"] = data["page"] + 1
                    if data["pages"] - data["page"] <= 1:
                        break
                    await asyncio.sleep(10)
                time_readed = time_readed - interval

    async def _get_specialities_ids(self) -> List[str]:
        result = []
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get('https://api.hh.ru/specializations') as resp:
                if not resp.ok:
                    return result

                data = await resp.json()
                for spec in data:
                    matched = filter(lambda subspec: (matched_specializations([{"name": spec["name"],
                                                                                "profarea_name": subspec["name"]}])),
                                     spec["specializations"])
                    result.extend(subspec["id"] for subspec in list(matched))
        return result

    def _get_parser(self) -> ArgumentParser:
        parser = super(MonthExtractor, self)._get_parser()
        parser.add_argument('-w', '--workers', type=int, default=1,
                            help="Number of processes in which vacancies for last month will be loaded")
        parser.add_argument('-k', '--kafka-producer', type=str, required=False,
                            help="Uri for kafka producer")
        parser.add_argument('-t', '--topic', type=str, default='Extract',
                            help="Name of topic from which extractor should listen messages")
        return parser

    def _validate_parsed_args(self, args: Namespace):
        super(MonthExtractor, self)._validate_parsed_args(args)

    def _configure(self):
        if self._config_path:
            config_data = load_config(self._config_path)
            mongodb_uri = config_data['mongodb_uri']
            database_name = config_data['hh_vac_database']
        else:
            mongodb_uri = os.getenv(DB_URI_STR)
            database_name = env[DB_NAME_STR]

        self._kafka_theme = env[EXTRACTOR_THEME_STR]
        self._kafka_port = os.getenv(KAFKA_PORT_STR)
        self._mongodb = MongoClient(mongodb_uri)[database_name]


if __name__ == '__main__':
    extractor = MonthExtractor()
    extractor.run()

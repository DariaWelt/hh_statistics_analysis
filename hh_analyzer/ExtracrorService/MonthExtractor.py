import asyncio
from argparse import Namespace, ArgumentParser
from asyncio import BaseEventLoop, get_event_loop, gather, new_event_loop, set_event_loop, create_task
from datetime import datetime, timedelta
from math import ceil
from typing import List, Dict

import aiohttp
from pymongo import MongoClient
from pymongo.database import Database

from hh_analyzer.service_core import HHService
from hh_analyzer.utils import load_config, matched_specializations


class MonthExtractor(HHService):
    _mongodb: Database
    _collection_name: str = 'hh_vacancies_RAW'
    _sourceUrl: str = 'https://api.hh.ru/vacancies'
    _workers: int

    def __init__(self):
        super(MonthExtractor, self).__init__('hh_month_extractor')

    # TODO: add kafka parameters setting
    def parse_args(self):
        namespace = super(MonthExtractor, self).parse_args()
        self._workers = max(1, namespace.workers)

    #TODO: add listening to kafka producer
    def run(self):
        asyncio.run(self._extract_monthly_records())

    async def _extract_monthly_records(self):
        ids = await self._get_specialities_ids()
        batch_size = ceil(len(ids) / self._workers)
        batches = [[ids[j] for j in range(i * batch_size, (i + 1) * batch_size)] for i in range(self._workers)]
        await gather(*(self._extract_speciality_records(ids) for ids in batches))

    def _check_vacation_exist(self, vac_id) -> bool:
        records = self._mongodb[self._collection_name].find({"id": vac_id})
        try:
            records.next()
            return True
        except:
            return False

    @staticmethod
    async def _get_request(url: str, params: Dict) -> Dict:
        async with aiohttp.ClientSession() as session:
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
                    to_add = list(filter(lambda vac: (not self._check_vacation_exist(vac["id"])), data["items"]))
                    if len(to_add) > 0:
                        self._mongodb[self._collection_name].insert_many(to_add)
                        print(f"added {len(to_add)} vacancies")

                    params["page"] = data["page"] + 1
                    if data["pages"] - data["page"] <= 1:
                        break
                time_readed = time_readed - interval

    async def _get_specialities_ids(self) -> List[str]:
        result = []
        async with aiohttp.ClientSession() as session:
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
        config_data = load_config(self._config_path)
        mongodb_uri = config_data['mongodb_uri']
        database_name = config_data['extractors_database']
        self._mongodb = MongoClient(mongodb_uri)[database_name]


if __name__ == '__main__':
    extractor = MonthExtractor()
    extractor.run()
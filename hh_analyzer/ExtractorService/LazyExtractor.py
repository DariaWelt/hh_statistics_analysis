import asyncio
import os.path
from argparse import Namespace, ArgumentParser
from asyncio import BaseEventLoop, get_event_loop, gather
from datetime import datetime, timedelta
from os import environ as env
from typing import Iterable, List

import aiohttp as aiohttp
from pymongo import MongoClient
from pymongo.database import Database

from ..ServisesUtils import DB_NAME_STR, DB_URI_STR
from ..ServisesUtils.service_core import HHService
from ..ServisesUtils.utils import load_config, matched_specializations


class LazyExtractor(HHService):
    _workers: int
    _mongodb: Database
    _current_id_to_read: int
    _requests_counter: int
    _requests_tick: datetime
    _collection_name: str = 'hh_vacancies_RAW'
    _sourceUrl: str = 'https://api.hh.ru/vacancies'
    _state_data_path: str = "extractor_state"
    _batch_size = 10
    _max_vac_id = (1 << 32) - 1

    def __init__(self):
        super(LazyExtractor, self).__init__('hh_lazy_extractor')
        LazyExtractor._requests_counter = 0
        LazyExtractor._requests_tick = datetime.now()

    def parse_args(self):
        namespace = super(LazyExtractor, self).parse_args()
        self._workers = max(1, namespace.workers)

    def run(self):
        async def process_batch(ids: List[int]):
            for i in ids:
                processed = False
                while not processed:
                    try:
                        await self._extract_vacancy(i)
                        processed = True
                    except Exception as err:
                        self._logger.warning(f"exception occured: {err}")
                        await asyncio.sleep(2)
            id = 0
            try:
                with open(self._state_data_path, 'r') as f:
                    id = int(f.readline().replace('\n', ''))
            except:
                pass
            if self._current_id_to_read > id:
                with open(self._state_data_path, 'w') as f:
                    f.write(str(self._current_id_to_read))

        for batch in self._get_batches():
            loop = get_event_loop()
            loop.run_until_complete(gather(*(process_batch(ids) for ids in batch)))

        with open(self._state_data_path, 'w') as f:
            f.write(str(self._current_id_to_read))

    def _get_batches(self) -> Iterable[List]:
        while self._current_id_to_read < self._max_vac_id:
            start_id = self._current_id_to_read
            batches = [[self._current_id_to_read + (self._batch_size * i + j)
                        for j in range(self._batch_size)
                        if self._current_id_to_read + (self._batch_size * i + j) < self._max_vac_id]
                       for i in range(self._workers)]
            self._current_id_to_read = min(self._current_id_to_read + self._workers * self._batch_size, self._max_vac_id)
            self._logger.info(f"prepared to process ids from {start_id} to {self._current_id_to_read}")
            yield batches

    @classmethod
    async def _do_tick(cls):
        if cls._requests_tick - datetime.now() >= timedelta(seconds=1):
            cls._requests_counter = 0
        elif cls._requests_counter > 8:
            cls._requests_counter = 0
            await asyncio.sleep(1)
            cls._requests_tick = datetime.now()

    async def _extract_vacancy(self, vacancy_id: int):
        await self._do_tick()
        LazyExtractor._requests_counter += 1
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{self._sourceUrl}/{vacancy_id}') as resp:
                if resp.ok:
                    vac = await resp.json()
                    records = self._mongodb[self._collection_name].find({"id": vac["id"]})
                    try:
                        records.next()
                        self._logger.warning(f"vacancy with id = {vacancy_id} already exists")
                        return
                    except:
                        if matched_specializations(vac.get("specializations")):
                            self._mongodb[self._collection_name].insert_one(vac)
                            self._logger.info(f"vacancy with id = {vacancy_id} was loaded")
                        else:
                            self._logger.warning(f"vacancy with id = {vacancy_id} was not loaded: specializations not matched")
                else:
                    self._logger.warning(f"no vacancy with id = {vacancy_id}")

    def _get_parser(self) -> ArgumentParser:
        parser = super(LazyExtractor, self)._get_parser()
        parser.add_argument('-w', '--workers', type=int, default=1,
                            help="Number of processes in which vacancies will be loaded in lazy mode")
        return parser

    def _validate_parsed_args(self, args: Namespace):
        super(LazyExtractor, self)._validate_parsed_args(args)

    def _configure(self):
        if self._config_path:
            config_data = load_config(self._config_path)
            mongodb_uri = config_data['mongodb_uri']
            database_name = config_data['hh_vac_database']
        else:
            mongodb_uri = env[DB_URI_STR]
            database_name = env[DB_NAME_STR]
        self._mongodb = MongoClient(mongodb_uri)[database_name]

        if os.path.exists(self._state_data_path) and os.path.isfile(self._state_data_path):
            with open(self._state_data_path, 'r') as f:
                try:
                    self._current_id_to_read = int(f.readline().replace('\n', ''))
                except:
                    self._current_id_to_read = 0
        else:
            self._current_id_to_read = 0


if __name__ == '__main__':
    extractor = LazyExtractor()
    extractor.run()

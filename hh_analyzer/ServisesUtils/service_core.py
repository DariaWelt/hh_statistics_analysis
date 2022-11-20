import logging
import os.path
from argparse import ArgumentParser, Namespace, ArgumentError
from abc import abstractmethod
from typing import Optional


class HHService:
    _logger: logging.Logger
    _config_path: Optional[str]

    def __init__(self, name: str = 'hh_service', log_dir: str = '/log'):
        self._logger = logging.Logger(name)
        fh = logging.FileHandler(f'{log_dir}/{name}.log')
        fh.setFormatter(logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s"))
        self._logger.addHandler(fh)

        self.parse_args()
        self._configure()

    def parse_args(self):
        parser = self._get_parser()
        namespace = parser.parse_args()
        self._validate_parsed_args(namespace)
        self._config_path = namespace.config
        return namespace

    def run(self):
        pass

    def _get_parser(self) -> ArgumentParser:
        parser = ArgumentParser(description='Configured hh analyzer service',
                                add_help=False)
        parser.add_argument("-h", "--help", action="help", help="Show this help message and exit")
        parser.add_argument("-c", "--config", type=str, required=False, help="Path to .json configure file for "
                                                                             "hh analyzer")
        return parser

    def _validate_parsed_args(self, args: Namespace):
        if args.config is not None and (not os.path.exists(args.config) or not os.path.isfile(args.config)):
            raise ArgumentError('Path to config file is invalid')

    @abstractmethod
    def _configure(self):
        pass

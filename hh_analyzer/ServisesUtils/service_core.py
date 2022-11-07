import logging
import os.path
from argparse import ArgumentParser, Namespace, ArgumentError
from abc import abstractmethod

from .utils import CONFIG_PATH


class HHService:
    _logger: logging.Logger
    _config_path: str

    def __init__(self, name: str = 'hh_service'):
        self._logger = logging.Logger(name)
        self.parse_args()
        self._configure()

    def parse_args(self):
        parser = self._get_parser()
        namespace = parser.parse_args()
        self._validate_parsed_args(namespace)
        self._config_path = namespace.config if namespace.config is not None else CONFIG_PATH
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

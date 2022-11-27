import json
from os import environ as env
from typing import List, Dict, Optional

import dash
from dash import html, dcc, Output, Input, State
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient

from hh_analyzer.ServisesUtils import DB_URI_STR, DB_NAME_STR, EXTRACTOR_THEME_STR, PROCESSING_THEME_STR, KAFKA_PORT_STR
from hh_analyzer.ServisesUtils.service_core import HHService
from hh_analyzer.ServisesUtils.utils import load_config
from hh_analyzer.UserApp.graphs import get_graph


class UserInterface(HHService):
    _kafka_port: str
    _extractor_kafka_theme: str
    _processing_kafka_theme: str
    _extractor_consumer: KafkaConsumer
    _processing_consumer: KafkaConsumer
    _kafka_producer: KafkaProducer

    _waiting_for_processing: bool
    _waiting_for_extractor: bool

    _collection_name: str = 'hh_vacancies_processed'
    _dash_app: dash.Dash

    _data: Dict

    def __init__(self):
        super(UserInterface, self).__init__('hh_user_interface', '../../gui_logs/')

        with open('processing_response_example.json', 'r') as f:
            self._data = json.load(f)

        self._dash_app = dash.Dash()
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._kafka_port, api_version=(0, 10))

        # consumers to listen to responses
        self._extractor_consumer = KafkaConsumer(f'resp_{self._extractor_kafka_theme}',
                                                 bootstrap_servers=self._kafka_port, auto_offset_reset='earliest',
                                                 api_version=(0, 10))
        self._processing_consumer = KafkaConsumer(f'resp_{self._processing_kafka_theme}',
                                                  bootstrap_servers=self._kafka_port, auto_offset_reset='earliest',
                                                  api_version=(0, 10))

        self.add_extractor_button()
        self.add_processor_button()

        self.add_dashboard()

    def add_extractor_button(self):
        self._dash_app.layout = html.Div([
            self._dash_app.layout,
            html.Button('Extract month data', id='extract-data',
                        style={'width': '10%', 'height': '10%', 'marginTop': 5, 'font-size': '14px'}),
        ])

        @self._dash_app.callback(
            Output('extract-data', 'n_clicks'),
            Input('extract-data', 'n_clicks')
        )
        def call_extractor(n_clicks):
            self._logger.info(f"preparing to send {self._extractor_kafka_theme}: message 'monthly'")
            self._kafka_producer.send(self._extractor_kafka_theme, b'monthly')
            self._kafka_producer.flush()
            self._logger.info(f"sent to {self._extractor_kafka_theme}: message 'monthly'")
            msg = next(self._extractor_consumer)
            self._logger.info(f"got {msg.topic}: message {msg.value}")
            return n_clicks

    def add_processor_button(self):
        self._dash_app.layout = html.Div([
            self._dash_app.layout,
            html.Br(),
            html.Div(children='List tools or technologies which you want to analyze with ";":'),
            dcc.Input(id='input-on-processing-button', type='text', placeholder='python; flask;gcc', required=True,
                      style={'width': '50%'}),
            html.Div(id='processing-button-data', style={'marginTop': 5, 'width': '50%'}),
            html.Button('Process data', id='processing-data', n_clicks=0,
                        style={'width': '10%', 'height': '10%', 'marginTop': 5, 'font-size': '14px'}),
        ])

        @self._dash_app.callback(
            Output('processing-button-data', 'children'),
            Input('input-on-processing-button', 'value')
        )
        def update_output(value):
            tools = value.split(';') if value else []
            return dcc.Dropdown(tools, tools, multi=True, disabled=True)

        @self._dash_app.callback(
            Output('dashboard', 'children'),
            Input('processing-data', 'n_clicks'),
            State('input-on-processing-button', 'value')
        )
        def call_processing(n_clicks, value):
            if value is None:
                return self.get_dashboard()

            # debug code
            consumer = KafkaConsumer(self._processing_kafka_theme, bootstrap_servers=self._kafka_port,
                                     api_version=(0, 10), auto_offset_reset='earliest')
            # end of debug code

            strings = value.split(';')
            message = {'data': strings}
            self._kafka_producer.send(self._processing_kafka_theme, json.dumps(message).encode('utf-8'))
            self._kafka_producer.flush()
            self._logger.info(f"sent to {self._processing_kafka_theme}: message '{message}'")

            # debug code
            # TODO: remove when handler will be connected
            self._logger.info(f'processing got message {next(consumer).value}')
            producer = KafkaProducer(bootstrap_servers=self._kafka_port, api_version=(0, 10))
            with open('processing_response_example.json', 'r') as f:
                self._logger.info('PUK')
                producer.send(f'resp_{self._processing_kafka_theme}', json.dumps(json.load(f)).encode('utf-8'))
            # end of debug code

            response = next(self._processing_consumer)
            self._logger.info(f"got {response.topic}: message {response.value}")
            data = json.loads(response.value)
            self._logger.info(f'got response: {data}')
            return self.get_dashboard(text_file=data)

    def add_dashboard(self):
        self._dash_app.layout = html.Div([
            self._dash_app.layout,
            html.Div(id='dashboard',
                     children=self.get_dashboard())
        ])

    def get_dashboard(self, text_file: Optional[Dict] = None) -> List[html.Div]:
        self._logger.info(f"called dash updating method..")
        res = []
        if text_file is not None:
            self._data = text_file

            self._logger.info(f"adding response text..")
            res.append(html.Div(html.Plaintext(str(text_file), style={'width': '100%'})))

        figures = []
        for i, task_data in enumerate(self._data['data']):
            try:
                figures.append(get_graph(task_data))
            except:
                figures.append(html.Div(html.Plaintext(str(self._data['data']))))
            if i % 2 == 1:
                res.append(html.Div(className='row', children=[figures[i-1], figures[i]]))
        if len(figures) % 2 == 1:
            res.append(figures[-1])
        return res

    def _configure(self):
        if self._config_path:
            config_data = load_config(self._config_path)
            mongodb_uri = config_data['mongodb_uri']
            database_name = config_data['hh_vac_database']
        else:
            mongodb_uri = env[DB_URI_STR]
            database_name = env[DB_NAME_STR]

        self._extractor_kafka_theme = env[EXTRACTOR_THEME_STR]
        self._processing_kafka_theme = env[PROCESSING_THEME_STR]
        self._kafka_port = env[KAFKA_PORT_STR]

        self._mongodb = MongoClient(mongodb_uri)[database_name]

    def run(self):
        self._dash_app.run_server(debug=True)


if __name__ == '__main__':
    extractor = UserInterface()
    extractor.run()

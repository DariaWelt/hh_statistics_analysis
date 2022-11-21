from os import environ as env

import dash
import numpy as np
from dash import html, dcc, Output, Input, State
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly import express

from hh_analyzer.ServisesUtils import DB_URI_STR, DB_NAME_STR, EXTRACTOR_THEME_STR, PROCESSING_THEME_STR, KAFKA_PORT_STR
from hh_analyzer.ServisesUtils.service_core import HHService
from hh_analyzer.ServisesUtils.utils import load_config

class UserInterface(HHService):
    _kafka_port: str
    _extractor_kafka_theme: str
    _processing_kafka_theme: str
    _kafka_consumer: KafkaConsumer
    _kafka_producer: KafkaProducer

    _waiting_for_processing: bool
    _waiting_for_extractor: bool

    _collection_name: str = 'hh_vacancies_processed'
    _dash_app: dash.Dash

    TEST_DATA_PART = 23.4
    TEST_DATA_SALARY = {
        'technologies': ['delphy', 'python', 'opengl'],
        'without': [np.mean([112000, 78000, 30000, 82000, 200000, 150000]),
                    np.mean([130000, 50000, 30000, 62000, 20000, 200000]),
                    np.mean([112000, 78000, 30000, 82000, 20000, 150000])],
        'with': [np.mean([100000, 50000, 20000, 30000, 40000]),
                 np.mean([100000, 150000, 200000, 30000, 63000]),
                 np.mean([200000, 150000, 20000, 112000, 40000])]
    }

    def __init__(self):
        super(UserInterface, self).__init__('hh_user_interface', '../../gui_logs/')
        self._dash_app = dash.Dash()
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._kafka_port, api_version=(0, 10))

        # consumers to listen to responses
        self._kafka_consumer = KafkaConsumer(bootstrap_servers=self._kafka_port, auto_offset_reset='earliest',
                                             api_version=(0,10))
        self._kafka_consumer.subscribe([f'resp_{self._extractor_kafka_theme}',
                                        f'resp_{self._processing_kafka_theme}',
                                        ])
        self._waiting_for_processing = False
        self._waiting_for_extractor = False

        self.add_extractor_button()
        self.add_processor_button()

        self.add_dashboard()

    def add_extractor_button(self):
        self._dash_app.layout = html.Div([
            self._dash_app.layout,
            html.Button('Extract month data', id='extract-data'),
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
            self._waiting_for_extractor = True
            self.wait_for_response()
            return n_clicks

    def add_processor_button(self):
        self._dash_app.layout = html.Div([
            self._dash_app.layout,
            'Type tool or technology which you want to analyze:',
            html.Div(dcc.Input(id='input-on-processing-button', type='text')),
            html.Button('Process data', id='processing-data', n_clicks=0),
            html.Div(id='processing-button-basic', children='Click to process data for last month'),
        ])

        @self._dash_app.callback(
            Output('processing-button-basic', 'children'),
            Input('processing-data', 'n_clicks'),
            State('input-on-processing-button', 'value')
        )
        def call_processing(n_clicks, value):
            self._logger.info(f"preparing to send {self._processing_kafka_theme}: message '{value}'")
            self._kafka_producer.send(self._processing_kafka_theme, value.encode('utf-8'))
            self._kafka_producer.flush()
            self._logger.info(f"sent to {self._processing_kafka_theme}: message '{value}'")
            self._waiting_for_processing = True
            self.wait_for_response()
            return 'The input value was "{}" and the button has been clicked {} times'.format(
                value,
                n_clicks
            )

    def wait_for_response(self):
        for message in self._kafka_consumer:
            self._logger.info(f"got {message.topic}: message '{message.value}'")
            if message.topic == f'resp_{self._extractor_kafka_theme}':
                self._waiting_for_extractor = False
                self._logger.info('got response from extractor')
            if message.topic == f'resp_{self._processing_kafka_theme}':
                self._waiting_for_processing = False
                self.update_dashboard()
                self._logger.info('got response from processing')

    def add_dashboard(self):
        vac_percentage = go.Figure(data=[go.Pie(values=[self.TEST_DATA_PART, 100-self.TEST_DATA_PART],
                                                name="Part of vacancies where technology is required",
                                                textinfo='none',
                                                hole=.6,
                                                marker_colors=['rgb(36, 73, 147)', 'rgb(256, 256, 256)'])])

        bar_fig = express.bar(self.TEST_DATA_SALARY, x='technologies', y='with',
                      barmode='stack', text='technologies')
        bar_fig.update_layout(title="Salaries distribution over technologies",
                           xaxis_title='technology',
                           yaxis_title='Salary', width=800)
        bar_fig.update_traces(texttemplate='%{text:.2s}', textposition='inside')

        correlation_fig = express.imshow([[0.2, 0.1, 0.6, 0.9],
                                          [0.3, 0.6, 0.9, 0.2],
                                          [0.5, 0.2, 0.4, 0.5],
                                          [0.1, 0.1, 0.6, 0.1]], text_auto=True)

        self._dash_app.layout = html.Div([
            self._dash_app.layout,
            html.Div(className='row',  # Define the row element
                     children=[
                         html.Div(className='histogramm', children=dcc.Graph(figure=bar_fig),
                                  style={'width': '69%', 'display': 'inline-block'}),  # Define the left element
                         html.Div(className='pie chart', children=dcc.Graph(figure=vac_percentage),
                                  style={'width': '29%', 'display': 'inline-block'})  # Define the right element
                     ]),
            html.Div(className='correlation_matrix',
                     children=dcc.Graph(figure=correlation_fig))

        ])

    def update_dashboard(self):
        self._logger.info(f"called dash updating method..")

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

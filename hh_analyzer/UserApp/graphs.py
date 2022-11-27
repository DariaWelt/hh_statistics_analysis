import math
from typing import Dict

import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html

from plotly import express
from plotly.subplots import make_subplots


def get_hist(task_data: Dict):
    data = task_data['technology_data']
    bar_fig = express.bar(data, x='name', y='value',
                          barmode='stack', color='value', color_continuous_scale='RdBu_r')
    bar_fig.update_layout(title=task_data['title'],
                          xaxis_title='technology',
                          yaxis_title=task_data['units'], width=800)
    bar_fig.update_traces(textposition='inside')
    return html.Div(className='histogramm', children=dcc.Graph(figure=bar_fig),
                    style={'width': '50%', 'display': 'inline-block'})


def get_matrix(task_data: Dict):
    data = task_data['technology_data']
    tech_names = set()
    corr_values = dict()
    for tech_info in data:
        tech_names.add(tech_info['name'])
        tech_names = tech_names.union(tech_info['value'])
        if corr_values.get(tech_info['name']) is None:
            corr_values[tech_info['name']] = dict()
        for tech_name, value in tech_info['value'].items():
            corr_values[tech_info['name']][tech_name] = value

    def get_corr(tech_1: str, tech_2: str):
        res = corr_values.get(tech_1)
        if res is None:
            return math.nan
        res = res.get(tech_2)
        if res is None:
            return math.nan
        return res

    df_raw = {tech_1: [get_corr(tech_1, tech_2) for tech_2 in tech_names] for tech_1 in tech_names}
    df = pd.DataFrame.from_dict(df_raw, orient='index', columns=list(tech_names))

    correlation_fig = express.imshow(df, text_auto=True, color_continuous_scale='RdBu_r', origin='lower')
    correlation_fig.update_layout(title=task_data['title'])
    return html.Div(className='correlation_matrix', children=dcc.Graph(figure=correlation_fig),
                    style={'width': '50%', 'display': 'inline-block'})


def get_pies(task_data: Dict):
    data = task_data['technology_data']
    fig = make_subplots(1, len(data), specs=[[{'type': 'domain'} for _ in data]],
                        subplot_titles=[t["name"] for t in data])
    for i, tech_info in enumerate(data):
        ann = f'{tech_info["value"]}{task_data["units"]}'
        fig.add_trace(go.Sunburst(values=[0, tech_info['value'], 100 - tech_info['value']],
                                  labels=[ann, " ", "  "], parents=["", ann, ann], name=task_data['title'],
                                  marker_colors=['rgb(256, 256, 256)', 'rgb(36, 73, 147)', 'rgb(245, 245, 250)']),
                      1, i + 1)
    fig.update_layout(title_text=task_data['title'])
    return html.Div(className='pie chart', children=dcc.Graph(figure=fig),
                    style={'width': '50%', 'display': 'inline-block'})


def get_graph(task_data: Dict):
    for tech_info in task_data['technology_data']:
        t = type(tech_info['value'])
        if t is dict:
            if len(task_data['technology_data']) == 1:
                return html.Div(children=[], style={'width': '50%', 'display': 'inline-block'})
            else:
                return get_matrix(task_data)
        if t is int or t is float:
            if len(task_data['technology_data']) > 2:
                return get_hist(task_data)
            else:
                return get_pies(task_data)
        else:
            raise ValueError('unknown format of task data')

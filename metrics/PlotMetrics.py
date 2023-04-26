import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import ast
import os

def date_parser(date_string):
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')


def load_data(file_name, metric):
    data = pd.read_csv(file_name, skiprows=1, names=['Timestamp', metric], parse_dates=['Timestamp'],
                       date_parser=date_parser)
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['value'] = data[metric].apply(lambda x: float(ast.literal_eval(x)[0]['value']))
    data['relative_time'] = (data['Timestamp'] - data['Timestamp'][0]).dt.total_seconds()

    return data

def multi_plot(file_names, operator, metric):

    if metric == 'latency':
        y_label = 'latency(ms)'
        title = operator + ' Latency'

    elif metric == 'Throughput':
        y_label = 'Throughput(events/s)'
        title = operator + ' Throughput'

    datas = [load_data(file, metric) for file in file_names]
    colors = ['blue', 'red', 'green']
    for file, data, color in zip(file_names, datas, colors):
        # label = os.path.splitext(file)[0]
        label = os.path.splitext(os.path.basename(file))[0]
        plt.plot(data['relative_time'], data['value'], label=label, color=color)

    plt.xlabel('Time(second)')
    plt.ylabel(y_label)
    plt.title(title)

    plt.legend()
    plt.show()


def plot_metric(file_name, operator, metric):
    data = pd.read_csv(file_name, skiprows=1, names=['Timestamp', metric], parse_dates=['Timestamp'],
                       date_parser=date_parser)

    if metric == 'latency':
        y_label = 'latency(ms)'
        title = operator + ' Latency'

    elif metric == 'Throughput':
        y_label = 'Throughput(events/s)'
        title = operator + ' Throughput'

    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['value'] = data[metric].apply(lambda x: float(ast.literal_eval(x)[0]['value']))
    data['relative_time'] = (data['Timestamp'] - data['Timestamp'][0]).dt.total_seconds()

    plt.plot(data['relative_time'], data['value'])
    plt.xlabel('Time(second)')
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid()
    plt.show()

files = ['./results/MapToCG_throughput.csv', './results/MapSampler_throughput.csv']
multi_plot(files, 'MapSampler', 'Throughput')
# plot_metric('./results/MapToCG_latency.csv', 'latency')
# plot_metric('./results/MapToCG_throughput.csv', 'Throughput')
# plot_metric('./results/MapToCG_throughput.csv', 'MapToCG', 'Throughput')
# plot_metric('./results/MapToCG_latency.csv', 'MapToCG', 'latency')
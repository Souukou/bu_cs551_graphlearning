import requests
import time
import sys
import csv
import os
import json
import threading


def monitor_throughput(flink_jobmanager_url, job_id, vertex_id, output_file, polling_interval, metric):
    def fetch_throughput(jobmanager_url, job_id, vertex_id, metric):
        url = f'{jobmanager_url}/jobs/{job_id}/vertices/{vertex_id}/metrics?get={metric}'
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'Error fetching throughput data: {response.status_code}')
            return None

    with open(output_file, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['Timestamp', 'Throughput'])

        while True:
            throughput_data = fetch_throughput(flink_jobmanager_url, job_id, vertex_id, metric)
            if throughput_data:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                csv_writer.writerow([timestamp, throughput_data])
                f.flush()
            time.sleep(polling_interval)


if __name__ == "__main__":
    # result_path = "./results"
    #
    # if not os.path.exists(result_path):
    #     os.makedirs(result_path)

    flink_rest_url = 'http://localhost:8081'
    polling_interval = 0.1  # in seconds
    metric = '0.numRecordsOutPerSecond'

    job_id = sys.argv[1]
    # job_id = '49ee611b00761daf0ba2724059f9d42e'
    vertices = json.loads(requests.get(flink_rest_url + "/jobs/" + job_id).text)['vertices']
    operators = ['Window', 'MapSampler', 'MapToCG']
    thread_dict = {}
    for vertex in vertices:
        if vertex['name'] in operators:
            thread_name = f"thread_{vertex['name']}"
            thread_dict[thread_name] = threading.Thread(
                target=monitor_throughput,
                args=(
                    flink_rest_url,
                    job_id,
                    vertex['id'],
                    './results/' + vertex['name'] + '_throughput.csv',
                    polling_interval,
                    metric,
                ),
            )

    for thread_name, thread_obj in thread_dict.items():
        print(f"Starting {thread_name}")
        thread_obj.start()

    for thread_name, thread_obj in thread_dict.items():
        thread_obj.join()
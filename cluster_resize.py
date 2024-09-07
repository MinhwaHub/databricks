import http.client
import json
import sys
from util import *
from datetime import datetime, timedelta
import time 
from dotenv import load_dotenv
import os
from web3 import Web3

load_dotenv()
databricks_host = os.environ.get('databricks_host')
token = os.environ.get('token')
job_id_dic = os.environ.get('job_id_dic')  # {job_name:job_id}

conn = http.client.HTTPSConnection(databricks_host)
headers = {
'Authorization': token
}

def get_cluster_list(): 
    conn.request("GET","/api/2.1/clusters/list",'', headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")

    parsed_data = json.loads(data)['clusters']

    return parsed_data

def reset_cluster(cluster_name:str, worker_type:str, driver_type:str, num_workers:tuple): 
    cluster_id = [cluster for cluster in target_cluster if cluster['cluster_name'] == cluster_name][0]['cluster_id']
    print(f"reset cluster {cluster_name}:{cluster_id}")

    payload = json.dumps({
        "cluster_id": cluster_id,
        "cluster": {
            "node_type_id":worker_type,
            "driver_node_type_id":driver_type,
            "autoscale": {
            "min_workers": num_workers[0],
            "max_workers": num_workers[1]
            }
        },
        "update_mask": "autoscale,node_type_id,driver_node_type_id"
        })
    conn.request("POST", "/api/2.1/clusters/update", payload, headers)
    res = conn.getresponse()
    data = res.read()

    print(str(data))

    assert 'error' not in str(data), 'Failed to reset cluster'
    print('Success !')

def get_cluster_state(job_id:int): 
    payload = json.dumps({
        "job_id": job_id
        })
    
    conn.request("GET", "/api/2.1/jobs/runs/list", payload, headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")

    parsed_data = json.loads(data)

    return parsed_data

target_cluster = []
cluster_list = get_cluster_list()


if __name__ == "__main__":
    for cluster in cluster_list:
        if any(keyword in cluster['cluster_name'] for keyword in ['api', 'master', 'etl','wemixplay','bi']):
            temp = {}
            temp['cluster_id'] = cluster['cluster_id']  ## do not change
            temp['cluster_name'] = cluster['cluster_name']  ## do not change
            temp['spark_version'] = cluster['spark_version']  ## do not change
            temp['node_type_id'] = cluster['node_type_id']  ## worker type
            temp['driver_node_type_id'] = cluster['driver_node_type_id']  ## driver type
            temp['autoscale'] = cluster['autoscale']   ## min max workers
            target_cluster.append(temp)

    if (datetime.now() + timedelta(hours=9)).hour < 9:  #배치 시각 이전의 클러스터 사이즈 확장
        for target in target_cluster:
            print("cluster size up")
            reset_cluster(target['cluster_name'], 'Standard_E4d_v4','Standard_DS4_v2',(1,10))
            # reset_cluster("wemixplay (catalog) cluster", 'Standard_E4d_v4','Standard_DS4_v2',(4,8))

    success_count = 0
    while success_count >= 0 :   #check daily batch status
        for job_name, job_id in job_id_dic.items():
            a=get_cluster_state(job_id) 
            largest_dict = max(a['runs'], key=lambda x: x["end_time"])
            print(largest_dict['run_name'])
            # print(largest_dict['end_time'])
            end_date = datetime.fromtimestamp(largest_dict['end_time']/1000).day
            if (datetime.now()).day != end_date:
                time.sleep(300)
                pass
            elif largest_dict['state']['result_state'] == 'SUCCESS':
                print("today job end")
                success_count +=1 
            
            if success_count == len(job_id_dic):
                break
            print(success_count)

        break
        
    #if finish daily batch job, 
    if success_count == len(job_id_dic):
        print("cluster size down")
        for target in target_cluster:
            reset_cluster(target['cluster_name'], 'Standard_E4d_v4','Standard_D3_v2',(1,5))
            # reset_cluster(target['cluster_name'], 'Standard_E4d_v4','Standard_D3_v2',(1,5))

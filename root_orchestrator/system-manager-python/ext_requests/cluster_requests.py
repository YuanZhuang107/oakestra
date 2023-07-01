import logging
import time
import requests

from ext_requests.apps_db import mongo_find_job_by_id, mongo_find_cluster_of_job
from ext_requests.cluster_db import mongo_find_cluster_by_id, mongo_find_cluster_by_ip


def cluster_request_to_deploy(cluster_id, job_id, instance_number):
    print('propagate to cluster...')
    cluster = mongo_find_cluster_by_id(cluster_id)
    job = mongo_find_job_by_id(job_id)
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/deploy/' + str(job_id) + "/" + str(instance_number)
        job['_id'] = str(job['_id'])
        resp = requests.post(cluster_addr, json=job)
        print(resp)
    except requests.exceptions.RequestException as e:
        print('Calling Cluster Orchestrator /api/deploy not successful.')


def cluster_request_to_delete_job(job_id, instance_number):
    cluster = mongo_find_cluster_of_job(job_id, int(instance_number))
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/delete/' + str(
            job_id) + "/" + str(instance_number)
        resp = requests.get(cluster_addr)
        print(resp)
    except Exception as e:
        logging.error(e)
        print(e)
        print('Calling Cluster Orchestrator /api/delete not successful.')


def cluster_request_to_delete_job_by_ip(job_id, instance_number,ip):
    try:
        cluster = mongo_find_cluster_by_ip(ip)
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/delete/' + str(
            job_id) + "/" + str(instance_number)
        resp = requests.get(cluster_addr)
        print(resp)
    except Exception as e:
        logging.error(e)
        print('Calling Cluster Orchestrator /api/delete not successful.')


def cluster_request_to_replicate_up(cluster_obj, job_obj, int_replicas):
    cluster_addr = 'http://' + cluster_obj.get('ip') + ':' + str(cluster_obj.get('port')) + '/api/replicate/'
    try:
        resp = requests.post(cluster_addr, json={'job': job_obj, 'int_replicas': int_replicas})
        print(resp)
        return 1
    except requests.exceptions.RequestException as e:
        print('Calling Cluster Orchestrator /api/replicate not successful.')


def cluster_request_to_replicate_down(cluster_obj, job_obj, int_replicas):
    cluster_addr = 'http://' + cluster_obj.get('ip') + ':' + str(cluster_obj.get('port')) + '/api/replicate/'
    try:
        resp = requests.post(cluster_addr, json={'job': job_obj, 'int_replicas': int_replicas})
        print(resp)
        return 1
    except requests.exceptions.RequestException as e:
        print('Calling Cluster Orchestrator /api/replicate not successful.')


def cluster_request_to_move_within_cluster(cluster_obj, job_id, node_from, node_to):
    cluster_addr = 'http://' + cluster_obj.get('ip') + ':' + str(cluster_obj.get('port')) + '/api/move/'
    try:
        resp = requests.post(cluster_addr, json={'job': job_id, 'node_from': node_from, 'node_to': node_to})
        print(resp)
        return 1
    except requests.exceptions.RequestException as e:
        print('Calling Cluster Orchestrator /api/move not successful.')

def cluster_request_to_get_aoi(cluster_id):
    print('getting cluster aoi', cluster_id)
    cluster = mongo_find_cluster_by_id(cluster_id)
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/aoi/'
        resp = requests.get(cluster_addr)
        print(resp)
        return resp
    except requests.exceptions.RequestException as e:
        print('Calling /api/aoi/ not successful:', e)

def cluster_request_to_reset_aoi(cluster_id):
    print('resetting cluster aoi', cluster_id)
    cluster = mongo_find_cluster_by_id(cluster_id)
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/aoi/'
        resp = requests.delete(cluster_addr)
        print(resp)
        return resp
    except requests.exceptions.RequestException as e:
        print('Calling /api/aoi/ not successful:', e)

def cluster_request_to_update_cadence(cluster_id, node_id, cadence):
    print('updating node cadence', node_id, cadence)
    cluster = mongo_find_cluster_by_id(cluster_id)
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/update/' + node_id + '/cadence'
        resp = requests.put(cluster_addr, json={'cadence': cadence})
        print(resp)
        return resp
    except requests.exceptions.RequestException as e:
        print('Calling api/update/cadence not successful:', e)

def cluster_request_to_get_nodes(cluster_id):
    print('getting all nodes', cluster_id)
    cluster = mongo_find_cluster_by_id(cluster_id)
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/nodes/'
        resp = requests.get(cluster_addr)
        print(resp)
        return resp
    except requests.exceptions.RequestException as e:
        print('Calling /api/nodes/ to GET not successful:', e)

def cluster_request_to_delete_all_nodes(cluster_id):
    print('deleting all nodes', cluster_id)
    cluster = mongo_find_cluster_by_id(cluster_id)
    try:
        cluster_addr = 'http://' + cluster.get('ip') + ':' + str(cluster.get('port')) + '/api/nodes/'
        resp = requests.delete(cluster_addr)
        print(resp)
        return resp
    except requests.exceptions.RequestException as e:
        print('Calling /api/nodes/ to DELETE not successful:', e)
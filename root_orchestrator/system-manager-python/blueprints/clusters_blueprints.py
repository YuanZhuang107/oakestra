import logging
import traceback
import json

from bson import json_util
from flask.views import MethodView
from flask import request
from flask_smorest import Blueprint, Api, abort

from ext_requests.cluster_requests import cluster_request_to_delete_job, cluster_request_to_delete_job_by_ip, cluster_request_to_get_aoi, cluster_request_to_reset_aoi, cluster_request_to_update_cadence, cluster_request_to_get_nodes, cluster_request_to_delete_all_nodes
from services.service_management import delete_service
from ext_requests.apps_db import mongo_update_job_status
from ext_requests.cluster_db import mongo_get_all_clusters, mongo_find_all_active_clusters, \
    mongo_update_cluster_information, mongo_find_cluster_by_id, mongo_delete_cluster
from services.instance_management import instance_scale_up_scheduled_handler

clustersbp = Blueprint(
    'Clusters', 'cluster management', url_prefix='/api/clusters'
)

clusterinfo = Blueprint(
    'Clusterinfo', 'cluster informations', url_prefix='/api/information'
)

cluster_info_schema = {
    "type": "object",
    "properties": {
        "cpu_percent": {"type": "string"},
        "cpu_cores": {"type": "string"},
        "gpu_cores": {"type": "string"},
        "gpu_percent": {"type": "string"},
        "cumulative_memory_in_mb": {"type": "string"},
        "number_of_nodes": {"type": "string"},
        "virtualization": {"type": "array", "items": {"type": "string"}},
        "more": {"type": "object"},
        "worker_groups": {"type": "string"},
        "jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "system_job_id": {"type": "string"},
                    "status": {"type": "string"},
                    "instance_list": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "instance_number": {"type": "string"},
                                "status": {"type": "string"},
                                "status_detail": {"type": "string"},
                                "publicip": {"type": "string"}
                            }
                        }
                    },
                }
            }
        },
    }
}

cluster_cadence_schema = {
    "type": "object",
    "properties": {
        "node_id": {"type": "string"},
        "cadence": {"type": "string"}
    }
}

@clustersbp.route('/')
class ClustersController(MethodView):

    def get(self, *args, **kwargs):
        return json_util.dumps(mongo_get_all_clusters())


@clustersbp.route('/active')
class ActiveClustersController(MethodView):

    def get(self, *args, **kwargs):
        return json_util.dumps(mongo_find_all_active_clusters())

@clusterinfo.route('/<clusterid>')
class ClusterController(MethodView):

    @clusterinfo.arguments(schema=cluster_info_schema, location="json", validate=False, unknown=True)
    def post(self, *args, **kwargs):
        data = request.json
        mongo_update_cluster_information(kwargs['clusterid'], data)
        jobs = data.get('jobs')
        for j in jobs:
            result = mongo_update_job_status(
                job_id=j.get('system_job_id'),
                status=j.get('status'),
                status_detail=j.get('status_detail'),
                instances=j.get('instance_list'))
            if result is None:
                # cluster has outdated jobs, ask to undeploy
                cluster_request_to_delete_job_by_ip(j.get('system_job_id'), -1, request.remote_addr)

        return 'ok'

    def delete(self, *args, **kwargs):
        mongo_delete_cluster(kwargs['clusterid'])
        return 'ok'

@clusterinfo.route('/<clusterid>/aoi')
class ClusterAOIController(MethodView):

    def get(self, clusterid):
        resp = cluster_request_to_get_aoi(clusterid)
        print("AOI result", resp.json())
        return json_util.dumps(resp.json())

    def delete(self, clusterid):
        resp = cluster_request_to_reset_aoi(clusterid)
        print("Reset AOI result", resp.json())
        return json_util.dumps(resp.json())

@clusterinfo.route('/<clusterid>/cadence')
class ClusterUpdateCadenceController(MethodView):

    @clusterinfo.arguments(schema=cluster_cadence_schema, location="json", validate=False, unknown=True)
    def put(self, *args, **kwargs):
        data = request.json
        resp = cluster_request_to_update_cadence(kwargs['clusterid'], data.get('node_id'), data.get('cadence'))
        print("Updated cadence result")
        return 'ok'

@clusterinfo.route('/<clusterid>/nodes')
class ClusterNodeController(MethodView):

    def get(self, clusterid):
        resp = cluster_request_to_get_nodes(clusterid)
        print("Get nodes result", resp.json())
        return json_util.dumps(resp.json())

    def delete(self, clusterid):
        resp = cluster_request_to_delete_all_nodes(clusterid)
        print("Delete all nodes result", resp.json())
        return json_util.dumps(resp.json())

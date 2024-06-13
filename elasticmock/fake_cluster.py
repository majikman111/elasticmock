# -*- coding: utf-8 -*-
import typing as t
from elasticsearch._sync.client.cluster import ClusterClient
from elasticsearch._sync.client.utils import _rewrite_parameters


from elastic_transport import ObjectApiResponse
from elasticmock.utilities.decorator import wrap_object_api_response


class FakeClusterClient(ClusterClient):

    @_rewrite_parameters()
    @wrap_object_api_response
    def health(
        self,
        **params
    ) -> ObjectApiResponse[t.Any]:
        return {
            'cluster_name': 'testcluster',
            'status': 'green',
            'timed_out': False,
            'number_of_nodes': 1,
            'number_of_data_nodes': 1,
            'active_primary_shards': 1,
            'active_shards': 1,
            'relocating_shards': 0,
            'initializing_shards': 0,
            'unassigned_shards': 1,
            'delayed_unassigned_shards': 0,
            'number_of_pending_tasks': 0,
            'number_of_in_flight_fetch': 0,
            'task_max_waiting_in_queue_millis': 0,
            'active_shards_percent_as_number': 50.0
        }

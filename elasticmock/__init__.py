# -*- coding: utf-8 -*-

from functools import wraps

from elasticsearch._sync.client.utils import client_node_configs
from unittest.mock import patch

from elasticmock.fake_elasticsearch import FakeElasticsearch

ELASTIC_INSTANCES = {}


def _get_elasticmock(hosts=None, *args, **kwargs):
    host = client_node_configs(hosts, cloud_id=None)[0]
    elastic_key = '{0}:{1}'.format(
        host.host, host.port
    )

    if elastic_key in ELASTIC_INSTANCES:
        connection = ELASTIC_INSTANCES.get(elastic_key)
    else:
        connection = FakeElasticsearch(hosts=[host])
        ELASTIC_INSTANCES[elastic_key] = connection
    return connection


def elasticmock(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        ELASTIC_INSTANCES.clear()
        with patch('elasticsearch.Elasticsearch', _get_elasticmock):
            result = f(*args, **kwargs)
        return result
    return decorated

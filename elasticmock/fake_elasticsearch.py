# -*- coding: utf-8 -*-
import re
import datetime
import json
import sys
import typing as t
import functools
from collections import defaultdict

import dateutil.parser
from elasticsearch import Elasticsearch
from elasticsearch._sync.client._base import BaseClient
from elasticsearch._sync.client.utils import _rewrite_parameters
from elasticsearch._sync.client.utils import client_node_configs
from elastic_transport import (
    ApiResponseMeta,
    HeadApiResponse,
    ObjectApiResponse,
    Transport,
)
from elastic_transport.client_utils import DEFAULT, DefaultType
from elasticsearch.exceptions import NotFoundError, BadRequestError

from elasticmock.behaviour.server_failure import server_failure
from elasticmock.fake_cluster import FakeClusterClient
from elasticmock.fake_indices import FakeIndicesClient
from elasticmock.utilities import get_random_id, get_random_scroll_id
from elasticmock.utilities.decorator import for_all_methods, wrap_object_api_response


SelfType = t.TypeVar("SelfType", bound="FakeElasticsearch")

PY3 = sys.version_info[0] == 3
if PY3:
    unicode = str


class QueryType:
    BOOL = 'BOOL'
    FILTER = 'FILTER'
    MATCH = 'MATCH'
    MATCH_ALL = 'MATCH_ALL'
    TERM = 'TERM'
    TERMS = 'TERMS'
    MUST = 'MUST'
    RANGE = 'RANGE'
    SHOULD = 'SHOULD'
    MINIMUM_SHOULD_MATCH = 'MINIMUM_SHOULD_MATCH'
    MULTI_MATCH = 'MULTI_MATCH'
    MUST_NOT = 'MUST_NOT'

    @staticmethod
    def get_query_type(type_str):
        if type_str == 'bool':
            return QueryType.BOOL
        elif type_str == 'filter':
            return QueryType.FILTER
        elif type_str == 'match':
            return QueryType.MATCH
        elif type_str == 'match_all':
            return QueryType.MATCH_ALL
        elif type_str == 'term':
            return QueryType.TERM
        elif type_str == 'terms':
            return QueryType.TERMS
        elif type_str == 'must':
            return QueryType.MUST
        elif type_str == 'range':
            return QueryType.RANGE
        elif type_str == 'should':
            return QueryType.SHOULD
        elif type_str == 'minimum_should_match':
            return QueryType.MINIMUM_SHOULD_MATCH
        elif type_str == 'multi_match':
            return QueryType.MULTI_MATCH
        elif type_str == 'must_not':
            return QueryType.MUST_NOT
        else:
            raise NotImplementedError(f'type {type_str} is not implemented for QueryType')


class MetricType:
    CARDINALITY = "CARDINALITY"

    @staticmethod
    def get_metric_type(type_str):
        if type_str == "cardinality":
            return MetricType.CARDINALITY
        else:
            raise NotImplementedError(f'type {type_str} is not implemented for MetricType')


class FakeQueryCondition:
    type = None
    condition = None

    def __init__(self, type, condition):
        self.type = type
        self.condition = condition

    def evaluate(self, document):
        return self._evaluate_for_query_type(document)

    def _evaluate_for_query_type(self, document):
        if self.type == QueryType.MATCH:
            return self._evaluate_for_match_query_type(document)
        elif self.type == QueryType.MATCH_ALL:
            return True
        elif self.type == QueryType.TERM:
            return self._evaluate_for_term_query_type(document)
        elif self.type == QueryType.TERMS:
            return self._evaluate_for_terms_query_type(document)
        elif self.type == QueryType.RANGE:
            return self._evaluate_for_range_query_type(document)
        elif self.type == QueryType.BOOL:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.FILTER:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.MUST:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.SHOULD:
            return self._evaluate_for_should_query_type(document)
        elif self.type == QueryType.MULTI_MATCH:
            return self._evaluate_for_multi_match_query_type(document)
        elif self.type == QueryType.MUST_NOT:
            return self._evaluate_for_must_not_query_type(document)
        else:
            raise NotImplementedError('Fake query evaluation not implemented for query type: %s' % self.type)

    def _evaluate_for_match_query_type(self, document):
        return self._evaluate_for_field(document, True)

    def _evaluate_for_term_query_type(self, document):
        return self._evaluate_for_field(document, False)

    def _evaluate_for_terms_query_type(self, document):
        for field in self.condition:
            for term in self.condition[field]:
                if FakeQueryCondition(QueryType.TERM, {field: term}).evaluate(document):
                    return True
        return False

    def _evaluate_for_field(self, document, ignore_case):
        doc_source = document['_source']
        return_val = False
        for field, value in self.condition.items():
            return_val = self._compare_value_for_field(
                doc_source,
                field,
                value,
                ignore_case
            )
            if return_val:
                break
        return return_val

    def _evaluate_for_fields(self, document):
        doc_source = document['_source']
        return_val = False
        value = self.condition.get('query')
        if not value:
            return return_val
        fields = self.condition.get('fields', [])
        for field in fields:
            return_val = self._compare_value_for_field(
                doc_source,
                field,
                value,
                True
            )
            if return_val:
                break

        return return_val

    def _evaluate_for_range_query_type(self, document):
        for field, comparisons in self.condition.items():
            doc_val = document['_source']
            for k in field.split("."):
                if hasattr(doc_val, k):
                    doc_val = getattr(doc_val, k)
                elif k in doc_val:
                    doc_val = doc_val[k]
                else:
                    return False

            if isinstance(doc_val, list):
                return False

            for sign, value in comparisons.items():
                if isinstance(doc_val, datetime.datetime):
                    value = dateutil.parser.isoparse(value)
                if sign == 'gte':
                    if doc_val < value:
                        return False
                elif sign == 'gt':
                    if doc_val <= value:
                        return False
                elif sign == 'lte':
                    if doc_val > value:
                        return False
                elif sign == 'lt':
                    if doc_val >= value:
                        return False
                else:
                    raise ValueError(f"Invalid comparison type {sign}")
            return True

    def _evaluate_for_compound_query_type(self, document):
        return_val = False
        if isinstance(self.condition, dict):
            for query_type, sub_query in self.condition.items():
                return_val = FakeQueryCondition(
                    QueryType.get_query_type(query_type),
                    sub_query
                ).evaluate(document)
                if not return_val:
                    return False
        elif isinstance(self.condition, list):
            for sub_condition in self.condition:
                for sub_condition_key in sub_condition:
                    return_val = FakeQueryCondition(
                        QueryType.get_query_type(sub_condition_key),
                        sub_condition[sub_condition_key]
                    ).evaluate(document)
                    if not return_val:
                        return False

        return return_val

    def _evaluate_for_must_not_query_type(self, document):
        if isinstance(self.condition, dict):
            for query_type, sub_query in self.condition.items():
                return_val = FakeQueryCondition(
                    QueryType.get_query_type(query_type),
                    sub_query
                ).evaluate(document)
                if return_val:
                    return False
        elif isinstance(self.condition, list):
            for sub_condition in self.condition:
                for sub_condition_key in sub_condition:
                    return_val = FakeQueryCondition(
                        QueryType.get_query_type(sub_condition_key),
                        sub_condition[sub_condition_key]
                    ).evaluate(document)
                    if return_val:
                        return False
        return True

    def _evaluate_for_should_query_type(self, document):
        return_val = False
        for sub_condition in self.condition:
            for sub_condition_key in sub_condition:
                return_val = FakeQueryCondition(
                    QueryType.get_query_type(sub_condition_key),
                    sub_condition[sub_condition_key]
                ).evaluate(document)
                if return_val:
                    return True
        return return_val

    def _evaluate_for_multi_match_query_type(self, document):
        return self._evaluate_for_fields(document)

    def _compare_value_for_field(self, doc_source, field, value, ignore_case):
        if ignore_case and isinstance(value, str):
            value = value.lower()

        doc_val = doc_source
        # Remove boosting
        field, *_ = field.split("*")
        for k in field.split("."):
            if hasattr(doc_val, k):
                doc_val = getattr(doc_val, k)
                break
            elif k in doc_val:
                doc_val = doc_val[k]
                break
            else:
                return False

        if not isinstance(doc_val, list):
            doc_val = [doc_val]

        for val in doc_val:
            if not isinstance(val, (int, float, complex)) or val is None:
                val = str(val)
                if ignore_case:
                    val = val.lower()

            if value == val:
                return True
            if isinstance(val, str) and str(value) in val:
                return True

        return False


class NoOpTransport(Transport):
    def perform_request(self, *args, **kwargs):
        assert False, "Transport should not be invoked during unit tests."


@for_all_methods([server_failure])
class FakeElasticsearch(Elasticsearch):
    _documents_dict = None

    def __init__(self, hosts=None, *, transport_class=None, _transport=None, **kwargs):
        self._documents_dict = {}
        self._scrolls = {}
        if _transport is None:
            _transport = NoOpTransport(hosts, **kwargs)
        BaseClient.__init__(self, _transport)

    @property
    def indices(self):
        return FakeIndicesClient(self)

    @property
    def cluster(self):
        return FakeClusterClient(self)

    def options(
        self: SelfType,
        ignore_status: t.Union[DefaultType, int, t.Collection[int]] = DEFAULT,
        **params
    ) -> SelfType:
        if ignore_status is not DEFAULT:
            if isinstance(ignore_status, int):
                ignore_status = (ignore_status,)
            self._ignore_status = ignore_status
        return self

    @_rewrite_parameters()
    def ping(self, **kwargs) -> bool:
        return True

    @_rewrite_parameters()
    @wrap_object_api_response
    def info(self, **kwargs) -> ObjectApiResponse[t.Any]:
        return {
            'status': 200,
            'cluster_name': 'elasticmock',
            'version':
                {
                    "number" : "8.12.0",
                    "build_flavor" : "default",
                    "build_type" : "tar",
                    "build_hash" : "1665f706fd9354802c02146c1e6b5c0fbcddfbc9",
                    "build_date" : "2024-01-11T10:05:27.953830042Z",
                    "build_snapshot" : False,
                    "lucene_version" : "9.9.1",
                    "minimum_wire_compatibility_version" : "7.17.0",
                    "minimum_index_compatibility_version" : "7.0.0"
                },
            'name': 'Nightwatch',
            'tagline': 'You Know, for Search'
        }

    @_rewrite_parameters(
        body_name="document",
    )
    @wrap_object_api_response
    def index(
        self,
        *,
        index: str,
        document: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        id: t.Optional[str] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        if index not in self._documents_dict:
            self._documents_dict[index] = list()

        version = 1
        doc_type = "_doc"

        result = 'created'
        if id is None:
            id = get_random_id()

        elif self.exists(index=index, id=id, **params):
            doc = self.get(index=index, id=id, **params)
            version = doc['_version'] + 1
            self.delete(index=index, id=id)
            result = 'updated'

        self._documents_dict[index].append({
            '_type': doc_type,
            '_id': id,
            '_source': document,
            '_index': index,
            '_version': version
        })

        return {
            '_type': doc_type,
            '_id': id,
            'created': True,
            '_version': version,
            '_index': index,
            'result': result
        }

    @_rewrite_parameters(
        body_name="operations",
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def bulk(
        self,
        *,
        operations: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        index: t.Optional[str] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        items = []
        errors = False

        if isinstance(operations, str):
            operations = [json.loads(raw_line.strip()) for raw_line in operations.splitlines() if raw_line.strip()]

        for line in operations:
            if any(action in line for action in ['index', 'create', 'update', 'delete']):
                action = next(iter(line.keys()))

                version = 1
                index = line[action].get('_index') or index

                if action in ['delete', 'update'] and not line[action].get("_id"):
                    raise BadRequestError(message='action_request_validation_exception, missing id', meta=ApiResponseMeta(400, "HTTP/1.1", {}, 1, None), body=operations)

                document_id = line[action].get('_id', get_random_id())

                if action == 'delete':
                    status, result, error = self._validate_action(
                        action, index, document_id, **params
                    )
                    item = {action: {
                        '_type': "_doc",
                        '_id': document_id,
                        '_index': index,
                        '_version': version,
                        'status': status,
                    }}
                    if error:
                        errors = True
                        item[action]["error"] = result
                    else:
                        self.delete(index=index, id=document_id, **params)
                        item[action]["result"] = result
                    items.append(item)

                if index not in self._documents_dict:
                    self._documents_dict[index] = list()
            else:
                if 'doc' in line and action == 'update':
                    source = line['doc']
                else:
                    source = line
                status, result, error = self._validate_action(
                    action, index, document_id, **params
                )
                item = {
                    action: {
                        '_type': "_doc",
                        '_id': document_id,
                        '_index': index,
                        '_version': version,
                        'status': status,
                    }
                }
                if not error:
                    item[action]["result"] = result
                    if self.exists(index=index, id=document_id, **params):
                        doc = self.get(index=index, id=document_id, **params)
                        version = doc['_version'] + 1
                        self.delete(index=index, id=document_id, **params)

                    self._documents_dict[index].append({
                        '_type': "_doc",
                        '_id': document_id,
                        '_source': source,
                        '_index': index,
                        '_version': version
                    })
                else:
                    errors = True
                    item[action]["error"] = result
                items.append(item)
        return ObjectApiResponse(
            body={
                'errors': errors,
                'items': items
            },
            meta=ApiResponseMeta(status, "HTTP/1.1", {}, 1, None),
        )

    def _validate_action(self, action, index, document_id, **params):
        if action in ['index', 'update'] and self.exists(index=index, id=document_id, **params):
            return 200, 'updated', False
        if action == 'create' and self.exists(index=index, id=document_id, **params):
            return 409, 'version_conflict_engine_exception', True
        elif action in ['index', 'create'] and not self.exists(index=index, id=document_id, **params):
            return 201, 'created', False
        elif action == "delete" and self.exists(index=index, id=document_id, **params):
            return 200, 'deleted', False
        elif action == 'update' and not self.exists(index=index, id=document_id, **params):
            return 404, 'document_missing_exception', True
        elif action == 'delete' and not self.exists(index=index, id=document_id, **params):
            return 404, 'not_found', True
        else:
            raise NotImplementedError(f"{action} behaviour hasn't been implemented")

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def exists(
        self,
        *,
        index: str,
        id: str,
        **params,
    ) -> HeadApiResponse:
        status = 404
        if index in self._documents_dict:
            for document in self._documents_dict[index]:
                if document.get('_id') == id:
                    status = 200
                    break

        return HeadApiResponse(
            meta=ApiResponseMeta(status, "HTTP/1.1", {}, 1, None),
        )

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    @wrap_object_api_response
    def get(
        self,
        *,
        index: str,
        id: str,
        **params,
    ) -> ObjectApiResponse[t.Any]:
        result = None

        if index in self._documents_dict:
            for document in self._documents_dict[index]:
                if document.get('_id') == id:
                    result = document
                    break

        if result:
            result['found'] = True
            return result
        elif self._ignore_status is not DEFAULT and 404 in self._ignore_status:
            return {'found': False}
        else:
            error_data = {
                '_index': index,
                '_type': "_doc",
                '_id': id,
                'found': False
            }
            raise NotFoundError(message="Not Found", meta=ApiResponseMeta(404, "HTTP/1.1", {}, 1, None), body=error_data)

    @_rewrite_parameters(
        body_fields=("conflicts", "max_docs", "query", "script", "slice"),
        parameter_aliases={"from": "from_"},
    )
    @wrap_object_api_response
    def update_by_query(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        script: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        # Actually it only supports script equal operations
        # TODO: Full support from painless language
        if body and script is None:
            script = body['script']
        total_updated = 0
        if isinstance(index, list):
            index, = index
        new_values = {}
        script_params = script['params']
        script_source = script['source'] \
            .split(';')
        for sentence in script_source:
            if sentence:
                mtch = re.match(r"\s*ctx._source.(?P<field>\w+)\s*=\s*\(?params.(?P<key>\w+)\)?\s*", sentence)
                if mtch:
                    field = mtch.group("field")
                    key = mtch.group("key")
                    value = script_params.get(key)
                new_values[field] = value

        matches = self.search(index=index, body=body, **params)
        if matches['hits']['total']:
            for hit in matches['hits']['hits']:
                body = hit['_source']
                body.update(new_values)
                self.index(index=index, body=body, doc_type=hit['_type'], id=hit['_id'])
                total_updated += 1

        return {
            'took': 1,
            'time_out': False,
            'total': matches['hits']['total'],
            'updated': total_updated,
            'deleted': 0,
            'batches': 1,
            'version_conflicts': 0,
            'noops': 0,
            'retries': 0,
            'throttled_millis': 100,
            'requests_per_second': 100,
            'throttled_until_millis': 0,
            'failures': []
        }

    @_rewrite_parameters(
        body_fields=("docs", "ids"),
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    @wrap_object_api_response
    def mget(
        self,
        *,
        index: t.Optional[str] = None,
        docs: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        docs = body.get('docs') or docs
        ids = [doc['_id'] for doc in docs]
        results = []
        for id in ids:
            try:
                results.append(self.get(index=index, id=id, **params))
            except:
                pass
        if not results:
            raise BadRequestError(message='action_request_validation_exception; Validation Failed: 1: no documents to get;', meta=ApiResponseMeta(400, "HTTP/1.1", {}, 1, None), body=body)
        return {'docs': results}

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    @wrap_object_api_response
    def get_source(
        self,
        *,
        index: str,
        id: str,
        **params
    ) -> ObjectApiResponse[t.Any]:
        document = self.get(index=index, id=id, **params)
        return document.get('_source')

    @_rewrite_parameters(
        body_fields=("scroll_id",),
    )
    @wrap_object_api_response
    def clear_scroll(
        self,
        *,
        scroll_id: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        succeeded = True
        num_freed = 1
        if scroll_id == "_all":
            num_freed = (len(self._scrolls))
            self._scrolls.clear()
        elif scroll_id in self._scrolls:
            del self._scrolls[scroll_id]
        else:
            succeeded = True
            num_freed = 0

        return {
            "succeeded": succeeded,
            "num_freed": num_freed,
        }

    @_rewrite_parameters(
        body_fields=("query",),
    )
    @wrap_object_api_response
    def count(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        searchable_indexes = self._normalize_index_to_list(index)

        i = 0
        for searchable_index in searchable_indexes:
            for document in self._documents_dict[searchable_index]:
                i += 1
        result = {
            'count': i,
            '_shards': {
                'successful': 1,
                'skipped': 0,
                'failed': 0,
                'total': 1
            }
        }

        return result

    def _get_fake_query_condition(self, query_type_str, condition):
        return FakeQueryCondition(QueryType.get_query_type(query_type_str), condition)

    @_rewrite_parameters(
        body_name="searches",
    )
    @wrap_object_api_response
    def msearch(
        self,
        *,
        searches: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        def grouped(iterable):
            if len(iterable) % 2 != 0:
                raise Exception('Malformed body')
            iterator = iter(iterable)
            while True:
                try:
                    yield (next(iterator)['index'], next(iterator))
                except StopIteration:
                    break

        responses = []
        took = 0
        for ind, query in grouped(searches):
            response = self.search(index=ind, body=query)
            took += response['took']
            responses.append(response)
        result = {
            'took': took,
            'responses': responses
        }
        return result

    @_rewrite_parameters(
        body_fields=(
            "aggregations",
            "aggs",
            "collapse",
            "docvalue_fields",
            "explain",
            "ext",
            "fields",
            "from_",
            "highlight",
            "indices_boost",
            "knn",
            "min_score",
            "pit",
            "post_filter",
            "profile",
            "query",
            "rank",
            "rescore",
            "runtime_mappings",
            "script_fields",
            "search_after",
            "seq_no_primary_term",
            "size",
            "slice",
            "sort",
            "source",
            "stats",
            "stored_fields",
            "suggest",
            "terminate_after",
            "timeout",
            "track_scores",
            "track_total_hits",
            "version",
        ),
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
            "from": "from_",
        },
    )
    @wrap_object_api_response
    def search(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        aggregations: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        aggs: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        from_: t.Optional[int] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        scroll: t.Optional[t.Union["t.Literal[-1]", "t.Literal[0]", str]] = None,
        size: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        searchable_indexes = self._normalize_index_to_list(index)
        body = body if body is not None else {}

        matches = []
        conditions = []

        if body and 'query' in body:
            query = body['query']
            for query_type_str, condition in query.items():
                conditions.append(self._get_fake_query_condition(query_type_str, condition))
        for searchable_index in searchable_indexes:

            for document in self._documents_dict[searchable_index]:
                if conditions:
                    for condition in conditions:
                        if condition.evaluate(document):
                            matches.append(document)
                            break
                else:
                    matches.append(document)

        for match in matches:
            self._find_and_convert_data_types(match['_source'])

        result = {
            'hits': {
                'total': {'value': len(matches), 'relation': 'eq'},
                'max_score': 1.0
            },
            '_shards': {
                # Simulate indexes with 1 shard each
                'successful': len(searchable_indexes),
                'skipped': 0,
                'failed': 0,
                'total': len(searchable_indexes)
            },
            'took': 1,
            'timed_out': False
        }

        hits = []
        for match in matches:
            match['_score'] = 1.0
            hits.append(match)

        # build aggregations
        _aggregations = aggregations or aggs
        if body:
            _aggregations = body.get("aggregations", None) or body.get("aggs", None)

        if _aggregations:
            aggregations = {}

            for aggregation, definition in _aggregations.items():
                aggregations[aggregation] = {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": self.make_aggregation_buckets(definition, matches)
                }

            if aggregations:
                result['aggregations'] = aggregations

        _size = int(body.get("size", size or 10))
        _from_ = int(body.get("from", from_ or 0))

        if scroll is not None:
            result['_scroll_id'] = str(get_random_scroll_id())
            params['size'] = _size
            params['from'] = _from_ + _size if _from_ is not None else 0
            self._scrolls[result.get('_scroll_id')] = {
                'index': index,
                'doc_type': "_doc",
                'body': body,
                'params': params
            }
            hits = hits[_from_:_from_ + _size]
        elif _size is not None:
            hits = hits[:_size]

        result['hits']['hits'] = hits

        return result

    @_rewrite_parameters(
        body_fields=("scroll_id", "scroll"),
    )
    @wrap_object_api_response
    def scroll(
        self,
        *,
        scroll_id: t.Optional[str] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        scroll = self._scrolls.pop(scroll_id, None)
        if scroll is None:
            raise NotFoundError(
                message="Not Found",
                meta=ApiResponseMeta(404, "HTTP/1.1", {}, 1, None),
                body={
                    "type": "search_phase_execution_exception",
                    "reason": "all shards failed",
                    "phase": "query",
                    "grouped": True,
                }
            )

        result = self.search(
            index=scroll.get('index'),
            # body=scroll.get("body"),
            scroll=scroll,
            **scroll.get("params"),
        )
        return result

    @_rewrite_parameters()
    @wrap_object_api_response
    def delete(
        self,
        *,
        index: str,
        id: str,
        **params
    ) -> ObjectApiResponse[t.Any]:
        found = False

        if index in self._documents_dict:
            for document in self._documents_dict[index]:
                if document.get('_id') == id:
                    found = True
                    self._documents_dict[index].remove(document)
                    break

        result_dict = {
            'found': found,
            '_index': index,
            '_type': "_doc",
            '_id': id,
            '_version': 1,
        }

        if found:
            return result_dict
        elif self._ignore_status is not DEFAULT and 404 in self._ignore_status:
            return {'found': False}
        else:
            raise NotFoundError(message="Not Found", meta=ApiResponseMeta(404, "HTTP/1.1", {}, 1, None), body=result_dict)

    def _normalize_index_to_list(self, index):
        # Ensure to have a list of index
        if index is None:
            searchable_indexes = self._documents_dict.keys()
        elif isinstance(index, str) or isinstance(index, unicode):
            searchable_indexes = [index]
        elif isinstance(index, list):
            searchable_indexes = index
        else:
            # Is it the correct exception to use ?
            raise ValueError("Invalid param 'index'")

        # Check index(es) exists
        for searchable_index in searchable_indexes:
            if searchable_index not in self._documents_dict:
                raise NotFoundError(message='IndexMissingException[[{0}] missing]'.format(searchable_index), meta=ApiResponseMeta(404, "HTTP/1.1", {}, 1, None), body={})

        return searchable_indexes

    @classmethod
    def _find_and_convert_data_types(cls, document):
        for key, value in document.items():
            if isinstance(value, dict):
                cls._find_and_convert_data_types(value)
            elif isinstance(value, datetime.datetime):
                document[key] = value.isoformat()

    def make_aggregation_buckets(self, aggregation, documents):
        if 'composite' in aggregation:
            return self.make_composite_aggregation_buckets(aggregation, documents)
        return []

    def make_composite_aggregation_buckets(self, aggregation, documents):

        def make_key(doc_source, agg_source):
            attr = list(agg_source.values())[0]["terms"]["field"]
            return doc_source[attr]

        def make_bucket(bucket_key, bucket):
            out = {
                "key": {k: v for k, v in zip(bucket_key_fields, bucket_key)},
                "doc_count": len(bucket),
            }

            for metric_key, metric_definition in aggregation["aggs"].items():
                metric_type_str = list(metric_definition)[0]
                metric_type = MetricType.get_metric_type(metric_type_str)
                attr = metric_definition[metric_type_str]["field"]
                data = [doc[attr] for doc in bucket]

                if metric_type == MetricType.CARDINALITY:
                    value = len(set(data))
                else:
                    raise NotImplementedError(f"Metric type '{metric_type}' not implemented")

                out[metric_key] = {"value": value}
            return out

        agg_sources = aggregation["composite"]["sources"]
        buckets = defaultdict(list)
        bucket_key_fields = [list(src)[0] for src in agg_sources]
        for document in documents:
            doc_src = document["_source"]
            key = tuple(make_key(doc_src, agg_src) for agg_src in aggregation["composite"]["sources"])
            buckets[key].append(doc_src)

        buckets = sorted(((k, v) for k, v in buckets.items()), key=lambda x: x[0])
        buckets = [make_bucket(bucket_key, bucket) for bucket_key, bucket in buckets]
        return buckets

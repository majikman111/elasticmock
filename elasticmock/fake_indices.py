# -*- coding: utf-8 -*-
import typing as t

from elastic_transport import ObjectApiResponse, HeadApiResponse, ApiResponseMeta
from elasticsearch.exceptions import NotFoundError, BadRequestError

from elasticsearch._sync.client.indices import IndicesClient
from elasticsearch._sync.client.utils import _rewrite_parameters
from elasticmock.utilities.decorator import wrap_object_api_response


class FakeIndicesClient(IndicesClient):

    @_rewrite_parameters(
        body_fields=("aliases", "mappings", "settings"),
    )
    @wrap_object_api_response
    def create(
        self,
        *,
        index: str,
        body: t.Optional[t.Dict[str, t.Any]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        documents_dict = self._get_documents_dict()
        if index not in documents_dict:
            documents_dict[index] = []
            return {
                "acknowledged": True,
                "shards_acknowledged": True,
                "index": index,
            }
        else:
            raise BadRequestError(message='resource_already_exists_exception', meta=ApiResponseMeta(400, "HTTP/1.1", {}, 1, None), body={"type": "resource_already_exists_exception"})

    @_rewrite_parameters(
        body_fields=("aliases", "mappings", "settings"),
    )
    def exists(
        self,
        *,
        index: str,
        body: t.Optional[t.Dict[str, t.Any]] = None,
        **params
    ) -> HeadApiResponse:
        status = 200 if index in self._get_documents_dict() else 404
        return HeadApiResponse(
            meta=ApiResponseMeta(status, "HTTP/1.1", {}, 1, None),
        )

    @_rewrite_parameters()
    def refresh(
        self,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        **params
    ) -> ObjectApiResponse[t.Any]:
        if self.exists(index=index):
            return {
                "_shards": {
                    "total": 1,
                    "successful": 1,
                    "failed": 0
                }
            }
        else:
            raise NotFoundError(message=f"no such index [{index}]", meta=ApiResponseMeta(404, "HTTP/1.1", {}, 1, None), body={"type": "index_not_found_exception"})

    @_rewrite_parameters()
    def delete(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        **params
    ) -> ObjectApiResponse[t.Any]:
        documents_dict = self._get_documents_dict()
        if index in documents_dict:
            del documents_dict[index]
            return {"acknowledged": True}
        else:
            raise NotFoundError(message=f"no such index [{index}]", meta=ApiResponseMeta(404, "HTTP/1.1", {}, 1, None), body={"type": "index_not_found_exception"})

    def _get_documents_dict(self):
        return self._client._documents_dict

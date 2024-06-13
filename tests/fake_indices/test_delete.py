# -*- coding: utf-8 -*-
from elasticsearch.exceptions import NotFoundError
from tests import TestElasticmock, INDEX_NAME


class TestDelete(TestElasticmock):

    def test_should_delete_index(self):
        self.assertFalse(self.es.indices.exists(index=INDEX_NAME))

        self.es.indices.create(index=INDEX_NAME)
        self.assertTrue(self.es.indices.exists(index=INDEX_NAME))

        self.es.indices.delete(index=INDEX_NAME)
        self.assertFalse(self.es.indices.exists(index=INDEX_NAME))

    def test_should_delete_inexistent_index(self):
        self.assertFalse(self.es.indices.exists(index=INDEX_NAME))

        with self.assertRaises(NotFoundError):
            self.es.indices.delete(index=INDEX_NAME)
        self.assertFalse(self.es.indices.exists(index=INDEX_NAME))

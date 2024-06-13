# -*- coding: utf-8 -*-

from tests import TestElasticmock


class TestTransport(TestElasticmock):

    def test_should_raise_error(self):
        with self.assertRaises(AssertionError):
            self.es.perform_request("HEAD", "/", params={}, headers={})

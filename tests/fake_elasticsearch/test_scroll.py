# -*- coding: utf-8 -*-
from elasticsearch.exceptions import NotFoundError
from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY


class TestScroll(TestElasticmock):

    def test_scrolling(self):
        for _ in range(100):
            self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        result = self.es.search(index=INDEX_NAME, **{'scroll': '1m', 'size': 30})
        self.__assert_scroll(result, 30)

        for _ in range(2):
            result = self.es.scroll(scroll_id=result.get('_scroll_id'), scroll='1m')
            self.__assert_scroll(result, 30)

        result = self.es.scroll(scroll_id=result.get('_scroll_id'), scroll='1m')
        self.__assert_scroll(result, 10)

    def test_clear_scroll(self):
        for _ in range(100):
            self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        result = self.es.search(index=INDEX_NAME, **{'scroll': '1m', 'size': 30})
        self.__assert_scroll(result, 30)

        self.es.clear_scroll(scroll_id=result.get('_scroll_id'))
        with self.assertRaises(NotFoundError):
            result = self.es.scroll(scroll_id=result.get('_scroll_id'), scroll='1m')

        # Clear using _all as scroll_id
        result = self.es.search(index=INDEX_NAME, **{'scroll': '1m', 'size': 30})
        self.__assert_scroll(result, 30)

        self.es.clear_scroll(scroll_id='_all')
        assert self.es._scrolls == {}
        with self.assertRaises(NotFoundError):
            result = self.es.scroll(scroll_id=result.get('_scroll_id'), scroll='1m')

    def __assert_scroll(self, result, expected_scroll_hits):
        hits = result.get('hits')

        self.assertNotEqual(None, result.get('_scroll_id', None))
        self.assertEqual(expected_scroll_hits, len(hits.get('hits')))
        self.assertEqual(100, hits.get('total').get('value'))

# -*- coding: utf-8 -*-
import subprocess
from time import sleep
from unittest import TestCase

from openprocurement.integrations.treasury.databridge.caching import Db, db_key, tender_key, contract_key, plan_key
from openprocurement.integrations.treasury.databridge.process_tracker import ProcessTracker
from openprocurement.integrations.treasury.databridge.utils import *
from openprocurement.integrations.treasury.databridge.contracts.contract_former import ContractFormer
from hypothesis import given
from hypothesis.strategies import datetimes, integers
from mock import MagicMock, patch
from redis import StrictRedis
from collections import namedtuple

config = {
    "app:api": {
        "cache_host": "127.0.0.1",
        "cache_port": "16379",
        "cache_db_name": 0
    }
}


class TestUtils(TestCase):
    relative_to = os.path.dirname(__file__)  # crafty line
    redis = None
    redis_process = None
    PORT = 16379
    db = Db(config)

    @classmethod
    def setUpClass(cls):
        cls.redis_process = subprocess.Popen(['redis-server', '--port', str(cls.PORT), '--logfile /dev/null'])
        sleep(0.1)
        cls.redis = StrictRedis(port=cls.PORT)

    def setUp(self):
        self.process_tracker = ProcessTracker(self.db)
        self.tender_id = "111"
        self.award_id = "222"
        self.document_id = "333"

    @classmethod
    def tearDownClass(cls):
        cls.redis_process.terminate()
        cls.redis_process.wait()

    def tearDown(self):
        self.redis.flushall()

    def test_db_init(self):
        self.assertEqual(self.db._backend, "redis")
        self.assertEqual(self.db._db_name, 0)
        self.assertEqual(self.db._port, "16379")
        self.assertEqual(self.db._host, "127.0.0.1")

    def test_db_get(self):
        self.assertIsNone(self.db.get("111"))
        self.db.put("111", "test data")
        self.assertEqual(self.db.get("111"), "test data")

    def test_db_set(self):
        self.db.put("111", "test data")
        self.assertEqual(self.db.get("111"), "test data")

    def test_db_has(self):
        self.assertFalse(self.db.has("111"))
        self.db.put("111", "test data")
        self.assertTrue(self.db.has("111"))

    def test_check_processing_item(self):
        self.assertEqual(self.process_tracker.processing_items, {})
        self.assertFalse(self.process_tracker.check_processing_item(self.tender_id, self.award_id))
        self.process_tracker.set_item(self.tender_id, self.award_id)
        self.assertTrue(self.process_tracker.check_processing_item(self.tender_id, self.award_id))

    def test_check_processed_item(self):
        self.assertEqual(self.process_tracker.processed_items, {})
        self.assertFalse(self.process_tracker.check_processed_item(self.tender_id, self.award_id))
        self.process_tracker.set_item(self.tender_id, self.award_id)
        self.process_tracker.update_items_and_tender(self.tender_id, self.award_id, self.document_id)
        self.assertTrue(self.process_tracker.check_processed_item(self.tender_id, self.award_id))

    def test_check_processed_tender(self):
        self.assertFalse(self.process_tracker.check_processed_tenders(self.tender_id))
        self.redis.set(tender_key(self.tender_id), "333")
        self.assertTrue(self.process_tracker.check_processed_tenders(self.tender_id))

    def test_update_processing_items(self):
        self.process_tracker.processing_items = {item_key(self.tender_id, self.award_id): 2}
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 2})
        self.process_tracker._update_processing_items(self.tender_id, self.award_id, self.document_id)
        self.assertEqual(self.process_tracker.processing_items, {item_key(self.tender_id, self.award_id): 1})
        self.process_tracker._update_processing_items(self.tender_id, self.award_id, self.document_id)
        self.assertEqual(self.process_tracker.processing_items, {})

    def test_check_412_function(self):
        func = check_412(MagicMock(side_effect=ResourceError(
            http_code=412, response=MagicMock(headers={'Set-Cookie': 1}))))
        with self.assertRaises(ResourceError):
            func(MagicMock(headers={'Cookie': 1}))
        func = check_412(MagicMock(side_effect=ResourceError(
            http_code=403, response=MagicMock(headers={'Set-Cookie': 1}))))
        with self.assertRaises(ResourceError):
            func(MagicMock(headers={'Cookie': 1}))
        f = check_412(MagicMock(side_effect=[1]))
        self.assertEqual(f(1), 1)

    @given(integers())
    def test_to_base36(self, input):
        self.assertEqual(to_base36(11), "B")
        self.assertEqual(to_base36(35), "Z")
        self.assertEqual(to_base36(36), "10")
        to_base36(input)

    def test_item_key(self):
        tender_id = '123'
        award_id = '456'
        self.assertEqual(item_key(tender_id, award_id), '{}_{}'.format(tender_id, award_id))

    def test_journal_context(self):
        params = {'text': '123'}
        self.assertTrue(journal_context(params=params))

    def test_generate_request_id(self):
        self.assertTrue(isinstance(generate_request_id(), str))

    def test_more_tenders(self):
        params = {'offset': '123', 'descending': 1}
        response = MagicMock(headers={'Set-Cookie': 1})
        self.assertTrue(more_tenders(params, response))

    @patch('openprocurement.integrations.treasury.databridge.utils.datetime')
    def test_business_date_checker_business_date(self, datetime_mock):
        datetime_mock.now = MagicMock(return_value=datetime(2017, 10, 10, 12, 00, 00, 000000))
        self.assertTrue(business_date_checker())

    @patch('openprocurement.integrations.treasury.databridge.utils.datetime')
    def test_business_date_checker_weekend(self, datetime_mock):
        datetime_mock.now = MagicMock(return_value=datetime(2017, 10, 16, 12, 00, 00, 000000))
        self.assertFalse(business_date_checker())

    @patch('openprocurement.integrations.treasury.databridge.utils.datetime')
    def test_business_date_checker_free_time(self, datetime_mock):
        datetime_mock.now = MagicMock(return_value=datetime(2017, 10, 10, 06, 00, 00, 000000))
        self.assertFalse(business_date_checker())

    def test_create_contract_former(self):
        contract_former = ContractFormer()

    def test_create_xml_from_data(self):
        Contract = namedtuple('Contract', ['contract_id', 'documents'])
        contract = Contract('123', ['file1', 'file2'])
        contract_former = ContractFormer()
        contract_former.form_xml_to_post(contract, 'request_id')



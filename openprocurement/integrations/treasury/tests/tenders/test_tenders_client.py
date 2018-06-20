# -*- coding: utf-8 -*-
from gevent import monkey, event

monkey.patch_all()
import exceptions
import json
import munch
import os
import sys
import types
import unittest

from copy import deepcopy
from datetime import datetime
from mock import patch, call, MagicMock
from munch import munchify
from unittest import TestCase
from ConfigParser import SafeConfigParser
from gevent.queue import Queue
from uuid import uuid4
from datetime import datetime
from restkit.errors import Unauthorized, RequestFailed, ResourceError

try:
    from openprocurement_client.exceptions import ResourceGone
except ImportError:
    from restkit.errors import ResourceGone
from openprocurement.integrations.treasury.databridge.tenders.tenders_scanner import TenderScanner
from openprocurement.integrations.treasury.databridge.sleep_change_value import APIRateController
from openprocurement.integrations.treasury.databridge.process_tracker import ProcessTracker
from openprocurement.integrations.treasury.tests.utils import custom_sleep


class TestTenderScanner(TestCase):
    __test__ = True

    def setUp(self):
        self.process_tracker = ProcessTracker(MagicMock(has=MagicMock(return_value=False)))
        self.tenders_id = [uuid4().hex for _ in range(4)]
        self.sleep_change_value = APIRateController()
        self.tenders_sync_client = MagicMock()
        self.filtered_tenders_queue = Queue(10)
        self.sna = event.Event()
        self.sna.set()
        self.worker = TenderScanner.spawn(self.tenders_sync_client, self.filtered_tenders_queue, self.sna, self.process_tracker,
                                    self.sleep_change_value)
        # (self, tenders_sync_client, filtered_tenders_queue, services_not_available, process_tracker,
        #         sleep_change_value, delay=15)

    def tearDown(self):
        self.worker.shutdown()
        del self.worker

    def test_init(self):
        self.assertGreater(datetime.now().isoformat(), self.worker.start_time.isoformat())
        self.assertEqual(self.worker.tenders_sync_client, self.tenders_sync_client)
        self.assertEqual(self.worker.filtered_tenders_queue, self.filtered_tenders_queue)
        self.assertEqual(self.worker.services_not_available, self.sna)
        self.assertEqual(self.worker.sleep_change_value.time_between_requests, 0)
        self.assertEqual(self.worker.delay, 15)
        self.assertEqual(self.worker.exit, False)

    @staticmethod
    def mock_tenders(status, id, procurementMethodType, data=True):
        if data:
            return munchify({'prev_page': {'offset': '123'},
                             'next_page': {'offset': '1234'},
                             'data': [{'status': status,
                                       "id": id,
                                       'procurementMethodType': 'aboveThreshold{}'.format(procurementMethodType)}]})
        else:
            return munchify({'prev_page': {'offset': '123'},
                             'next_page': {'offset': '1234'},
                             'data': []})

    @patch('gevent.sleep')
    def test_worker(self, gevent_sleep):
        """ Returns tenders, check queue elements after filtering """
        gevent_sleep.side_effect = custom_sleep
        self.tenders_sync_client.sync_tenders.side_effect = [RequestFailed(),
                                                # worker must restart
                                                self.mock_tenders("active.qualification", self.tenders_id[0], 'UA'),
                                                Unauthorized(),
                                                self.mock_tenders("active.tendering", uuid4().hex, 'UA'),
                                                self.mock_tenders("active.pre-qualification", self.tenders_id[1], 'EU')]
        for tender_id in self.tenders_id[0:2]:
            self.assertEqual(self.filtered_tenders_queue.get(), tender_id)

    @patch('gevent.sleep')
    def test_429(self, gevent_sleep):
        """Receive 429 status, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        self.tenders_sync_client.sync_tenders.side_effect = [self.mock_tenders("active.pre-qualification", self.tenders_id[0], 'EU'),
                                                self.mock_tenders("active.tendering", uuid4().hex, 'UA'),
                                                self.mock_tenders("active.qualification", self.tenders_id[1], 'UA'),
                                                ResourceError(http_code=429),
                                                self.mock_tenders("active.qualification", self.tenders_id[2], 'UA')]
        self.sleep_change_value.increment_step = 2
        self.sleep_change_value.decrement_step = 1
        for tender_id in self.tenders_id[0:3]:
            self.assertEqual(self.filtered_tenders_queue.get(), tender_id)
        self.assertEqual(self.sleep_change_value.time_between_requests, 1)

    @patch('gevent.sleep')
    def test_429_sleep_change_value(self, gevent_sleep):
        """Three times receive 429, check queue, check sleep_change_value"""
        gevent_sleep.side_effect = custom_sleep
        self.tenders_sync_client.sync_tenders.side_effect = [self.mock_tenders("active.pre-qualification", self.tenders_id[0], 'EU'),
                                                self.mock_tenders("active.tendering", uuid4().hex, 'UA'),
                                                self.mock_tenders("active.tendering", uuid4().hex, 'UA'),
                                                self.mock_tenders("active.tendering", uuid4().hex, 'UA'),
                                                ResourceError(http_code=429),
                                                ResourceError(http_code=429),
                                                ResourceError(http_code=429),
                                                self.mock_tenders("active.pre-qualification", self.tenders_id[1], 'EU')]
        self.sleep_change_value.increment_step = 1
        self.sleep_change_value.decrement_step = 0.5
        for tender_id in self.tenders_id[0:2]:
            self.assertEqual(self.filtered_tenders_queue.get(), tender_id)
        self.assertEqual(self.sleep_change_value.time_between_requests, 2.5)
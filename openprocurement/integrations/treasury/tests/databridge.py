# -*- coding: utf-8 -*-
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

from ConfigParser import SafeConfigParser

try:
    from openprocurement_client.exceptions import ResourceGone
except ImportError:
    from restkit.errors import ResourceGone
from openprocurement_client.client import ResourceNotFound
from openprocurement.integrations.treasury.databridge.bridge import ContractingDataBridge, journal_context
from openprocurement.integrations.treasury.databridge.utils import CacheDB, generate_request_id
from openprocurement.integrations.treasury.databridge import journal_msg_ids as j_msg
from openprocurement.integrations.treasury.tests.base import AlmostAlwaysTrue, MockedResponse

PWD = os.path.dirname(os.path.realpath(__file__))


@patch(
    'restkit.resource.Resource.request',
    return_value=MockedResponse(
        status_int=200, headers={}, body_string='{"prev_page": {"offset": 0}, "next_page": {"offset": 1}}'
    )
)
@patch('openprocurement.integrations.treasury.databridge.bridge.gevent')
@patch('openprocurement.integrations.treasury.databridge.bridge.logger')
@patch('openprocurement.integrations.treasury.databridge.bridge.INFINITY_LOOP')
class TestDatabridge(unittest.TestCase):
    def setUp(self):
        self.config = SafeConfigParser()
        self.config.add_section('app:api')
        self.config.set('app:api', 'cache_db_name', '0')
        self.config.set('app:api', 'cache_host', '127.0.0.1')
        self.config.set('app:api', 'cache_port', '6379')
        self.config.set('app:api', 'buffers_size', '500')
        self.config.set('app:api', 'full_stack_sync_delay', '15')
        self.config.set('app:api', 'empty_stack_sync_delay', '101')
        self.config.set('app:api', 'on_error_sleep_delay', '5')
        self.config.set('app:api', 'jobs_watcher_delay', '15')
        self.config.set('app:api', 'tenders_api_server', 'http://localhost:6543')
        self.config.set('app:api', 'tenders_api_version', '2.4')
        self.config.set('app:api', 'contracting_api_server', 'http://localhost:6543')
        self.config.set('app:api', 'contracting_api_version', '0')
        self.config.set('app:api', 'public_tenders_api_server', 'http://localhost:6543')
        self.config.set('app:api', 'plans_api_server', 'http://localhost:6543')
        self.config.set('app:api', 'plans_api_version', '0')
        self.config.set('app:api', 'resource', '')
        self.config.set('app:api', 'api_token', 'bot')
        self.config.set('app:api', 'delay', '15')
        self.config.set('app:api', 'increment_step', '1')
        self.config.set('app:api', 'decrement_step', '1')
        self.config.set('app:api', 'time_to_live', '10')

        with open(PWD + '/data/tender.json', 'r') as json_file:
            self.tender = json.load(json_file)

        self.contract = deepcopy(self.tender['contracts'][1])
        self.TENDER_ID = self.tender['id']
        self.DIRECTION = 'backward'
        self.owner_and_token = {'owner': 'owner', 'tender_token': 'tender_token'}

    def _get_calls_count(self, calls_list, call_obj):
        count = 0

        for c in calls_list:
            if c == call_obj:
                count += 1
        return count

    # def test_run_with_dead_all_jobs_and_workers(self, *mocks):

    #     # Prepare context
    #     true_list = [True for i in xrange(0, 21)]
    #     true_list.append(False)
    #     mocks[0].__nonzero__.side_effect = true_list

    #     cb = ContractingDataBridge(self.config)

    #     # Check initialization
    #     msg = "Caching backend: '{}', db name: '{}', host: '{}', port: '{}'".format(
    #         cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host, cb.cache_db._port
    #     )
    #     calls = [
    #         call(msg, extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO}),
    #         call('Initialization contracting clients.', extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO})
    #     ]

    #     mocks[1].info.assert_has_calls(calls)

    #     # Run: while loop has 22 iterations with dead workers and jobs
    #     cb.launch()
    #     logger_calls = mocks[1].info.call_args_list
    #     start_bridge = call('Start Contracting Data Bridge', extra={'MESSAGE_ID': j_msg.DATABRIDGE_START})
    #     current_stage = call(
    #         'Current state: Tenders to process 0; Unhandled contracts 0; '
    #         'Contracts to create 0; Retrying to create 0',
    #         extra={
    #             'handicap_contracts_queue_size': 0,
    #             'contracts_queue_size': 0,
    #             'tenders_queue_size': 0,
    #             'contracts_retry_queue': 0
    #         }
    #     )
    #     starting_sync_workers = call('Starting forward and backward sync workers')
    #     init_clients = call('Initialization contracting clients.', extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO})

    #     self.assertEqual(self._get_calls_count(logger_calls, start_bridge), 1)
    #     self.assertEqual(self._get_calls_count(logger_calls, current_stage), 1)
    #     self.assertEqual(self._get_calls_count(logger_calls, starting_sync_workers), 22)
    #     self.assertEqual(self._get_calls_count(logger_calls, init_clients), 43)

    #     warn_calls = mocks[1].warn.call_args_list
    #     restart_sync = call('Restarting synchronization', extra={'MESSAGE_ID': j_msg.DATABRIDGE_RESTART})
    #     restart_tender_worker = call('Restarting get_tender_contracts worker')
    #     restart_retry_worker = call('Restarting retry_put_contracts worker')
    #     restart_prepare_data = call('Restarting prepare_contract_data worker')
    #     restart_put = call('Restarting put_contracts worker')
    #     restart_prepare_data_retry = call('Restarting prepare_contract_data_retry worker')
    #     self.assertEqual(self._get_calls_count(warn_calls, restart_sync), 21)
    #     self.assertEqual(self._get_calls_count(warn_calls, restart_tender_worker), 21)
    #     self.assertEqual(self._get_calls_count(warn_calls, restart_retry_worker), 21)
    #     self.assertEqual(self._get_calls_count(warn_calls, restart_prepare_data), 21)
    #     self.assertEqual(self._get_calls_count(warn_calls, restart_put), 21)
    #     self.assertEqual(self._get_calls_count(warn_calls, restart_prepare_data_retry), 21)

    #     spawn_calls = mocks[2].spawn.call_args_list

    #     self.assertEqual(len(spawn_calls), 154)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.get_tender_contracts)), 22)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.prepare_contract_data)), 22)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.prepare_contract_data_retry)), 22)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.put_contracts)), 22)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.retry_put_contracts)), 22)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.get_tender_contracts_backward)), 22)
    #     self.assertEqual(self._get_calls_count(spawn_calls, call(cb.get_tender_contracts_forward)), 22)

    # def test_run_with_all_jobs_and_workers(self, *mocks):
    #     cb = ContractingDataBridge(self.config)

    #     true_list = [True, False]
    #     mocks[0].__nonzero__.side_effect = true_list

    #     def _start_conrtact_sculptors(cb):
    #         get_tender_contracts = MagicMock()
    #         prepare_contract_data = MagicMock()
    #         prepare_contract_data_retry = MagicMock()
    #         put_contracts = MagicMock()
    #         retry_put_contracts = MagicMock()

    #         get_tender_contracts.dead = False
    #         prepare_contract_data.dead = False
    #         prepare_contract_data_retry.dead = False
    #         put_contracts.dead = False
    #         retry_put_contracts.dead = False

    #         cb.immortal_jobs = {
    #             'get_tender_contracts': get_tender_contracts,
    #             'prepare_contract_data': prepare_contract_data,
    #             'prepare_contract_data_retry': prepare_contract_data_retry,
    #             'put_contracts': put_contracts,
    #             'retry_put_contracts': retry_put_contracts,
    #         }

    #     def _start_synchronization_workers(cb):
    #         get_tender_contracts_backward = MagicMock()
    #         get_tender_contracts_forward = MagicMock()
    #         get_tender_contracts_backward.dead = False
    #         get_tender_contracts_forward.dead = False
    #         cb.jobs = [get_tender_contracts_backward, get_tender_contracts_forward]

    #     cb._start_contract_sculptors = MagicMock(side_effect=_start_conrtact_sculptors(cb))

    #     cb._start_synchronization_workers = MagicMock(side_effect=_start_synchronization_workers(cb))
    #     _restart_synchronization_workers = MagicMock()
    #     cb._restart_synchronization_workers = _restart_synchronization_workers
    #     cb.run()

    #     logger_calls = mocks[1].info.call_args_list

    #     first_log = call(
    #         "Caching backend: '{}', db name: '{}', host: '{}', port: '{}'".format(
    #             cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host, cb.cache_db._port
    #         ), extra={"MESSAGE_ID": j_msg.DATABRIDGE_INFO}
    #     )
    #     second_log = call('Initialization contracting clients.', extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO})
    #     thread_log = call('Start Contracting Data Bridge', extra=({'MESSAGE_ID': j_msg.DATABRIDGE_START}))

    #     self.assertEqual(mocks[1].info.call_count, 3)
    #     self.assertEqual(self._get_calls_count(logger_calls, first_log), 1)
    #     self.assertEqual(self._get_calls_count(logger_calls, second_log), 1)
    #     self.assertEqual(self._get_calls_count(logger_calls, thread_log), 1)
    #     self.assertEqual(mocks[1].warn.call_count, 0)
    #     self.assertEqual(mocks[2].spawn.call_count, 0)
    #     self.assertEqual(cb._restart_synchronization_workers.call_count, 0)

    # def test_get_tender_credentials(self, *mocks):
    #     cb = ContractingDataBridge(self.config)
    #     cb.client = MagicMock()
    #     cb.client.extract_credentials.side_effect = (Exception(), Exception(), Exception(), self.TENDER_ID)
    #     with self.assertRaises(Exception):
    #         cb.get_tender_credentials(self.TENDER_ID)

    #     extract_credentials_calls = cb.client.extract_credentials.call_args_list
    #     self.assertEqual(self._get_calls_count(extract_credentials_calls, call(self.TENDER_ID)), 3)
    #     self.assertEqual(len(extract_credentials_calls), 3)

    #     cb.client = MagicMock()
    #     cb.client.extract_credentials.return_value = self.TENDER_ID

    #     data = cb.get_tender_credentials(self.TENDER_ID)
    #     self.assertEqual(data, self.TENDER_ID)
    #     cb.client.extract_credentials.assert_called_once_with(self.TENDER_ID)

    #     cb.client = MagicMock()
    #     cb.client.extract_credentials.side_effect = (Exception('Boom!'), Exception('Boom!'), self.TENDER_ID)
    #     data = cb.get_tender_credentials(self.TENDER_ID)

    #     extract_credentials_calls = cb.client.extract_credentials.call_args_list
    #     self.assertEqual(self._get_calls_count(extract_credentials_calls, call(self.TENDER_ID)), 3)
    #     self.assertEqual(len(extract_credentials_calls), 3)
    #     self.assertEqual(data, self.TENDER_ID)

    # def test_put_tender_in_cache_by_contract(self, *mocks):
    #     cb = ContractingDataBridge(self.config)
    #     cb.basket = dict()
    #     contracts = deepcopy(self.tender['contracts'])
    #     for contract in contracts:
    #         cb.basket[contract['id']] = contract['date']
    #     cb.cache_db = MagicMock()

    #     contract = deepcopy(self.tender['contracts'][0])
    #     contract['id'] = 'fake_id'

    #     self.assertEquals(cb._put_tender_in_cache_by_contract(contract, self.TENDER_ID), None)
    #     self.assertEqual(cb.basket.get(contracts[0]['id'], None), contracts[0]['date'])
    #     self.assertEqual(cb.cache_db.put.called, False)

    #     self.assertEquals(cb._put_tender_in_cache_by_contract(contracts[0], self.TENDER_ID), None)
    #     self.assertEqual(cb.basket.get(contracts[0]['id'], None), None)
    #     cb.cache_db.put.assert_called_once_with(self.TENDER_ID, contracts[0]['date'])

    # def test_restart_synchronization_workers(self, *mocks):

    #     cb = ContractingDataBridge(self.config)
    #     cb.clients_initialize = MagicMock()
    #     job_1 = MagicMock()
    #     job_2 = MagicMock()
    #     jobs_list = [job_1, job_2]
    #     cb.jobs = jobs_list
    #     cb._start_synchronization_workers = MagicMock()

    #     cb._restart_synchronization_workers()

    #     self.assertEqual(job_1.kill.call_count, 1)
    #     self.assertEqual(job_2.kill.call_count, 1)
    #     cb._start_synchronization_workers.assert_called_once_with()
    #     cb.clients_initialize.assert_called_once_with()
    #     mocks[1].warn.assert_called_once_with(
    #         'Restarting synchronization', extra={'MESSAGE_ID': j_msg.DATABRIDGE_RESTART}
    #     )

    # def test_run_with_KeyboardInterrupt(self, *mocks):

    #     true_list = [True for i in xrange(0, 21)]
    #     true_list.append(False)
    #     mocks[0].__nonzero__.side_effect = true_list

    #     cb = ContractingDataBridge(self.config)

    #     cb._restart_synchronization_workers = MagicMock(side_effect=KeyboardInterrupt)
    #     cb.launch()

    #     gevent_calls = mocks[2].killall.call_args_list
    #     logger_calls = mocks[1].info.call_args_list

    #     keyboard_interrut_log = call('Exiting...')
    #     kill_all_jobs = call(cb.jobs, timeout=5)
    #     kill_all_immortal_jobs = call(cb.immortal_jobs, timeout=5)

    #     self.assertEqual(self._get_calls_count(logger_calls, keyboard_interrut_log), 1)
    #     self.assertEqual(self._get_calls_count(gevent_calls, kill_all_jobs), 1)
    #     self.assertEqual(self._get_calls_count(gevent_calls, kill_all_immortal_jobs), 1)

    #     cb._start_contract_sculptors = MagicMock(side_effect=KeyboardInterrupt)
    #     with self.assertRaises(KeyboardInterrupt) as e:
    #         cb.launch()
    #     isinstance(e.exception, exceptions.KeyboardInterrupt)

    # def test_run_with_Exception(self, *mocks):
    #     cb = ContractingDataBridge(self.config)
    #     true_list = [True for i in xrange(0, 21)]
    #     true_list.append(False)
    #     mocks[0].__nonzero__.side_effect = true_list

    #     e = Exception('Error!')
    #     cb._restart_synchronization_workers = MagicMock(side_effect=e)
    #     cb.launch()

    #     mocks[1].exception.assert_called_once_with(e)

    # def test_sync_single_tender(self, *mocks):
    #     body_string = '{"prev_page": {"offset": 0}, "next_page": {"offset": 1}, "data": {"owner": "fake_owner", ' \
    #                   '"tender_token": "fake_token", "id": "fake_id"}}'

    #     mocks[3].return_value = MockedResponse(
    #         status_int=200, headers={'Set-Cookie': 'fake_cookie'},
    #         body_string=body_string
    #     )

    #     cb = ContractingDataBridge(self.config)
    #     tender = deepcopy(self.tender)
    #     tender['status'] = 'active'
    #     contract = deepcopy(self.contract)
    #     contract['status'] = 'no_active'
    #     tender['contracts'] = [munchify(contract)]
    #     cb.tenders_sync_client.get_tender = MagicMock(return_value={'data': tender})

    #     cb.sync_single_tender(self.TENDER_ID)

    #     calls_logs = mocks[1].info.call_args_list
    #     self.assertEqual(self._get_calls_count(
    #         calls_logs, call('Skip contract {} in status {}'.format(contract['id'], contract['status']))
    #     ), 1)
    #     self.assertEqual(self._get_calls_count(calls_logs, call(
    #         'Tender {} does not contain contracts to transfer'.format(self.TENDER_ID)
    #     )), 1)

    #     contract = deepcopy(self.contract)
    #     contract['status'] = 'active'
    #     tender['contracts'] = [munchify(contract)]
    #     cb.tenders_sync_client.get_tender = MagicMock(return_value={'data': tender})

    #     cb.get_tender_credentials = MagicMock(
    #         return_value={'data': {'procuringEntity': 'procuringEntity', 'tender_token': 'tender_token'}}
    #     )

    #     cb.contracting_client.get_contract = MagicMock(side_effect=ResourceNotFound)
    #     cb.contracting_client.create_contract = MagicMock(return_value={'data': ['test1', 'test2']})
    #     cb.sync_single_tender(self.TENDER_ID)

    #     calls_logs = mocks[1].info.call_args_list

    #     self.assertEqual(
    #         self._get_calls_count(calls_logs, call('Getting tender {}'.format(self.TENDER_ID))), 2
    #     )
    #     self.assertEqual(
    #         self._get_calls_count(
    #             calls_logs, call('Got tender {} in status {}'.format(self.TENDER_ID, tender['status']))
    #         ),  2
    #     )
    #     self.assertEqual(
    #         self._get_calls_count(calls_logs, call('Getting tender {} credentials'.format(self.TENDER_ID))), 2
    #     )
    #     self.assertEqual(
    #         self._get_calls_count(calls_logs, call('Got tender {} credentials'.format(self.TENDER_ID))), 2
    #     )
    #     self.assertEqual(
    #         self._get_calls_count(calls_logs, call('Checking if contract {} already exists'.format(contract['id']))), 1
    #     )
    #     self.assertEqual(
    #         self._get_calls_count(
    #             calls_logs, call('Contract {} does not exists. Prepare contract for creation.'.format(contract['id']))
    #         ), 1
    #     )
    #     self.assertEqual(
    #         self._get_calls_count(calls_logs, call('Extending contract {} with extra data'.format(contract['id']))), 1
    #     )
    #     self.assertEqual(self._get_calls_count(calls_logs, call('Creating contract {}'.format(contract['id']))), 1)
    #     self.assertEqual(self._get_calls_count(calls_logs, call('Contract {} created'.format(contract['id']))), 1)
    #     self.assertEqual(
    #         self._get_calls_count(
    #             calls_logs, call('Successfully transferred contracts: [u\'{}\']'.format(contract['id']))
    #         ), 1
    #     )

    # def test_sync_single_tender_Exception(self, *mocks):
    #     cb = ContractingDataBridge(self.config)
    #     tender = deepcopy(self.tender)
    #     tender['status'] = 'active'
    #     contract = deepcopy(self.contract)
    #     contract['status'] = 'active'
    #     cb.tenders_sync_client.get_tender = MagicMock(return_value={'data': tender})
    #     cb.get_tender_credentials = MagicMock(
    #         return_value={'data': {'procuringEntity': 'procuringEntity', 'tender_token': 'tender_token'}}
    #     )

    #     cb.sync_single_tender(self.TENDER_ID)
    #     calls_logs = mocks[1].info.call_args_list

    #     self.assertEqual(self._get_calls_count(calls_logs, call('Contract exists {}'.format(contract['id']))), 1)

    #     error = Exception('Error!')
    #     cb.contracting_client.get_contract = MagicMock(side_effect=error)

    #     with self.assertRaises(Exception) as e:
    #         cb.sync_single_tender(self.TENDER_ID)
    #     mocks[1].exception.assert_called_once_with(e.exception)

    # def test_retry_put_contracts(self, *mocks):
    #     true_list = [True, False]
    #     mocks[0].__nonzero__.side_effect = true_list

    #     contract = deepcopy(self.contract)
    #     contract['tender_id'] = self.TENDER_ID

    #     cb = ContractingDataBridge(self.config)
    #     remember_put_with_retry = cb._put_with_retry
    #     cb.contracts_retry_put_queue = MagicMock()
    #     cb._put_with_retry = MagicMock()
    #     cb.cache_db = MagicMock()
    #     cb._put_tender_in_cache_by_contract = MagicMock()
    #     cb.contracts_retry_put_queue.get.return_value = contract

    #     cb.retry_put_contracts()

    #     cb.contracts_retry_put_queue.get.assert_called_once_with()
    #     cb._put_with_retry.assert_called_once_with(contract)
    #     cb.cache_db.put.assert_called_once_with(contract['id'], True)
    #     cb._put_tender_in_cache_by_contract.assert_called_once_with(contract, contract['tender_id'])
    #     mocks[2].sleep.assert_called_once_with(0)

    #     cb._put_with_retry = remember_put_with_retry
    #     mocks[0].__nonzero__.side_effect = true_list
    #     e = Exception('Exception!')
    #     cb.contracting_client.create_contract = MagicMock(side_effect=[e, True])
    #     contract = munch.munchify(contract)
    #     cb.contracts_retry_put_queue.get.return_value = contract
    #     cb.retry_put_contracts()

    #     mocks[1].exception.assert_called_once_with(e)

    # def test_put_contracts(self, *mocks):
    #     list_loop = [True, False]
    #     mocks[0].__nonzero__.side_effect = list_loop
    #     contract = deepcopy(self.contract)
    #     contract['tender_id'] = self.TENDER_ID
    #     contract = munch.munchify(contract)

    #     cb = ContractingDataBridge(self.config)
    #     cb.contracts_put_queue = MagicMock()
    #     cb.contracts_put_queue.get.return_value = contract
    #     cb.contracting_client = MagicMock()
    #     cb.contracts_retry_put_queue = MagicMock()
    #     cb.contracting_client_init = MagicMock()
    #     cb.cache_db = MagicMock()
    #     cb._put_tender_in_cache_by_contract = MagicMock()

    #     cb.put_contracts()

    #     cb.contracts_put_queue.get.assert_called_once_with()
    #     cb.contracting_client.create_contract.assert_called_once_with({'data': contract.toDict()})
    #     cb.cache_db.put.assert_called_once_with(contract.id, True)
    #     cb._put_tender_in_cache_by_contract.assert_called_once_with(
    #         contract.toDict(), contract.tender_id)
    #     mocks[2].sleep.assert_called_once_with(0)

    #     list_contracts = []

    #     for i in range(0, 10):
    #         contract = deepcopy(self.contract)
    #         contract['id'] = i
    #         contract['tender_id'] = i + 100
    #         list_contracts.append(contract)

    #     cb.contracts_put_queue = MagicMock()
    #     cb.contracts_put_queue.get.side_effect = list_contracts
    #     list_loop = [True for i in range(0, 10)]
    #     list_loop.append(False)
    #     mocks[0].__nonzero__.side_effect = list_loop

    #     cb.put_contracts()

    #     extract_calls = [data[0] for data, call in cb.contracts_retry_put_queue.put.call_args_list]
    #     for i in range(0, 10):
    #         assert extract_calls[i]['id'] == i
    #     self.assertEqual(len(extract_calls), 10)
    #     cb.contracting_client_init.assert_called_once_with()

    # def test_prepare_contract_data_retry(self, *mocks):
    #     true_list = [True, False]
    #     mocks[0].__nonzero__.side_effect = true_list

    #     cb = ContractingDataBridge(self.config)
    #     contract = deepcopy(self.contract)
    #     contract['tender_id'] = self.TENDER_ID
    #     tender_data = MagicMock()
    #     tender_data.data = deepcopy(self.tender)
    #     tender_data.data.update(self.owner_and_token)
    #     cb.handicap_contracts_queue_retry.get = MagicMock(return_value=contract)
    #     cb.get_tender_data_with_retry = MagicMock(return_value=tender_data)
    #     cb.prepare_contract_data_retry()
    #     self.assertEquals(cb.contracts_put_queue.qsize(), 1)
    #     self.assertEquals(cb.contracts_put_queue.get(), contract)

    # def test_prepare_contract_data_retry_with_exception(self, *mocks):
    #     true_list = [True, False]
    #     mocks[0].__nonzero__.side_effect = true_list

    #     cb = ContractingDataBridge(self.config)
    #     contract = deepcopy(self.contract)
    #     contract['tender_id'] = self.TENDER_ID
    #     cb.handicap_contracts_queue_retry.get = MagicMock(return_value=contract)
    #     e = Exception("Error!!! prepare_contract_data_retry")
    #     cb.get_tender_data_with_retry = MagicMock(side_effect=e)
    #     cb.prepare_contract_data_retry()
    #     mocks[1].exception.assert_called_with(e)

    # def test_prepare_contract_data(self, *mocks):
    #     true_list = [True, False]
    #     mocks[0].__nonzero__.side_effect = true_list
    #     cb = ContractingDataBridge(self.config)
    #     contract = deepcopy(self.contract)
    #     contract['tender_id'] = self.TENDER_ID

    #     tender_data = MagicMock()
    #     tender_data.data = deepcopy(self.tender)
    #     tender_data.data.update(self.owner_and_token)

    #     cb.handicap_contracts_queue.get = MagicMock(return_value=contract)
    #     cb.get_tender_credentials = MagicMock(return_value=tender_data)
    #     cb.prepare_contract_data()
    #     self.assertEquals(cb.contracts_put_queue.qsize(), 1)
    #     self.assertEquals(cb.contracts_put_queue.get(), contract)

    # def test_prepare_contract_data_with_exception(self, *mocks):
    #     static_number = 12
    #     true_list = [True for i in xrange(0, static_number)]
    #     true_list.append(False)
    #     mocks[0].__nonzero__.side_effect = true_list

    #     cb = ContractingDataBridge(self.config)

    #     for i in range(static_number):
    #         cb.handicap_contracts_queue.put({'id': i, 'tender_id': i + 1111})

    #     tender_data = MagicMock()
    #     tender_data.data = deepcopy(self.tender)
    #     tender_data.data.update({'no_owner': '', 'no_tender_token': ''})

    #     cb.get_tender_credentials = MagicMock(return_value=tender_data)

    #     cb.prepare_contract_data()
    #     list_calls = mocks[2].sleep.call_args_list
    #     calls_logs = mocks[1].info.call_args_list

    #     calls_with_error_delay = call(float(cb.on_error_delay))
    #     self.assertEqual(self._get_calls_count(list_calls, calls_with_error_delay), static_number)

    #     reconnecting_log = call(
    #         'Reconnecting tenders client',
    #         extra={'JOURNAL_TENDER_ID': 1120, 'MESSAGE_ID': j_msg.DATABRIDGE_RECONNECT, 'JOURNAL_CONTRACT_ID': 9}
    #     )
    #     self.assertEqual(self._get_calls_count(calls_logs, reconnecting_log), 1)

    # def test_get_tender_contracts_resource_gone(self, *mocks):
    #     error_msg = {
    #         'status': 'error',
    #         'errors': [
    #             {
    #                 'location': 'url',
    #                 'name': 'contract_id',
    #                 'description': 'Archived'
    #             }
    #         ]
    #     }
    #     resp = MagicMock()
    #     resp.body_string.return_value = json.dumps(error_msg)
    #     resp.text = json.dumps(error_msg)
    #     resp.status_code = 410
    #     resp.status_int = 410
    #     cb = ContractingDataBridge(self.config)
    #     tender_to_sync = {'id': self.TENDER_ID, 'dateModified': datetime.now().isoformat()}
    #     tender = {'data': deepcopy(self.tender)}
    #     tender['data']['contracts'] = [deepcopy(self.contract)]
    #     cb.tenders_queue.put(tender_to_sync)
    #     cb.cache_db = MagicMock()
    #     cb.cache_db.has.return_value = False
    #     cb.tenders_sync_client = MagicMock()
    #     cb.tenders_sync_client.get_tender.return_value = tender
    #     exception = ResourceGone(response=resp)
    #     cb.contracting_client_ro = MagicMock()
    #     cb.contracting_client_ro.get_contract.side_effect = [exception]
    #     cb._get_tender_contracts()
    #     logger_msg = 'Sync contract {} of tender {} has been archived'.format(self.contract['id'], self.TENDER_ID)
    #     extra = {
    #         'JOURNAL_TENDER_ID': tender['data']['id'],
    #         'MESSAGE_ID': j_msg.DATABRIDGE_CONTRACT_TO_SYNC,
    #         'JOURNAL_CONTRACT_ID': self.contract['id']
    #     }
    #     mocks[1].info.assert_has_calls([call(logger_msg, extra=extra)])

    #     #  check if no contracts in tender, raises exception
    #     cb.tenders_queue = MagicMock()
    #     cb.tenders_queue.get.return_value = {'id': 'id'}
    #     del tender['data']['contracts']
    #     cb._get_tender_contracts()
    #     mocks[1].warn.assert_has_calls([
    #         call('!!!No contracts found in tender {}'.format(self.TENDER_ID), extra={
    #             'MESSAGE_ID': j_msg.DATABRIDGE_EXCEPTION, 'JOURNAL_TENDER_ID': self.TENDER_ID
    #         })]
    #     )

    #     # when exceptions are raised
    #     cb.tenders_sync_client.get_tender.side_effect = Exception()
    #     cb.tenders_sync_client.get_tender.return_value = MagicMock()
    #     cb._get_tender_contracts()
    #     mocks[1].warn.called_once_with(
    #         'Fail to get tender info id', extra={'MESSAGE_ID': j_msg.DATABRIDGE_EXCEPTION, 'JOURNAL_TENDER_ID': 'id'}
    #     )
    #     mocks[1].info.called_once_with(
    #         'Put tender id back to tenders queue', 
    #         extra={'MESSAGE_ID': j_msg.DATABRIDGE_EXCEPTION, 'JOURNAL_TENDER_ID': 'id'}
    #     )
    
    # def test_initialize_sync(self, *mocks):
    #     body_string = '{"prev_page": {"offset": 0}, "next_page": {"offset": 1} }'
    #     mocks[3].return_value = MockedResponse(
    #         status_int=200, headers={'Set-Cookie': 'fake_cookie'}, body_string=body_string
    #     )
    #     cb = ContractingDataBridge(self.config)
    #     response = cb.initialize_sync(
    #         params={'descending': True}, direction='backward'
    #     )
    
    #     self.assertEquals(
    #         str(response), "Munch({'next_page': Munch({'offset': 1}), 'prev_page': Munch({'offset': 0})})"
    #     )

    #     mocks[1].info.assert_has_calls(call(
    #         'Initial sync point {}'.format({'forward_offset': 0, 'backward_offset': 1}),
    #     ))
    #     response = cb.initialize_sync(params={})
    #     self.assertEquals(
    #         str(response), "Munch({'next_page': Munch({'offset': 1}), 'prev_page': Munch({'offset': 0})})"
    #     )
    #     mocks[1].info.assert_has_calls(call('Starting forward sync from offset {}'.format(0)))
    
    # def _fake_response(self, status=None):
    #     class Empty:
    #         pass
    
    #     response = Empty()
    #     response.data = list()
    #     response.next_page = Empty()
    #     response.next_page.offset = True
    #     return response
    
    # def test_get_tenders(self, *mocks):

    #     info_calls = list()
    #     debug_calls = list()

    #     cb = ContractingDataBridge(self.config)
    #     # Check initialization
    #     msg = "Caching backend: '{}', db name: '{}', host: '{}', port: '{}'".format(
    #         cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host, cb.cache_db._port
    #     )
    #     info_calls += [
    #         call(msg, extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO}),
    #         call('Initialization contracting clients.', extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO})
    #     ]

    #     response = self._fake_response()
    #     competitiveDialogueUA = deepcopy(self.tender)
    #     competitiveDialogueUA['procurementMethodType'] = 'competitiveDialogueUA'
    #     competitiveDialogueEU = deepcopy(self.tender)
    #     competitiveDialogueEU['procurementMethodType'] = 'competitiveDialogueEU'
    #     pending = deepcopy(self.tender)
    #     pending['status'] = 'pending'
    #     complete = deepcopy(self.tender)
    #     complete['status'] = 'complete'
    #     complete_lots = deepcopy(self.tender)
    #     complete_lots['status'] = 'complete'
    #     complete_lots['lots'] = [{'status': 'complete'}]
    #     tenders = [
    #         munchify(competitiveDialogueUA),
    #         munchify(competitiveDialogueEU),
    #         munchify(pending),
    #         munchify(complete),
    #         munchify(complete_lots),
    #     ]
    #     response.data = tenders
    #     params = {'descending': True, 'offset': True}
    #     direction = self.DIRECTION

    #     cb.initialize_sync = MagicMock(return_value=response)
    #     cb.tenders_sync_client = MagicMock()
    #     cb.tenders_sync_client.sync_tenders = MagicMock(
    #         return_value=self._fake_response()
    #     )

    #     for _ in cb.get_tenders(params=params, direction=direction):
    #         pass

    #     info_calls += [
    #         call('Client {} params: {}'.format(direction, params)),
    #         call(
    #             'Skipping {} tender {}'.format(tenders[0]['procurementMethodType'], tenders[0]['id']),
    #             extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_INFO}, params={'TENDER_ID': tenders[0]['id']})
    #         ),
    #         call(
    #             'Skipping {} tender {}'.format(tenders[1]['procurementMethodType'], tenders[1]['id']),
    #             extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_INFO}, params={'TENDER_ID': tenders[1]['id']})
    #         ),
    #         call(
    #             '{} sync: Found tender in complete status {}'.format(direction.capitalize(), tenders[3]['id']),
    #              extra=journal_context(
    #                 {'MESSAGE_ID': j_msg.DATABRIDGE_FOUND_NOLOT_COMPLETE}, {'TENDER_ID': tenders[3]['id']}
    #             )
    #         ),
    #         call(
    #             '{} sync: Found multilot tender {} in status {}'.format(
    #                 direction.capitalize(), tenders[4]['id'], tenders[4]['status']
    #             ),
    #             extra=journal_context(
    #                 {'MESSAGE_ID': j_msg.DATABRIDGE_FOUND_MULTILOT_COMPLETE},
    #                 {'TENDER_ID': tenders[4]['id']}
    #             )
    #         ),
    #         call(
    #             'Sleep {} sync...'.format(direction),
    #             extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_SYNC_SLEEP})
    #         ),
    #         call(
    #             'Restore {} sync'.format(direction),
    #             extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_SYNC_RESUME})
    #         )
    #     ]
    #     debug_calls += [
    #         call('{} sync: Skipping tender {} in status {}'.format(
    #             direction.capitalize(), tenders[2]['id'], tenders[2]['status']
    #         ), extra=journal_context(params={"TENDER_ID": tenders[2]['id']})), call('{} {}'.format(direction, params))
    #     ]

    #     self.assertEqual(mocks[1].debug.mock_calls, debug_calls)
    #     self.assertEqual(mocks[1].info.mock_calls, info_calls)
    
    # def _fake_generator(self, flag, items=None):
    #     if flag:
    #         for item in items:
    #             yield item
    #     else:
    #         raise Exception()
    
    # def test_get_tender_contracts_forward(self, *mocks):
    #     info_calls = []
    #     warn_calls = []
    
    #     cb = ContractingDataBridge(self.config)
    #     # Check initialization
    #     msg = "Caching backend: '{}', db name: '{}', host: '{}', port: '{}'".format(
    #         cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host, cb.cache_db._port
    #     )
    #     info_calls += [
    #         call(msg, extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO}),
    #         call('Initialization contracting clients.', extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO})
    #     ]
    
    #     cb.get_tenders = MagicMock(return_value=self._fake_generator(True, [{'id': 'some_id'}]))
    #     cb.get_tender_contracts_forward()
    
    #     info_calls += [
    #         call('Start forward data sync worker...'),
    #         call('Forward sync: Put tender {} to process...'.format('some_id'),
    #              extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_TENDER_PROCESS}, {'TENDER_ID': 'some_id'}))
    #     ]
    #     warn_calls += [
    #         call(
    #             'Forward data sync finished!',
    #             extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_WORKER_DIED}, {})
    #         )
    #     ]
    
    #     cb.get_tenders.return_value = self._fake_generator(False)

    #     with self.assertRaises(Exception) as e:
    #         cb.get_tender_contracts_forward()
    #         mocks[1].exception.assert_called_once_with(e)
    
    #     info_calls += [call('Start forward data sync worker...')]
    #     warn_calls += [call(
    #         'Forward worker died!', extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_WORKER_DIED}, {})
    #     )]
    
    #     self.assertEqual(mocks[1].info.mock_calls, info_calls)
    #     self.assertEqual(mocks[1].warn.mock_calls, warn_calls)
    
    # def test_get_tender_contracts_backward(self, *mocks):
    
    #     info_calls = []
    
    #     cb = ContractingDataBridge(self.config)
    #     # Check initialization
    #     msg = "Caching backend: '{}', db name: '{}', host: '{}', port: '{}'".format(
    #         cb.cache_db._backend, cb.cache_db._db_name, cb.cache_db._host, cb.cache_db._port
    #     )
    #     info_calls += [
    #         call(msg, extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO}),
    #         call('Initialization contracting clients.', extra={'MESSAGE_ID': j_msg.DATABRIDGE_INFO})
    #     ]
    
    #     tenders = [{'id': 'id{}'.format(i), 'dateModified': bool(i % 2)} for i in range(2)]
    #     cb.get_tenders = MagicMock(return_value=self._fake_generator(True, tenders))
    #     cb.cache_db = MagicMock()
    #     cb.cache_db.get = MagicMock(return_value=True)
    #     cb.get_tender_contracts_backward()
    
    #     info_calls += [
    #         call('Start backward data sync worker...'),
    #         call(
    #             'Backward sync: Put tender {} to process...'.format(tenders[0]['id']),
    #             extra=journal_context(
    #                 {'MESSAGE_ID': j_msg.DATABRIDGE_TENDER_PROCESS}, {'TENDER_ID': tenders[0]['id']}
    #             )
    #         ),
    #         call(
    #             'Tender {} not modified from last check. Skipping'.format(tenders[1]['id']),
    #             extra=journal_context(
    #                 {'MESSAGE_ID': j_msg.DATABRIDGE_SKIP_NOT_MODIFIED}, {'TENDER_ID': tenders[1]['id']}
    #             )
    #         ),
    #         call('Backward data sync finished.')
    #     ]
    
    #     cb.get_tenders.return_value = self._fake_generator(False)

    #     with self.assertRaises(Exception) as e:
    #         cb.get_tender_contracts_backward()
    #         mocks[1].exception.assert_called_once_with(e)
    
    #     info_calls += [call('Start backward data sync worker...')]
    #     mocks[1].warn.assert_called_once_with(
    #         'Backward worker died!', extra=journal_context({'MESSAGE_ID': j_msg.DATABRIDGE_WORKER_DIED}, {})
    #     )
    #     self.assertEqual(mocks[1].info.mock_calls, info_calls)
    
    # def test__get_tender_contracts(self, *mocks):
    #     cb = ContractingDataBridge(self.config)
    #     tender_to_sync = {'id': self.TENDER_ID, 'dateModified': datetime.now().isoformat()}
    #     tender = {'data': deepcopy(self.tender)}
    #     tender['data']['contracts'] = [deepcopy(self.contract)]
    #     cb.tenders_queue.put(tender_to_sync)
    #     cb.cache_db = MagicMock()
    #     cb.cache_db.has.return_value = False
    #     cb.tenders_sync_client = MagicMock()
    #     cb.tenders_sync_client.get_tender.return_value = tender
    #     exception = IndexError()
    #     cb.contracting_client_ro = MagicMock()
    #     cb.contracting_client_ro.get_contract.side_effect = [exception]
    #     cb._put_tender_in_cache_by_contract = MagicMock()
    
    #     with self.assertRaises(Exception) as e:
    #         cb._get_tender_contracts()

    #     mocks[1].warn.assert_called_once_with(
    #         'Fail to contract existance {}'.format(self.contract['id']),
    #         extra=journal_context({
    #             'MESSAGE_ID': j_msg.DATABRIDGE_EXCEPTION,
    #             'JOURNAL_TENDER_ID': self.TENDER_ID,
    #             'JOURNAL_CONTRACT_ID': self.contract['id']
    #         })
    #     )
    #     mocks[1].exception.assert_called_once_with(exception)
    #     mocks[1].info.assert_has_calls([
    #         call(
    #             'Put tender {} back to tenders queue'.format(self.TENDER_ID),
    #             extra={
    #                 'JOURNAL_TENDER_ID': self.TENDER_ID,
    #                 'MESSAGE_ID': j_msg.DATABRIDGE_EXCEPTION,
    #                 'JOURNAL_CONTRACT_ID': self.contract['id']
    #             }
    #         )
    #     ])
    
    #     # no exception is raised, so else is executed
    #     cb.contracting_client_ro.get_contract = MagicMock()
    #     cb._get_tender_contracts()
    #     mocks[1].info.assert_has_calls([
    #         call(
    #             'Contract exists {}'.format(self.contract['id']),
    #             extra={
    #                 'JOURNAL_TENDER_ID': self.TENDER_ID,
    #                 'MESSAGE_ID': j_msg.DATABRIDGE_CONTRACT_EXISTS,
    #                 'JOURNAL_CONTRACT_ID': self.contract['id']
    #             }
    #         )
    #     ])
    #     cb._put_tender_in_cache_by_contract.assert_called_once_with(tender['data']['contracts'][0], self.TENDER_ID)

   


class TestDb(unittest.TestCase):
    def setUp(self):
        config = SafeConfigParser()
        config.add_section('app:api')
        config.set('app:api', 'cache_db_name', '0')
        config.set('app:api', 'cache_host', '127.0.0.1')
        config.set('app:api', 'cache_port', '6379')

        redis_mock = MagicMock()
        redis_mock.StrictRedis.return_value = MagicMock()
        sys.modules['redis'] = redis_mock
        self.db = CacheDB(config)
        self.db.db = dict()

        def set_value(key, value):
            self.db.db[key] = value

        self.db.set_value = set_value
        self.db.has_value = lambda x: x in self.db.db

    def test_cache_host_not_in_config(self):
        config = SafeConfigParser()
        config.add_section('app:api')
        config.set('app:api', 'cache_db_name', '0')
        config.set('app:api', 'cache_port', '6379')

        StrictRedis_mock = MagicMock()
        StrictRedis_mock.configure_mock(**{'set': None, 'exists': None})
        redis_mock = MagicMock()
        redis_mock.StrictRedis.return_value = StrictRedis_mock
        sys.modules['redis'] = redis_mock
        db = CacheDB(config)

        self.assertEqual(db._backend, 'redis')
        self.assertEqual(db._db_name, config.get('app:api', 'cache_db_name'))
        self.assertEqual(db._port, config.get('app:api', 'cache_port'))

    def test_cache_host_in_config(self):
        config = SafeConfigParser()
        config.add_section('app:api')
        config.set('app:api', 'cache_db_name', '0')
        config.set('app:api', 'cache_host', '127.0.0.1')
        config.set('app:api', 'cache_port', '6379')

        sys.modules['redis'] = MagicMock()
        db = CacheDB(config)

        self.assertEqual(db._backend, 'redis')
        self.assertEqual(db._db_name, config.get('app:api', 'cache_db_name'))
        self.assertEqual(db._host, config.get('app:api', 'cache_host'))
        self.assertEqual(db._port, config.get('app:api', 'cache_port'))

    def test_get(self):
        self.assertEquals(self.db.get('test'), None)
        self.db.set_value('test', 'test')
        self.assertEquals(self.db.get('test'), 'test')

    def test_put(self):
        self.db.put('test_put', 'test_put')
        self.assertEquals(self.db.get('test_put'), 'test_put')

    def test_has(self):
        self.assertEquals(self.db.has('test_has'), False)
        self.db.set_value('test_has', 'test_has')
        self.assertEquals(self.db.has('test_has'), True)


class TestDatabridgeFunctions(unittest.TestCase):

    def test_generate_req_id(self):
        id = generate_request_id()
        self.assertEquals(len(id), 52)
        self.assertEquals(id.startswith('data-bridge-req-'), True)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDatabridge))
    suite.addTest(unittest.makeSuite(TestDb))
    suite.addTest(unittest.makeSuite(TestDatabridgeFunctions))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
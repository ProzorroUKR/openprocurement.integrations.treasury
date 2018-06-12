# -*- coding: utf-8 -*-
import logging.config

from datetime import datetime

import gevent
from gevent import spawn
from gevent.event import Event
from openprocurement.integrations.treasury.databridge.base_worker import BaseWorker
from openprocurement.integrations.treasury.databridge.constants import retry_mult
from openprocurement.integrations.treasury.databridge.journal_msg_ids import DATABRIDGE_INFO, DATABRIDGE_SYNC_SLEEP, \
    DATABRIDGE_CONTRACT_PROCESS, DATABRIDGE_WORKER_DIED
from openprocurement.integrations.treasury.databridge.utils import journal_context, generate_request_id, \
    more_contracts, valid_contract, document_of_change
from openprocurement.integrations.treasury.databridge.caching import contract_key
from restkit import ResourceError
from retrying import retry

logger = logging.getLogger(__name__)


class ContractScanner(BaseWorker):
    """ Edr API Data Bridge """

    def __init__(self, contracts_client, filtered_contracts_queue, services_not_available, process_tracker,
                 sleep_change_value, delay=15):
        super(ContractScanner, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.delay = delay
        # init clients
        self.contracts_client = contracts_client

        # init queues for workers
        self.filtered_contracts_queue = filtered_contracts_queue

        self.process_tracker = process_tracker

        # blockers
        self.initialization_event = Event()
        self.sleep_change_value = sleep_change_value

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=retry_mult)
    def initialize_sync(self, params=None, direction=None):
        if direction == "backward":
            self.initialization_event.clear()
            assert params['descending']
            logger.info('params are weird, check them out? params={}'.format(params))
            response = self.contracts_client.get_contracts(params,
                                                             extra_headers={'X-Client-Request-ID': generate_request_id()})
            # set values in reverse order due to 'descending' option
            logger.info('response.prev_page: {}'.format(response.prev_page))
            self.initial_sync_point = {'forward_offset': response.prev_page.offset,
                                       'backward_offset': response.next_page.offset}
            self.initialization_event.set()  # wake up forward worker
            logger.info("Initial sync point {}".format(self.initial_sync_point))
            return response
        else:
            assert 'descending' not in params
            self.initialization_event.wait()
            params['offset'] = self.initial_sync_point['forward_offset']
            logger.info("Starting forward sync from offset {}".format(params['offset']))
            return self.contracts_client.get_contracts(params,
                                                        extra_headers={'X-Client-Request-ID': generate_request_id()})

    def get_contracts(self, params={}, direction=""):
        response = self.initialize_sync(params=params, direction=direction)

        while more_contracts(params, response):
            contracts = response.data if response else []
            params['offset'] = response.next_page.offset
            for contract in contracts:
                # logger.info('contract {}'.format(contract))
                if self.should_process_contract(contract):
                    yield contract
                else:
                    logger.info('Skipping contract {} with status {}'.format(
                        contract['id'], contract['status']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_INFO},
                                            params={"contract_ID": contract['id']}))
            logger.info('Sleep {} sync...'.format(direction),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_SYNC_SLEEP}))
            gevent.sleep(self.delay + self.sleep_change_value.time_between_requests)
            try:
                response = self.contracts_client.get_contracts(params, extra_headers={
                    'X-Client-Request-ID': generate_request_id()})
                self.sleep_change_value.decrement()
            except ResourceError as re:
                if re.status_int == 429:
                    self.sleep_change_value.increment()
                    logger.info("Received 429, will sleep for {}".format(self.sleep_change_value.time_between_requests))
                else:
                    raise re

    def should_process_contract(self, contract):
        return (not self.process_tracker.check_processed_contracts(contract['id']) and
                (valid_contract(contract)))

    def get_contracts_forward(self):
        self.services_not_available.wait()
        logger.info('Start forward data sync worker...')
        params = {'opt_fields': 'status, changes, documents, dateSigned', 'mode': '_all_', 'feed': 'changes'}
        try:
            self.put_contracts_to_process(params, "forward")
        except Exception as e:
            logger.warning('Forward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
        else:
            logger.warning('Forward data sync finished!')

    def get_contracts_backward(self):
        self.services_not_available.wait()
        logger.info('Start backward data sync worker...')
        params = {'opt_fields': 'status, changes, documents, dateSigned', 'descending': 1, 'mode': '_all_', 'feed': 'changes'}
        try:
            self.put_contracts_to_process(params, "backward")
        except Exception as e:
            logger.warning('Backward worker died!', extra=journal_context({"MESSAGE_ID": DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
            return False
        else:
            logger.info('Backward data sync finished.')
            return True

    def put_contracts_to_process(self, params, direction):
        for contract in self.get_contracts(params=params, direction=direction):
            logger.info('Backward sync: Put contract {} to process...'.format(contract['id']),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_CONTRACT_PROCESS},
                                              {"contract_ID": contract['id']}))
            self.process_tracker.put_contract(contract['id'], contract['dateModified'])
            
            self.filtered_contracts_queue.put(contract)

    def _start_jobs(self):
        return {'get_contracts_backward': spawn(self.get_contracts_backward),
                'get_contracts_forward': spawn(self.get_contracts_forward)}

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead and not job.value:
                self.revive_job(name)

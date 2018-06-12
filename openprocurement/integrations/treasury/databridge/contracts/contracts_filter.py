# # -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import gevent
import logging
from gevent import spawn
from openprocurement.integrations.treasury.databridge.base_worker import BaseWorker
from datetime import datetime
from openprocurement.integrations.treasury.databridge.utils import (
    generate_request_id, journal_context, document_of_change,
    fill_base_contract_data)
from openprocurement.integrations.treasury.databridge import journal_msg_ids
try:
    from openprocurement_client.exceptions import ResourceGone, ResourceNotFound
except ImportError:
    from openprocurement_client.client import ResourceNotFound
    from restkit.errors import ResourceGone


logger = logging.getLogger(__name__)


class ContractFilter(BaseWorker):

    def __init__(self, resource, contracts_sync_client, contracting_client, contracting_client_ro, filtered_contracts_queue, process_tracker, cache_db,
                 services_not_available, sleep_change_value, delay=15):
        super(ContractFilter, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.resource = resource
        self.delay = delay
        self.process_tracker = process_tracker
        self.cache_db = cache_db
        # init clients
        self.contracts_sync_client = contracts_sync_client
        self.contracting_client = contracting_client
        self.contracting_client_ro = contracting_client_ro

        # init queues for workers
        self.filtered_contracts_queue = filtered_contracts_queue
        self.sleep_change_value = sleep_change_value
        self.basket = dict()

    def get_contracts_worker(self):
        logger.info('internal contracts worker')
        while True:
            try:
                self.get_contract()
            except Exception as e:
                logger.warn('Fail to handle contract contracts',
                    extra=journal_context({'MESSAGE_ID': journal_msg_ids.DATABRIDGE_EXCEPTION}, {}))
                logger.exception(e)
                gevent.sleep(self.sleep_change_value.time_between_requests)
                raise
            gevent.sleep(1)

    def get_contract(self):
        contract_to_sync = self.filtered_contracts_queue.get()
        logger.info('Getting a contract {}'.format(contract_to_sync))
        try:
            contract = self.contracts_sync_client.get_contract(
                contract_to_sync['id'],
                extra_headers={'X-Client-Request-ID': generate_request_id()}
            )['data']
        except Exception as e:
            logger.warn(
                'Fail to handle contract contracts',
                extra=journal_context({'MESSAGE_ID': journal_msg_ids.DATABRIDGE_EXCEPTION}, {})
            )
            logger.exception(e)
            gevent.sleep(self.sleep_change_value.time_between_requests)
            raise
        self._put_contract_in_cache_by_contract(contract, contract_to_sync['id'])
        gevent.sleep(1)

    def _put_contract_in_cache_by_contract(self, contract, contract_id):
        # logger.info('Putting info forward {}'.format(contract))
        data_to_put_forward = (contract['id'], contract['dateSigned'], document_of_change(contract))
        logger.info('data to put forward {}'.format(data_to_put_forward))
        date_modified = self.basket.get(contract['id'])
        if date_modified:
            self.cache_db.put(contract_id, date_modified)
        self.basket.pop(contract['id'], None)

    def _start_jobs(self):
        return {'get_contracts_worker': spawn(self.get_contracts_worker)}

# # -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import gevent
import logging
from gevent import spawn
from openprocurement.integrations.treasury.databridge.base_worker import BaseWorker
from datetime import datetime
from openprocurement.integrations.treasury.databridge.utils import (
    generate_request_id, journal_context,
    fill_base_contract_data, handle_common_plans, handle_esco_plans)
from openprocurement.integrations.treasury.databridge import journal_msg_ids
try:
    from openprocurement_client.exceptions import ResourceGone, ResourceNotFound
except ImportError:
    from openprocurement_client.client import ResourceNotFound
    from restkit.errors import ResourceGone


logger = logging.getLogger(__name__)


class PlanFilter(BaseWorker):

    def __init__(self, resource, plans_sync_client, contracting_client, contracting_client_ro, filtered_plans_queue, process_tracker, cache_db,
                 services_not_available, sleep_change_value, delay=15):
        super(PlanFilter, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.resource = resource
        self.delay = delay
        self.process_tracker = process_tracker
        self.cache_db = cache_db
        # init clients
        self.plans_sync_client = plans_sync_client
        self.contracting_client = contracting_client
        self.contracting_client_ro = contracting_client_ro

        # init queues for workers
        self.filtered_plans_queue = filtered_plans_queue
        self.sleep_change_value = sleep_change_value
        self.basket = dict()
    
    
    def _put_plan_in_cache_by_contract(self, contract, plan_id):
        date_modified = self.basket.get(contract['id'])

        if date_modified:
            self.cache_db.put(plan_id, date_modified)

        self.basket.pop(contract['id'], None)

    def _start_jobs(self):
        return {'get_plan_contracts': spawn(self.get_plan_contracts)}
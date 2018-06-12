# -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import logging.config

from datetime import datetime

import gevent
from gevent import spawn
from gevent.event import Event
from restkit import ResourceError
from openprocurement.integrations.treasury.databridge.utils import (
    CacheDB,
    generate_request_id,
    fill_base_contract_data,
    more_plans,
    journal_context,
    valid_plan
)


from openprocurement.integrations.treasury.databridge.base_worker import BaseWorker
from openprocurement.integrations.treasury.databridge import constants
from openprocurement.integrations.treasury.databridge.caching import plan_key
from openprocurement.integrations.treasury.databridge import journal_msg_ids
from retrying import retry

logger = logging.getLogger(__name__)


class PlanScanner(BaseWorker):
    """ Edr API Data Bridge """

    def __init__(self, plans_sync_client, filtered_plans_queue, services_not_available, process_tracker,
                 sleep_change_value, delay=15):
        super(PlanScanner, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.delay = delay
        # init clients
        self.plans_sync_client = plans_sync_client

        # init queues for workers
        self.filtered_plans_queue = filtered_plans_queue

        self.process_tracker = process_tracker

        # blockers
        self.initialization_event = Event()
        self.sleep_change_value = sleep_change_value
        logger.info('Plans scanner successfullly initialized')

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=constants.retry_mult)
    def initialize_sync(self, params=None, direction=None):
        if direction == "backward":
            self.initialization_event.clear()
            assert params['descending']
            response = self.plans_sync_client.sync_plans(params,
                                                             extra_headers={'X-Client-Request-ID': generate_request_id()})
            # set values in reverse order due to 'descending' option
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
            return self.plans_sync_client.sync_plans(params,
                                                         extra_headers={'X-Client-Request-ID': generate_request_id()})

    def get_plans(self, params={}, direction=""):
        response = self.initialize_sync(params=params, direction=direction)

        while more_plans(params, response):
            plans = response.data if response else []
            params['offset'] = response.next_page.offset
            for plan in plans:
                if self.should_process_plan(plan):
                    yield plan
                else:
                    logger.info('Skipping plan {} '.format(
                        plan['id']),
                        extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_INFO},
                                              params={"TENDER_ID": plan['id']}))
            logger.info('Sleep {} sync...'.format(direction),
                        extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_SYNC_SLEEP}))
            gevent.sleep(self.delay + self.sleep_change_value.time_between_requests)
            try:
                response = self.plans_sync_client.sync_plans(params, extra_headers={
                    'X-Client-Request-ID': generate_request_id()})
                self.sleep_change_value.decrement()
            except ResourceError as re:
                if re.status_int == 429:
                    self.sleep_change_value.increment()
                    logger.info("Received 429, will sleep for {}".format(self.sleep_change_value.time_between_requests))
                else:
                    raise re

    def should_process_plan(self, plan):
        return not self.process_tracker.check_processed_plans(plan['id']) and (valid_plan(plan))

    def get_plans_forward(self):
        self.services_not_available.wait()
        logger.info('Start forward data sync worker...')
        params = {'mode': '_all_'}
        try:
            self.put_plans_to_process(params, "forward")
        except Exception as e:
            logger.warning('Forward worker died!', extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
        else:
            logger.warning('Forward data sync finished!')

    def get_plans_backward(self):
        self.services_not_available.wait()
        logger.info('Start backward data sync worker...')
        params = {'descending': 1, 'mode': '_all_'}
        try:
            self.put_plans_to_process(params, "backward")
        except Exception as e:
            logger.warning('Backward worker died!', extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
            return False
        else:
            logger.info('Backward data sync finished.')
            return True

    def put_plans_to_process(self, params, direction):
        for plan in self.get_plans(params=params, direction=direction):
            logger.info('{} sync: Put plan {} to process...'.format(direction, plan['id']),
                        extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_TENDER_PROCESS},
                                              {"TENDER_ID": plan['id']}))
            self.process_tracker.put_plan(plan['id'], plan['dateModified'])
            self.filtered_plans_queue.put(plan)

    def _start_jobs(self):
        logger.info('starting jobs')
        return {'get_plans_backward': spawn(self.get_plans_backward),
                'get_plans_forward': spawn(self.get_plans_forward)}

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead and not job.value:
                self.revive_job(name)

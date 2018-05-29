# # -*- coding: utf-8 -*-
# """A scanner for tenders from openprocurement api
# """

# import gevent
# import logging.config

# from datetime import datetime
# from .base_worker import BaseWorker
# from gevent import spawn
# from gevent.event import Event
# from retrying import retry
# from openprocurement.integrations.treasury.databridge.utils import (
#     CacheDB,
#     generate_request_id,
#     fill_base_contract_data,
#     handle_common_tenders,
#     handle_esco_tenders
# )


# from openprocurement.integrations.treasury.databridge import constants
# from openprocurement.integrations.treasury.databridge.utils import journal_context
# from openprocurement.integrations.treasury.databridge import journal_msg_ids

# logger = logging.getLogger(__name__)


# class TendersScanner(BaseWorker):
#     """An object which scans tenders, instantiated from bridge.py

#     Arguments:
#         object {[type]} -- [description]
#     """

#     def __init__(self, tenders_sync_client, filtered_tender_ids_queue, services_not_available, process_tracker,
#                  sleep_change_value, delay=15):
#         super(TendersScanner, self).__init__()
#         self.start_time = datetime.now()
#         self.delay = delay
#         # init clients
#         self.tenders_sync_client = tenders_sync_client

#         # init queues for workers
#         self.filtered_tender_ids_queue = filtered_tender_ids_queue

#         self.process_tracker = process_tracker

#         # blockers
#         self.initialization_event = Event()
#         self.sleep_change_value = sleep_change_value
    
#     @retry(stop_max_attempt_number=5, wait_exponential_multiplier=constants.retry_mult)
#     def initialize_sync(self, params=None, direction=None):
#         """initialization of the syncronization with openprocurement tenders
        
#         Keyword Arguments:
#             params {list} -- list of parameters (default: {None})
#             direction {str} -- sets direction of reading the tenders (default: {None})
        
#         Returns:
#             [type] -- Response of the openprocurement api
#         """

#         if direction == "backward":
#             self.initialization_event.clear()
#             assert params['descending']
#             response = self.tenders_sync_client.sync_tenders(params,
#                                                              extra_headers={
#                                                                  'X-Client-Request-ID': generate_request_id()})
#             # set values in reverse order due to 'descending' option
#             self.initial_sync_point = {'forward_offset': response.prev_page.offset,
#                                        'backward_offset': response.next_page.offset}
#             self.initialization_event.set()  # wake up forward worker
#             logger.info("Initial sync point {}".format(self.initial_sync_point))
#             return response
#         else:
#             assert 'descending' not in params
#             self.initialization_event.wait()
#             params['offset'] = self.initial_sync_point['forward_offset']
#             logger.info("Starting forward sync from offset {}".format(params['offset']))
#             return self.tenders_sync_client.sync_tenders(params,
#                                                          extra_headers={'X-Client-Request-ID': generate_request_id()})

#     def get_tenders(self, params=None, direction=None):
#         if params is None:
#             params = {}

#         if direction is None:
#             direction = str()

#         response = self.initialize_sync(params=params, direction=direction)

#         conditions = [
#             params.get('descending'), not len(response.data), params.get('offset') == response.next_page.offset
#         ]

#         while not all(conditions):
#             tenders_list = response.data

#             params['offset'] = response.next_page.offset

#             delay = self.empty_stack_sync_delay

#             if tenders_list:
#                 delay = self.full_stack_sync_delay
#                 logger.info('Client {} params: {}'.format(direction, params))

#             for tender in tenders_list:
#                 if tender['status'] in constants.TARGET_TENDER_STATUSES:
#                     if hasattr(tender, 'lots'):
#                         if any([1 for lot in tender['lots'] if lot['status'] == constants.TARGET_LOT_STATUS]):
#                             logger.info(
#                                 '{} sync: Found multilot tender {} in status {}'.format(
#                                     direction.capitalize(),
#                                     tender['id'],
#                                     tender['status']
#                                 ),
#                                 extra=journal_context(
#                                     {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_FOUND_MULTILOT_COMPLETE},
#                                     {self.resource['id_key_upper']: tender['id']}
#                                 )
#                             )
#                             yield tender
#                     elif tender['status'] == 'complete':
#                         logger.info(
#                             '{} sync: Found tender in complete status {}'.format(
#                                 direction.capitalize(),
#                                 tender['id']
#                             ),
#                             extra=journal_context(
#                                 {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_FOUND_NOLOT_COMPLETE},
#                                 {self.resource['id_key_upper']: tender['id']}
#                             )
#                         )
#                         yield tender
#                 else:
#                     logger.debug(
#                         '{} sync: Skipping tender {} in status {}'.format(
#                             direction.capitalize(),
#                             tender['id'],
#                             tender['status']
#                         ),
#                         extra=journal_context(params={self.resource['id_key_upper']: tender['id']})
#                     )

#             logger.info(
#                 'Sleep {} sync...'.format(direction),
#                 extra=journal_context({'MESSAGE_ID': journal_msg_ids.DATABRIDGE_SYNC_SLEEP})
#             )

#             gevent.sleep(float(delay))

#             logger.info(
#                 'Restore {} sync'.format(direction),
#                 extra=journal_context({'MESSAGE_ID': journal_msg_ids.DATABRIDGE_SYNC_RESUME})
#             )

#             logger.debug('{} {}'.format(direction, params))

#             response = self.tenders_sync_client.sync_tenders(
#                 params, extra_headers={'X-Client-Request-ID': generate_request_id()}
#             )

#     @retry(stop_max_attempt_number=7, wait_exponential_multiplier=1000 * 90)
#     def get_tender_data_with_retry(self, contract):
#         logger.info('Getting extra info for tender {}'.format(contract[self.resource['id_key']]),
#             extra=journal_context(
#                 {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_GET_EXTRA_INFO},
#                 {self.resource['id_key_upper']: contract[self.resource['id_key']],
#                  'CONTRACT_ID': contract['id']}
#             )
#         )

#         tender_data = self.get_tender_credentials(contract[self.resource['id_key']])

#         assert 'owner' in tender_data.data
#         assert 'tender_token' in tender_data.data

#         return tender_data

#     def _start_jobs(self):
#     return {'get_tenders_backward': spawn(self.get_tenders_backward),
#             'get_tenders_forward': spawn(self.get_tenders_forward)}

#     def check_and_revive_jobs(self):
#         for name, job in self.immortal_jobs.items():
#             if job.dead and not job.value:
#                 self.revive_job(name)


# -*- coding: utf-8 -*-
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
    handle_common_tenders,
    handle_esco_tenders
)


from .base_worker import BaseWorker
from openprocurement.integrations.treasury.databridge import constants
from openprocurement.integrations.treasury.databridge.utils import (journal_context, more_tenders, 
    valid_qualification_tender, valid_prequal_tender)
from openprocurement.integrations.treasury.databridge import journal_msg_ids
from retrying import retry

logger = logging.getLogger(__name__)


class TenderScanner(BaseWorker):
    """ Edr API Data Bridge """

    def __init__(self, tenders_sync_client, filtered_tender_ids_queue, services_not_available, process_tracker,
                 sleep_change_value, delay=15):
        super(TenderScanner, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.delay = delay
        # init clients
        self.tenders_sync_client = tenders_sync_client

        # init queues for workers
        self.filtered_tender_ids_queue = filtered_tender_ids_queue

        self.process_tracker = process_tracker

        # blockers
        self.initialization_event = Event()
        self.sleep_change_value = sleep_change_value

    @retry(stop_max_attempt_number=5, wait_exponential_multiplier=constants.retry_mult)
    def initialize_sync(self, params=None, direction=None):
        if direction == "backward":
            self.initialization_event.clear()
            assert params['descending']
            response = self.tenders_sync_client.sync_tenders(params,
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
            return self.tenders_sync_client.sync_tenders(params,
                                                         extra_headers={'X-Client-Request-ID': generate_request_id()})

    def get_tenders(self, params={}, direction=""):
        response = self.initialize_sync(params=params, direction=direction)

        while more_tenders(params, response):
            tenders = response.data if response else []
            params['offset'] = response.next_page.offset
            for tender in tenders:
                if self.should_process_tender(tender):
                    yield tender
                else:
                    logger.info('Skipping tender {} with status {} with procurementMethodType {}'.format(
                        tender['id'], tender['status'], tender['procurementMethodType']),
                        extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_INFO},
                                              params={"TENDER_ID": tender['id']}))
            logger.info('Sleep {} sync...'.format(direction),
                        extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_SYNC_SLEEP}))
            gevent.sleep(self.delay + self.sleep_change_value.time_between_requests)
            try:
                response = self.tenders_sync_client.sync_tenders(params, extra_headers={
                    'X-Client-Request-ID': generate_request_id()})
                self.sleep_change_value.decrement()
            except ResourceError as re:
                if re.status_int == 429:
                    self.sleep_change_value.increment()
                    logger.info("Received 429, will sleep for {}".format(self.sleep_change_value.time_between_requests))
                else:
                    raise re

    def should_process_tender(self, tender):
        return (not self.process_tracker.check_processed_tenders(tender['id']) and
                (valid_qualification_tender(tender) or valid_prequal_tender(tender)))

    def get_tenders_forward(self):
        self.services_not_available.wait()
        logger.info('Start forward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'mode': '_all_'}
        try:
            self.put_tenders_to_process(params, "forward")
        except Exception as e:
            logger.warning('Forward worker died!', extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
        else:
            logger.warning('Forward data sync finished!')

    def get_tenders_backward(self):
        self.services_not_available.wait()
        logger.info('Start backward data sync worker...')
        params = {'opt_fields': 'status,procurementMethodType', 'descending': 1, 'mode': '_all_'}
        try:
            self.put_tenders_to_process(params, "backward")
        except Exception as e:
            logger.warning('Backward worker died!', extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_WORKER_DIED}, {}))
            logger.exception("Message: {}".format(e.message))
            return False
        else:
            logger.info('Backward data sync finished.')
            return True

    def put_tenders_to_process(self, params, direction):
        for tender in self.get_tenders(params=params, direction=direction):
            logger.info('Backward sync: Put tender {} to process...'.format(tender['id']),
                        extra=journal_context({"MESSAGE_ID": journal_msg_ids.DATABRIDGE_TENDER_PROCESS},
                                              {"TENDER_ID": tender['id']}))
            self.filtered_tender_ids_queue.put(tender['id'])

    def _start_jobs(self):
        return {'get_tenders_backward': spawn(self.get_tenders_backward),
                'get_tenders_forward': spawn(self.get_tenders_forward)}

    def check_and_revive_jobs(self):
        for name, job in self.immortal_jobs.items():
            if job.dead and not job.value:
                self.revive_job(name)

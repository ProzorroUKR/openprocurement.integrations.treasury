# # -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import gevent
import logging
from gevent import spawn
from openprocurement.integrations.treasury.databridge.base_worker import BaseWorker
from datetime import datetime
from xml.etree.ElementTree import tostring
from openprocurement.integrations.treasury.databridge.utils import (
    generate_request_id, journal_context,
    fill_base_contract_data, handle_common_tenders, handle_esco_tenders)
from collections import namedtuple
from openprocurement.integrations.treasury.databridge import journal_msg_ids
from openprocurement.integrations.treasury.databridge.tenders.tender_former import TenderFormer
try:
    from openprocurement_client.exceptions import ResourceGone, ResourceNotFound
except ImportError:
    from openprocurement_client.client import ResourceNotFound
    from restkit.errors import ResourceGone


logger = logging.getLogger(__name__)


class TenderFilter(BaseWorker):

    def __init__(self, resource, tenders_sync_client, contracting_client, contracting_client_ro, filtered_tenders_queue, process_tracker, cache_db,
                 services_not_available, sleep_change_value, delay=15):
        super(TenderFilter, self).__init__(services_not_available)
        self.start_time = datetime.now()
        self.resource = resource
        self.delay = delay
        self.process_tracker = process_tracker
        self.tender_former = TenderFormer()
        self.cache_db = cache_db
        # init clients
        self.tenders_sync_client = tenders_sync_client
        self.contracting_client = contracting_client
        self.contracting_client_ro = contracting_client_ro

        # init queues for workers
        self.filtered_tenders_queue = filtered_tenders_queue
        self.sleep_change_value = sleep_change_value
        self.basket = dict()
    
    def get_tender_contracts(self):
        while True:
            try:
                self._get_tender_contracts()
            except Exception as e:
                logger.warn(
                    'Fail to handle tender contracts',
                    extra=journal_context({'MESSAGE_ID': journal_msg_ids.DATABRIDGE_EXCEPTION}, {})
                )
                logger.exception(e)
                gevent.sleep(self.sleep_change_value.time_between_requests)
                raise
            gevent.sleep(1)
    
    def _get_tender_contracts(self):
        tender_to_sync = self.filtered_tenders_queue.get()
        try:
            tender = self.tenders_sync_client.get_tender(
                tender_to_sync['id'],
                extra_headers={'X-Client-Request-ID': generate_request_id()}
            )['data']
        except Exception as e:
            logger.exception(e)
            logger.info(
                'Put tender {} back to tenders queue'.format(tender_to_sync['id']),
                extra=journal_context(
                    {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_EXCEPTION},
                    params={self.resource['id_key_upper']: tender_to_sync['id']}
                )
            )
            self.filtered_tenders_queue.put(tender_to_sync)
            gevent.sleep(self.on_error_delay)
        else:
            if 'contracts' not in tender:
                logger.warn(
                    '!!!No contracts found in tender {}'.format(tender['id']),
                    extra=journal_context(
                        {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_EXCEPTION},
                        params={self.resource['id_key_upper']: tender['id']}
                    )
                )

                return
            for contract in tender['contracts']:
                if contract['status'] == 'active':
                    self.basket[contract['id']] = tender_to_sync['dateModified']

                    try:
                        if not self.cache_db.has(contract['id']):
                            self.contracting_client_ro.get_contract(contract['id'])
                        else:
                            logger.info(
                                'Contract {} exists in local db'.format(contract['id']),
                                extra=journal_context(
                                    {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_CACHED},
                                    params={'CONTRACT_ID': contract['id']}
                                )
                            )
                            self._put_tender_in_cache_by_contract(contract, tender_to_sync['id'])

                            continue
                    except ResourceNotFound:
                        logger.info(
                            'Sync contract {} of tender {}'.format(contract['id'], tender['id']),
                            extra=journal_context(
                                {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_CONTRACT_TO_SYNC},
                                {'CONTRACT_ID': contract['id'], self.resource['id_key_upper']: tender['id']}
                            )
                        )
                    except ResourceGone:
                        logger.info(
                            'Sync contract {} of tender {} has been '
                            'archived'.format(contract['id'], tender['id']),
                            extra=journal_context(
                                {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_CONTRACT_TO_SYNC},
                                {'CONTRACT_ID': contract['id'],
                                 self.resource['id_key_upper']: tender['id']}
                            )
                        )

                        continue
                    except Exception as e:
                        logger.exception(e)
                        logger.info(
                            'Put tender {} back to tenders queue'.format(tender_to_sync['id']),
                            extra=journal_context(
                                {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_EXCEPTION},
                                params={
                                    self.resource['id_key_upper']: tender_to_sync['id'],
                                    'CONTRACT_ID': contract['id']
                                }
                            )
                        )
                        self.filtered_tenders_queue.put(tender_to_sync)

                        raise
                    else:
                        self.cache_db.put(contract['id'], True)

                        logger.info(
                            'Contract exists {}'.format(contract['id']),
                            extra=journal_context(
                                {'MESSAGE_ID': journal_msg_ids.DATABRIDGE_CONTRACT_EXISTS},
                                {self.resource['id_key_upper']: tender_to_sync['id'], 'CONTRACT_ID': contract['id']}
                            )
                        )
                        self._put_tender_in_cache_by_contract(contract, tender_to_sync['id'])

                        continue

                    fill_base_contract_data(contract, tender)

                    if tender.get('procurementMethodType') == 'esco':
                        handle_esco_tenders(contract, tender)
                    else:
                        handle_common_tenders(contract, tender)
                    logger.info("CONTRAAAAAAAAAAAAAAAACT {}".format(contract))
                    logger.info("TENDEEEEER {}".format(tender))
                    TenderData = namedtuple('TenderData', ['tender_id', 'contract_id', 'identifier_id', 'contract_number', 'date_signed', 
                                                           'contracts_amount', 'currency', 'vat_included', 'documents'])
                    tender_data = TenderData(tender['id'], tender['contracts'][0]['id'], 
                                             tender['procuringEntity']['identifier']['id'], tender['contracts'][0]['contractNumber'], 
                                             tender['contracts'][0]['dateSigned'], tender['contracts'][0]['value']['amount'], 
                                             tender['contracts'][0]['value']['currency'], tender['contracts'][0]['value']['valueAddedTaxIncluded'],
                                             tender['contracts'][0]['documents'])
                    tender_xml = self.tender_former.form_xml_to_post(tender_data, generate_request_id())
                    with open('packets/tender_'+str(tender['id'])+'.xml', 'w') as file:
                        file.write(tostring(tender_xml, encoding='UTF-8'))
                    logger.info('tender_xml: {}'.format(tostring(tender_xml, encoding='UTF-8')))
                    self.handicap_contracts_queue.put(contract)

    def _put_tender_in_cache_by_contract(self, contract, tender_id):
        date_modified = self.basket.get(contract['id'])

        if date_modified:
            self.cache_db.put(tender_id, date_modified)

        self.basket.pop(contract['id'], None)

    def _start_jobs(self):
        return {'get_tender_contracts': spawn(self.get_tender_contracts)}
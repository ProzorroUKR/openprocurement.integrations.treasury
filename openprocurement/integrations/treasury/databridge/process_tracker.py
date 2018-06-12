# coding=utf-8
import pickle
import logging.config
from datetime import datetime

from openprocurement.integrations.treasury.databridge.caching import db_key, tender_key, contract_key, plan_key
from openprocurement.integrations.treasury.databridge.utils import item_key


logger = logging.getLogger(__name__)

class ProcessTracker(object):
    def __init__(self, db=None, ttl=300):
        self.processing_items = {}
        self.processed_items = {}
        self._db = db
        self.tender_documents_to_process = {}
        self.ttl = ttl

    def set_item(self, tender_id, item_id, docs_amount=0):
        self.processing_items[item_key(tender_id, item_id)] = docs_amount
        self._add_docs_amount_to_tender(tender_id, docs_amount)

    def _add_docs_amount_to_tender(self, tender_id, docs_amount):
        if self.tender_documents_to_process.get(tender_id):
            self.tender_documents_to_process[tender_id] += docs_amount
        else:
            self.tender_documents_to_process[tender_id] = docs_amount

    def _remove_docs_amount_from_tender(self, tender_id):
        if self.tender_documents_to_process[tender_id] > 1:
            self.tender_documents_to_process[tender_id] -= 1
        else:
            self._db.put(tender_key(tender_id), datetime.now().isoformat(), self.ttl)
            del self.tender_documents_to_process[tender_id]

    def check_processing_item(self, tender_id, item_id):
        """Check if current tender_id, item_id is processing"""
        return item_key(tender_id, item_id) in self.processing_items.keys()

    def check_processed_item(self, tender_id, item_id):
        """Check if current tender_id, item_id was already processed"""
        return item_key(tender_id, item_id) in self.processed_items.keys()

    def check_processed_tenders(self, tender_id):
        return self._db.has(tender_key(tender_id)) or False

    def check_processed_contracts(self, contract_id):
        return self._db.has(contract_key(contract_id)) or False

    def check_processed_plans(self, plan_id):
        return self._db.has(plan_key(plan_id)) or False

    def get_unprocessed_items(self):
        return self._db.get_items("unprocessed_*") or []

    def add_unprocessed_item(self, data):
        self._db.put(data.doc_id(), pickle.dumps(data), self.ttl)

    def _remove_unprocessed_item(self, document_id):
        self._db.remove(document_id)

    def put(self, id, data):
        # logger.info('pickledump {}'.format(pickle.dumps(data)))
        self._db.put(id, pickle.dumps(data))
    
    def put_tender(self, id, data):
        self.put(tender_key(id), data)
    
    def put_contract(self, id, data):
        self.put(contract_key(id), data)
    
    def put_plan(self, id, data):
        self.put(plan_key(id), data)

    def _update_processing_items(self, tender_id, item_id, document_id):
        key = item_key(tender_id, item_id)
        if self.processing_items[key] > 1:
            self.processing_items[key] -= 1
        else:
            self.processed_items[key] = datetime.now()
            self._remove_unprocessed_item(document_id)
            del self.processing_items[key]

    def update_items_and_tender(self, tender_id, item_id, document_id):
        self._update_processing_items(tender_id, item_id, document_id)
        self._remove_docs_amount_from_tender(tender_id)

# # -*- coding: utf-8 -*-
from gevent import monkey

monkey.patch_all()
import os
import xmlschema
import logging.config

from datetime import datetime
from xml.dom import minidom
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement


logger = logging.getLogger(__name__)


class TenderFormer(object):

    def __init__(self):
        pass

    def is_valid(self, request):
        schema = xmlschema.XMLSchema(os.path.join(os.getcwd(), "resources/request.xsd"))
        return schema.is_valid(request)

    def form_xml_to_post(self, data, request_id):
        request = Element('request')
        contract_id = SubElement(request, 'ContractId')
        contract_id.text = data.contract_id
        tender_id = SubElement(request, 'TenderId')
        tender_id.text = data.tender_id
        identifier_id = SubElement(request, 'IdentifierId')
        identifier_id.text = data.identifier_id
        contract_number = SubElement(request, 'ContractNumber')
        contract_number.text = data.contract_number
        date_signed = SubElement(request, 'DateSigned')
        date_signed.text = data.date_signed
        contracts_amount = SubElement(request, 'ContractsAmount')
        contracts_amount.text = data.contracts_amount
        currency = SubElement(request, 'Currency')
        currency.text = data.currency
        vat_included = SubElement(request, 'ValueAddedTaxIncluded')
        vat_included.text = data.vat_included
        documents = SubElement(request, 'Documents')
        i = 0
        for file in data.documents:
            i += 1
            new_file = SubElement(documents, 'file'+str(i))
            new_file.text = file
        # report = SubElement(request, 'Report')
        # plans = SubElement(request, 'Plans') 
        # TODO eventually there will be plans 
        bids = SubElement(request, 'Bids')
        i = 0
        for bid in data.bids:
            i += 1
            new_bid = SubElement(bids, 'bid'+str(i))
            new_bid.text = bid
        logger.info("Request {} is valid? {}".format(request_id, self.is_valid(request)))
        return request
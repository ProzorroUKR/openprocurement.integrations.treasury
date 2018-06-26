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


class ContractFormer(object):

    def __init__(self):
        pass

    def is_valid(self, request):
        schema = xmlschema.XMLSchema(os.path.join(os.getcwd(), "resources/request_contracts.xsd"))
        return schema.is_valid(request)

    def form_xml_to_post(self, data, request_id):
        request = Element('request')
        contract_id = SubElement(request, 'ContractId')
        contract_id.text = data.contract_id
        documents = SubElement(request, 'Documents')
        i = 0
        for file in data.documents:
            i += 1
            new_file = SubElement(documents, 'file'+str(i))
            new_file.text = file
        logger.info("Request {} is valid? {}".format(request_id, self.is_valid(request)))
        return request
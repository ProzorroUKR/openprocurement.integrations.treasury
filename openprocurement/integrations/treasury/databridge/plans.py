from munch import munchify
from simplejson import loads
import logging.config
from openprocurement_client.plan import PlansClient

logger = logging.getLogger(__name__)

class PlansClientSync(PlansClient):
    def sync_plans(self, params=None, extra_headers=None):
        logger.info("our sync_plans")
        if params is None:
            params = dict()

        if extra_headers is None:
            extra_headers = dict()

        params['feed'] = 'changes'
        self.headers.update(extra_headers)

        response = self.get(self.prefix_path, params_dict=params)

        if response.status_int == 200:
            plans_list = munchify(loads(response.body_string()))
            return plans_list
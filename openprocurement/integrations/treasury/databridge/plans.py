from munch import munchify
from simplejson import loads

from openprocurement_client.plan import PlansClient


class PlansClientSync(PlansClient):
    def sync_plans(self, params=None, extra_headers=None):
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
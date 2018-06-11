from json import loads
from munch import munchify
from client import APIBaseClient, verify_file


class PlaningClient(APIBaseClient):
    """ planing client """

    def __init__(self, key,
                 host_url="https://public.api-sandbox.openprocurement.org",
                 api_version='2.4',
                 params=None):
        super(PlaningClient, self).__init__(key, host_url, api_version,
                                                "plans", params)

    @verify_file
    def upload_document(self, file_, plan):
        return self._upload_resource_file(
            '{}/{}/documents'.format(
                self.prefix_path,
                plan.data.id
            ),
            data={"file": file_},
            headers={'X-Access-Token':
                     getattr(getattr(plan, 'access', ''), 'token', '')}
        )

    def create_plan(self, plan):
        return self._create_resource_item(self.prefix_path, plan)

    def get_plan(self, id):
        return self._get_resource_item('{}/{}'.format(self.prefix_path, id))

    def get_plans(self, params={}, feed='changes', extra_headers={}):
        params['feed'] = feed
        self._update_params(params)
        response = self.get(
            self.prefix_path,
            params_dict=self.params,
            headers=extra_headers)
        # return response
        if response.status_int == 200:
            data = munchify(loads(response.body_string()))
            self._update_params(data.next_page)
            return data

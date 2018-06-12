# coding=utf-8

from logging import getLogger
from ConfigParser import NoOptionError

LOGGER = getLogger(__name__)


class Db(object):
    """ Database proxy """

    def __init__(self, config):
        self.config = config

        self._backend = None
        self._db_name = None
        self._port = None
        self._host = None
        try:
            self.config.get('app:api', 'cache_host')
            import redis
            self._backend = "redis"
            self._host = self.config_get('cache_host')
            self._port = self.config_get('cache_port') or 6379
            self._db_name = self.config_get('cache_db_name') or 0
            self.db = redis.StrictRedis(host=self._host, port=self._port, db=self._db_name)
            self.set_value = self.db.set
            self.has_value = self.db.exists
            self.remove_value = self.db.delete
            LOGGER.info("Cache initialized")
        except NoOptionError as error:
            self.set_value = lambda x, y, z: None
            self.has_value = lambda x: None
            self.remove_value = lambda x: None

    def config_get(self, name):
        return self.config.get('app:api', name)

    def get(self, key):
        LOGGER.info("Getting item {} from the cache".format(key))
        return self.db.get(key)

    def get_items(self, requested_key):
        keys = self.db.keys(requested_key)
        return [self.get(key) for key in keys]

    def put(self, key, value, ex=86400):
        LOGGER.info("Saving key {} to cache".format(key))
        self.set_value(key, value, ex)

    def remove(self, key):
        self.remove_value(key)

    def has(self, key):
        LOGGER.info("Checking if code {} is in the cache".format(key))
        return self.has_value(key)


def db_key(tender_id):
    return "{}".format(tender_id)

def tender_key(tender_id):
    return "tender_{}".format(tender_id)

def contract_key(contract_id):
    return "contract_{}".format(contract_id)

def plan_key(plan_id):
    return "plan_{}".format(plan_id)

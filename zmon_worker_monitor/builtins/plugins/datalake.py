#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import logging
import time
import tokens

from zmon_worker_monitor.zmon_worker.errors import ConfigurationError, CheckError
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

logger = logging.getLogger('zmon-worker.scalyr-function')

QUERY_STATUS_INTERVAL = 5

# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()


class DatalakeFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(DatalakeFactory, self).__init__()
        self._url = None

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        self._url = conf.get('url')

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(DatalakeWrapper, url=self._url)


class DatalakeWrapper(object):
    def __init__(self, url, oauth2=False):
        if not url:
            raise ConfigurationError('Datalake wrapper improperly configured. URL is missing!')

        self.url = url

        self.__session = requests.Session()

        if oauth2:
            self.__session.headers.update({'Authorization': 'Bearer {}'.format(tokens.get('uid'))})

    def query(self, query, timeout=60):
        start_time = time.time()

        # submit query first
        job_id = self.__session.post('{}/jobs'.format(self.url), json=query).json()['id']

        # check if query has finished
        while self.__session.get('{}/jobs/{}'.format(self.url, job_id), json=query).json()['status'] == 'RUNNING':
            if time.time() > start_time + timeout:
                raise CheckError('Datalake query timed out!')
            time.sleep(QUERY_STATUS_INTERVAL)

        # check for failures
        job_status = self.__session.get('{}/jobs/{}'.format(self.url, job_id), json=query).json()
        if job_status['status'] != 'FINISHED':
            logs = self.__session.get('{}/jobs/{}/logs'.format(self.url, job_id), json=query).json()
            raise CheckError('Datalake query failed! error message: {}'.format(logs['message']))
        else:
            # return result
            return self.__session.get('{}/jobs/{}/output'.format(self.url, job_id), json=query).json()

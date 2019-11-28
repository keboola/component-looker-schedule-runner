'''
Template Component main class.

'''

import logging
import logging_gelf.handlers
import logging_gelf.formatters
import sys
import os
import datetime  # noqa
import requests
import json
from urllib.parse import urlencode

from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa


# configuration variables
KEY_CLIENT_ID = 'client_id'
KEY_CLIENT_SECRET = '#client_secret'
KEY_LOOKER_HOST_URL = 'looker_host_url'
KEY_DASHBOARDS = 'dashboards'

MANDATORY_PARS = [
    KEY_CLIENT_ID,
    KEY_CLIENT_SECRET,
    KEY_LOOKER_HOST_URL,
    KEY_DASHBOARDS
]
MANDATORY_IMAGE_PARS = []

# Default Table Output Destination
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_FILE_SOURCE = "/data/in/files/"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'), port=int(os.getenv('KBC_LOGGER_PORT')))
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # remove default logging to stdout
    logger.removeHandler(logger.handlers[0])

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        """
        # override debug from config
        if self.cfg_params.get('debug'):
            debug = True
        else:
            debug = False

        self.set_default_logger('DEBUG' if debug else 'INFO')
        """
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def post_request(self, url, header, body=None):
        '''
        Standard Post request
        '''

        r = requests.post(url=url, headers=header, data=json.dumps(body))

        return r

    def authorize(self, client_id, client_secret):
        '''
        Authorizing Looker account with client id and secret
        '''

        auth_url = self.base_url + 'login'
        auth_header = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        auth_body = 'client_id={}&client_secret={}'.format(
            client_id, client_secret)
        request_url = auth_url+'?'+auth_body

        res = self.post_request(request_url, auth_header)
        if res.status_code != 200:
            logging.error(
                "Authorization failed. Please check your credentials.")
            sys.exit(1)

        self.access_token = res.json()['access_token']

    def _construct_filters(self, filters):
        '''
        Filters Constructor
        '''

        temp = {}
        for f in filters:
            temp[f['filter_property']] = f['filter_value']
        filter_string = '?{}'.format(urlencode(temp))
        # filter_string = '?Country=USA&Date%20%20Selector=2019%2F11%2F27'

        return filter_string

    def _construct_contacts(self, contacts):
        '''
        Contacts destination constructor
        '''

        contact_destination = []
        for contact in contacts:
            temp_base = {
                'format': 'wysiwyg_pdf',
                'apply_formatting': True,
                'apply_vis': True,
                'type': 'email',
                'address': contact['recipient']
            }
            contact_destination.append(temp_base)

        return contact_destination

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)
        self.base_url = '{}api/3.1/'.format(params.get(KEY_LOOKER_HOST_URL))
        dashboards = params.get(KEY_DASHBOARDS)

        # Authorizating Looker Account
        self.authorize(client_id, client_secret)
        # Header for all requests
        self.request_header = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'Content-Type': 'application/json'
        }

        # Running each of the dashboard in the dashboard configurations
        for dashboard in dashboards:
            logging.info(
                'Processing Dashboard - {}'.format(dashboard['dashboard_id']))
            request_base_form = {
                'name': 'run_once - {}'.format(dashboard['dashboard_id']),
                'dashboard_id': int(dashboard['dashboard_id']),
                'title': 'run_once - {}'.format(dashboard['dashboard_id']),
                'enable': True,
                'run_once': True
            }

            contact_destination = self._construct_contacts(
                dashboard['recipients'])
            request_base_form['scheduled_plan_destination'] = contact_destination

            # If filters exist in the configuration
            if dashboard['filters']:
                filters_string = self._construct_filters(
                    filters=dashboard['filters'])
                request_base_form['filters_string'] = filters_string

            # Schedule requesting
            request_url = '{}scheduled_plans/run_once'.format(self.base_url)
            res = self.post_request(
                url=request_url, header=self.request_header, body=request_base_form)
            if res.status_code != 200:
                logging.error(
                    'Error in processing Dashboard - [{}] - {}'.format(dashboard['dashboard_id'], res.json()))

        logging.info("Extraction finished")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = Component(debug)
    comp.run()

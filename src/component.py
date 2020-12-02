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
import pandas as pd
from urllib.parse import urlencode
import validators

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

NOW = datetime.datetime.now()

APP_VERSION = '0.0.3'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def get_tables(self, tables, mapping):
        """
        Evaluate input and output table names.
        Only taking the first one into consideration!
        mapping: input_mapping, output_mappings
        """
        # input file
        table_list = []
        for table in tables:
            if mapping == "input_mapping":
                destination = table["destination"]
            elif mapping == "output_mapping":
                destination = table["source"]
            table_list.append(destination)

        return table_list

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

        filter_string = '?{}'.format(urlencode(filters))

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

    def validate_user_inputs(self, params, in_tables):
        '''
        Validating user inputs
        '''

        # Empty Configuration
        if not params:
            logging.error('Your configuration is missing.')
            sys.exit(1)

        # Validate if any of the configuration is missing
        if not params.get(KEY_CLIENT_ID) or not params.get(KEY_CLIENT_SECRET) or not params.get(KEY_LOOKER_HOST_URL):
            logging.error(
                'Required configuration cannot be empty: Client ID, Client Secret, Looker Host URL')
            sys.exit(1)

        # Validating if there are any input files
        if len(in_tables) == 0:
            logging.error('Input tables are missing.')
            sys.exit(1)

        # Validating column inputs in the input files
        required_columns = ['dashboard_id', 'recipients', 'filters']
        for table in in_tables:
            missing_columns = []
            table_manifest_path = '{}.manifest'.format(table['full_path'])

            with open(table_manifest_path, 'r') as f:
                table_manifest = json.load(f)

            for column in required_columns:
                if column not in table_manifest['columns']:
                    missing_columns.append(column)

            if len(missing_columns) > 0:
                logging.error('Input Table [{}] is missing required columns: {}'.format(
                    table['destination'], missing_columns))
                sys.exit(1)

    def validate_url(self, url):
        '''
        URL adjustments if required
        Validating if the URL is valid
        '''

        # Validating URL structure
        looker_url = f'{url}/' if url[-1] != '/' else url

        if not (looker_url[:8] == 'https://' or looker_url[:7] == 'http://'):
            looker_url = 'https://{}'.format(looker_url)

        # Validating if URL is valid
        if not validators.url(looker_url):
            logging.error('Your Looker URL is not valid.')
            sys.exit(1)

        looker_api_url = '{}api/3.1/'.format(looker_url)

        return looker_api_url

    def output_log(self, logs):
        '''
        Outputting log messages
        '''

        log_df = pd.DataFrame(logs)
        log_df.to_csv(DEFAULT_TABLE_DESTINATION+'log.csv', index=False)

        manifest = {
            'incremental': True,
            'primary_key': [
                'datetime',
                'dashboard_id',
                'recipient',
                'filters'
            ]
        }

        with open(DEFAULT_TABLE_DESTINATION+'log.csv.manifest', 'w') as f:
            json.dump(manifest, f)

    def run(self):
        '''
        Main execution code
        '''
        # Get proper list of tables
        in_tables = self.configuration.get_input_tables()
        in_table_names = self.get_tables(in_tables, 'input_mapping')
        logging.info("IN tables mapped: "+str(in_table_names))

        # Requests Parameters
        params = self.cfg_params  # noqa

        # Validating user inputs
        self.validate_user_inputs(params, in_tables)

        # User input parameters
        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)
        self.base_url = self.validate_url(params.get(KEY_LOOKER_HOST_URL))

        # Authorizating Looker Account
        self.authorize(client_id, client_secret)
        # Header for all requests
        self.request_header = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'Content-Type': 'application/json'
        }

        # Parsing dashboard configuration from each row
        dashboards = []
        for table in in_table_names:
            dashboard_config = pd.read_csv(DEFAULT_TABLE_SOURCE+table)

            for index, row in dashboard_config.iterrows():
                dashboard_constructor = {
                    'dashboard_id': row['dashboard_id'],
                    'recipients': [
                        {
                            'recipient': row['recipients']
                        }
                    ]
                    # 'filters': row['filters']
                }
                if not pd.isnull(row['filters']):
                    dashboard_constructor['filters'] = json.loads(
                        row['filters'])
                dashboards.append(dashboard_constructor)

        process_counter = 0
        process_log = []
        # Running each of the dashboard in the dashboard configurations
        for dashboard in dashboards:
            logging.debug(
                'Processing Dashboard - {} sending to {}'.format(dashboard['dashboard_id'],
                                                                 dashboard['recipients'][0]['recipient']))
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
            if 'filters' in dashboard:
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

            # Logging number of processed records
            process_counter += 1
            if process_counter % 100 == 0:
                logging.info('Processed {} records'.format(process_counter))

            # Logging processed data
            log_json = {
                'datetime': NOW,
                'dashboard_id': dashboard['dashboard_id'],
                'recipient': dashboard['recipients'][0]['recipient'],
                'filters': dashboard['filters'] if 'filters' in dashboard else '',
                'request_status': 'Sent' if res.status_code in (200, 201) else 'Error',
                'request_message': '' if res.status_code in (200, 201) else res.text()
            }
            process_log.append(log_json)

        logging.info('Total processed records: {}'.format(process_counter))
        # Output Log
        self.output_log(process_log)

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

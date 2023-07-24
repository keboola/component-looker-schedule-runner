from keboola.http_client import HttpClient
from urllib.parse import urlencode

import datetime


class LookerClientException(Exception):
    pass


class LookerClient(HttpClient):
    def __init__(self, base_url, client_id, client_secret):
        self.access_token = None
        self.request_header = None

        super().__init__(base_url)
        self.time_at_init = datetime.datetime.now()
        self.authorize(client_id, client_secret)

    def authorize(self, client_id, client_secret):
        '''
        Authorizing Looker account with client id and secret
        '''

        auth_url = f'{self.base_url}login'
        auth_header = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        auth_body = f'client_id={client_id}&client_secret={client_secret}'
        request_url = f'{auth_url}?{auth_body}'

        res = self.post_raw(request_url, auth_header)
        if res.status_code != 200:
            raise LookerClientException("Authorization failed. Please check your credentials.")

        self.access_token = res.json()['access_token']
        self.request_header = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'Content-Type': 'application/json'
        }

    @staticmethod
    def _construct_filters(filters):
        return f'?{urlencode(filters)}'

    @staticmethod
    def _construct_contacts(contacts):
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

    def run_dashboard(self, dashboard):
        request_base_form = {'name': f"run_once - {dashboard['dashboard_id']}",
                             'dashboard_id': int(dashboard['dashboard_id']),
                             'title': f"run_once - {dashboard['dashboard_id']}",
                             'enable': True,
                             'run_once': True}

        contact_destination = self._construct_contacts(dashboard['recipients'])
        request_base_form['scheduled_plan_destination'] = contact_destination

        # If filters exist in the configuration
        if 'filters' in dashboard:
            filters_string = self._construct_filters(filters=dashboard['filters'])
            request_base_form['filters_string'] = filters_string

        res = self.post_raw("scheduled_plans/run_once", headers=self.request_header, json=request_base_form)

        if res.status_code != 200:
            raise LookerClientException(f"Error in processing Dashboard - [{dashboard['dashboard_id']}] - {res.json()}")

        return {'datetime': self.time_at_init,
                'dashboard_id': dashboard['dashboard_id'],
                'recipient': dashboard['recipients'][0]['recipient'],
                'filters': dashboard['filters'] if 'filters' in dashboard else '',
                'request_status': 'Sent' if res.status_code in (200, 201) else 'Error',
                'request_message': '' if res.status_code in (200, 201) else res.text()}

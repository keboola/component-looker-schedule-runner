import logging
import validators
import json
from csv import DictReader, DictWriter

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client import LookerClient, LookerClientException
from configuration import Configuration

REQUIRED_INPUT_TABLE_COLUMNS = ['dashboard_id', 'recipients', 'filters']

OUTPUT_TABLE_NAME = "log.csv"
OUTPUT_TABLE_P_KEYS = ['datetime', 'dashboard_id', 'recipient', 'filters']
OUTPUT_TABLE_COLUMNS = ['datetime', 'dashboard_id', 'recipient', 'filters', 'request_status', 'request_message']


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self._configuration: Configuration
        self.client: LookerClient

    def run(self):
        self._init_configuration()
        self._init_client()

        dashboards = self.get_all_dashboards()

        processed_records = 0

        outfile = self.create_out_table_definition(OUTPUT_TABLE_NAME,
                                                   incremental=True,
                                                   primary_key=OUTPUT_TABLE_P_KEYS)

        with open(outfile.full_path, 'w') as output:
            writer = DictWriter(output, OUTPUT_TABLE_COLUMNS)
            for dashboard in dashboards:
                log = self.run_dashboard(dashboard)
                logging.debug(f"Processing Dashboard - {dashboard['dashboard_id']} sending to "
                              f"{dashboard['recipients'][0]['recipient']}")

                processed_records += 1
                if processed_records % 100 == 0:
                    logging.info(f'Processed {processed_records} records')

                writer.writerow(log)

        logging.info(f'Total processed records: {processed_records}')

        logging.info("Extraction finished")
        self.write_manifest(outfile)

    def run_dashboard(self, dashboard: dict) -> dict:
        try:
            return self.client.run_dashboard(dashboard)
        except LookerClientException as looker_exc:
            raise UserException(looker_exc) from looker_exc

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def _init_client(self) -> None:
        base_url = self.validate_url(self._configuration.looker_host_url)
        try:
            self.client = LookerClient(base_url=base_url,
                                       client_id=self._configuration.client_id,
                                       client_secret=self._configuration.pswd_client_secret)
        except LookerClientException as looker_exc:
            raise UserException(looker_exc) from looker_exc

    def get_all_dashboards(self) -> list[dict]:
        dashboards = []
        for table in self.get_input_tables_definitions():
            with open(table.full_path) as input_table:
                reader = DictReader(input_table)
                self.validate_input_table_columns(list(reader.fieldnames), table.name)
                for row in reader:
                    dashboard = {'dashboard_id': row.get('dashboard_id'),
                                 'recipients': [{'recipient': row.get('recipients')}],
                                 }
                    if row.get('filters'):
                        dashboard["filters"] = json.loads(row['filters'])
                    dashboards.append(dashboard)
        return dashboards

    @staticmethod
    def validate_url(url: str) -> str:
        """
        URL adjustments if required
        Validating if the URL is valid
        """
        # Validating URL structure
        looker_url = f'{url}/' if url[-1] != '/' else url

        if looker_url[:8] != 'https://' and looker_url[:7] != 'http://':
            looker_url = f'https://{looker_url}'

        # Validating if URL is valid
        if not validators.url(looker_url):
            raise UserException('Your Looker URL is not valid.')

        return f'{looker_url}api/4.0/'

    @staticmethod
    def validate_input_table_columns(input_columns: list[str], table_name: str) -> None:
        missing_columns = []
        for required_column in REQUIRED_INPUT_TABLE_COLUMNS:
            if required_column not in input_columns:
                missing_columns.append(required_column)
        if len(missing_columns) > 0:
            raise UserException(f"Input Table [{table_name}] is missing required columns: {missing_columns}")


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)

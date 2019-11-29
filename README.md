# Keboola Looker Scheduler Plan Runner

The purpose of this application is to create a one-time schedule plan firing off target dashboards. With the combination of KBC Orchestration and Transformations, users can use this application to 'refresh' the dataset in Looker and send out updated reports to their target audiences. 

### Input Mapping Requirements

The application will be fetching all tables from the input mapping as an input. File names will not be a factor to cause the application to fail! However, each table from the input mapping must contain the following columns (*all lower case*):

  1. dashboard_id

      - The ID of the dashboard that the user is trying oo refresh or send out
  
  2. recipients

      - The current configuration of this application can only support sending out Emails, not FTP.
      - The application can only accept 1 email entry. If the email is invalid, the Looker API will proceed with that incorrect entries only refreshing the dashboard with the latest dataset in Looker.
      - If the sole purpose of using this application is to refresh the dashboard and not to receive an email, user must enter a random `string` into the cell. This can be as random as entering `nobody` or `someone`.

      Examples:

      |Recipients|Email Sent|Dashboard Refresh|
      |-|-|-|
      someone@keboola.com|True|True
      some_random_string|False|True
      invalid_email@invalid.email|False|True

  3. filters

      - Looker will be executing the dashboard with the filters specified in the same row
      - This cell's value *MUST* be in `JSON` formatted string
      - If user does not want to filter anything, please leave this field blank. Application will then proceed to refresh the dashboard with its `default preset filters`

### Application Configuration

1. Client ID
2. Client Secret
3. Looker Host URL
    - Full URL of the Looker URL that is used for your Looker instance
    - Please invlud '/' at the end of the URL address
    - Example: https://{{looker_id}}.looker.com/


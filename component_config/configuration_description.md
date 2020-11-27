### Configuration Parameters

1. Client ID
2. Client Secret
3. Looker Host URL
    - Full URL of the Looker URL that is used for your Looker API instance
        - Can be found following path below
            - Your Looker instance > "Admin" > "API" > "View API Docs"
            - Your API URL will be in the URL with the port number
            - API URL Details can be found [here](https://docs.looker.com/reference/api-and-integration/api-getting-started)
    - Please include '/' at the end of the URL address
    - Example: https://{{looker_id}}.looker.com/

### Input Mapping Requirements

The application will be fetching all tables from the input mapping as an input. File names will not be a factor to cause the application to fail! However, each table from the input mapping must contain the all of the following columns (*all lower case*):

  1. dashboard_id

      - The ID of the dashboard to refresh
  
  2. recipients

      - The application supports sending out Emails only, FTP is not supported.
      - If entered recipient email address is invalid, the application will proceed with the incorrect email address and only refresh the dashboard with the given parameters.
      - Each row of the recipient can only contain 1 email address. If 1 or more email address is entered in the same row, the application will deem that email address is invalid. 
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

Sample Input File

|dashboard_id|recipients|filters|
|-|-|-|
|1|leo@keboola.com|{"Company":"Keboola","Position":"Ninja"}|
|2|fisa@keboola.com|{"Company":"Keboola","Position":"Master"}|
|3|marcus@keboola.com|{"Company":"Keboola","Position":"Chef"}|



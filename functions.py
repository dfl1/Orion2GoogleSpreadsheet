"""
This module is part of Orion2GoogleSpreadsheet project.

Contains utility functions used by Orion2GoogleSpreadsheet
"""

import gdata.spreadsheet.service
from apiclient import errors
from clientcreds import get_client_credentials
import unicodedata
import string
import json
import logs
import yaml

################################# IGNORE THE CODE BELOW #################################

# IGNORE. Load TEST json data. TO BE DELETED IN FINAL RELEASE.
# Emulates DefaultHandler.post() catching data function
d = open("json_test_data.txt").read()
data = json.loads(d)

# IGNORE. Listen to Context Broker requests TEST. TO BE DELETED IN FINAL RELEASE
def post_TEST():
    global data
    try:
        # Loop for entities and store data
        entities = []
        for entity_id in data["contextResponses"]:
            # Get entity name (replace '.' and ' ' and lower-case)
            entity_name = string_normalizer(entity_id["contextElement"]["id"])

            # Loop for attributes and their types to append them into a dictionary
            attributes = {}  # Initialization
            for attribute in entity_id['contextElement']['attributes']:
                # Append {attr name:attr value, attr name:attr value...}
                attributes[string_normalizer(str(attribute["name"]))] = str(attribute["value"])
                # Create the data and launch insert_data()
            entity = {'entity_name': entity_name, 'attributes': attributes}
            entities.append(entity)
        return entities

    except:
        print "An exception occurred"

################################# IGNORE THE CODE ABOVE #################################

# Normalize strings
def string_normalizer(message):
    try:
        # Convert to unicode format
        message = message.decode()

        # Lower-case
        message = message.lower()

        # Replace some characters
        message = message.replace('.', '_')
        message = message.replace(' ', '_')
        message = message.replace(':', '_')

        # Get NFKD unicode format
        message = unicodedata.normalize('NFKD', message)

        # Delete not ascii_letters
        message = ''.join(x for x in message if x in string.ascii_letters or x == "_" or x.isdigit())
    except:
        logs.logger.warn("An error occurred while trying to normalize string")
        return ""

    # Return normalized string
    return message

# Insert new file
def insert_file():
    """
    Inserts a new empty Spreadsheet file in the user's Google Drive account/ Spreadsheets

    :return: None
    """
    drive_service = get_client_credentials('drive')
    body = {
        'title': 'Orion 2 GSP',
        'description': 'description',
        'mimeType': 'application/vnd.google-apps.spreadsheet'
    }
    try:
        logs.logger.info("Inserting new Spreadsheet file")
        file = drive_service.files().insert(body=body).execute()
        with open('spreadsheet_key.yaml', 'w') as f:
            yaml.safe_dump(file['id'], f, default_flow_style=False)
        print 'File Created'
        return None

    except errors.HttpError, error:
        logs.logger.warn("An error occurred: " + str(error))
        return None

# Get Spreadsheet Key from file
def get_spreadsheet_key():
    """
    Gets the spreadsheet key from the spreadsheet created by the app

    :return: Spreadsheet Key string
    """
    if 'spreadsheet_key.yaml':
        try:
            logs.logger.info("Checking Spreadsheet Key")
            with open('spreadsheet_key.yaml') as f:
                spreadsheet_key = yaml.load(f)
            return spreadsheet_key
        except:
            logs.logger.info("No Key in file... Obtaining key from Drive account")
            # Get spreadsheet_key
            drive_service = get_client_credentials('drive')
            result = []
            try:
                fields = 'items(id,labels/trashed,title)'
                q = "title = 'Orion 2 GSP' and trashed = false"
                files = drive_service.files().list(fields=fields, q=q).execute()
                result.extend(files['items'])
                for item in result:
                    spreadsheet_key = item['id']
                    return spreadsheet_key

            except errors.HttpError, error:
                logs.logger.warn("An error occurred: " + str(error))

# Check Headers
def check_headers():
    """
    Check column headers in the previously created spreadsheet

    return: headers dictionary. Key = Column number (starting from 1):Value = Column name
    """
    logs.logger.info("Checking headers")
    client = get_client_credentials('sheets')
    spreadsheet_key = get_spreadsheet_key()
    worksheet_id = 'od6'  # default
    headers = {}

    try:
        query = gdata.spreadsheet.service.CellQuery()
        query.max_row = '1'
        cells = client.GetCellsFeed(spreadsheet_key, wksht_id=worksheet_id, query=query)
        for i, entry in enumerate(cells.entry):
            headers[i+1] = entry.cell.text

        return headers

    except:
        logs.logger.warn("An error occurred while checking headers")

# Move columns in Spreadsheet
def move_column(origin, destination):
    """
    Moves the required columns in the Spreadsheet to handle new incoming attributes
    Columns are moved as many spaces right as new attributes coming in

    param origin: origin column number
    type origin: int
    param destination: destination column number
    type destination: int

    return: None
    """
    logs.logger.info("Adjusting columns")

    from collections import OrderedDict
    client = get_client_credentials('sheets')
    spreadsheet_key = get_spreadsheet_key()
    worksheet_id = 'od6'  # default
    col_values = OrderedDict() #Ordered Dict

    try:

        try:
            logs.logger.info("Generating origin column")
            # Origin Column
            query_orig = gdata.spreadsheet.service.CellQuery()
            query_orig.return_empty = "true"
            query_orig.min_col = str(origin)
            query_orig.max_col = str(origin)
            cells_orig = client.GetCellsFeed(spreadsheet_key, wksht_id=worksheet_id, query=query_orig)

            batch_request_orig = gdata.spreadsheet.SpreadsheetsCellsFeed()

            for i, entry_orig in enumerate(cells_orig.entry):
                col_values[i] = entry_orig.cell.text
                if entry_orig.cell.text != None: # Avoid inserting new rows at the end due to the initial query
                    entry_orig.cell.inputValue = ''
                    batch_request_orig.AddUpdate(cells_orig.entry[i])

            # Execute origin column request
            client.ExecuteBatch(batch_request_orig, cells_orig.GetBatchLink().href)

        except:
            logs.logger.warn("An error occurred while generating origin column")

        #handle empty cells to do destination query
        col_values_clean = {}

        try:
            for i in enumerate(col_values.items()):
                if i[1][1] != None:
                    col_values_clean[i[0]] = i[1][1]

            for x in range(0,max(col_values_clean.keys())+1):
                if x not in col_values_clean.iterkeys():
                    col_values_clean[x] = '-'
        except:
            logs.logger.warn("An error occurred while handing empty cells in column")

        try:
            logs.logger.info("Generating destination column")
            # Destination Column
            query_dest = gdata.spreadsheet.service.CellQuery()
            query_dest.return_empty = "true"
            query_dest.max_row = str(len(col_values_clean))
            query_dest.min_col = str(destination)
            query_dest.max_col = str(destination)
            cells_dest = client.GetCellsFeed(spreadsheet_key, wksht_id=worksheet_id, query=query_dest)

            batch_request_dest = gdata.spreadsheet.SpreadsheetsCellsFeed()

            for i, entry_dest in enumerate(cells_dest.entry):
                entry_dest.cell.inputValue = col_values[i]
                batch_request_dest.AddUpdate(cells_dest.entry[col_values.keys().index(i)])

            # Execute destination column request
            client.ExecuteBatch(batch_request_dest, cells_dest.GetBatchLink().href)

        except:
            logs.logger.warn("An error occurred while generating destination column")

    except:
        logs.logger.warn("An error occurred while moving columns")
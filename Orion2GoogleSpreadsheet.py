__author__ = 'Daniel Fernandez Lazaro'

import urllib2
import webapp2
import json
from paste import httpserver
import logs
import unicodedata
import string
import httplib2
import http
from apiclient import errors
from googleapiclient.discovery import build
import time
import gdata.spreadsheet.service
import gdata.service
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client import tools
import yaml


logs.config_log()

### CREDENTIALS from the console ###

logs.logger.info("Loading properties from orion2cartodb.yaml")
file = open("credentials.yaml")
properties = yaml.load(file)
logs.logger.info("Loaded")

CLIENT_ID = properties['CLIENT_ID']
CLIENT_SECRET = properties['CLIENT_SECRET']
OAUTH_SCOPE = properties['OAUTH_SCOPE']
REDIRECT_URI = properties['REDIRECT_URI']

# Load TEST json data. TO BE DELETED IN FINAL RELEASE.
# Emulates DefaultHandler.post() catching data function
d = open("json_test_data.txt").read()
data = json.loads(d)

### CLASSES ###

class DefaultHandler(webapp2.RequestHandler):
    """Listen and Catch Context Broker data"""

    # Listen to Context Broker requests
    def post(self):
        """
        Listens to and Catches incoming Context Broker requests.
        Transforms incoming data and stores it to be processed later

        return: entities List of Entity dict objects.
                Each entity contains: Entity Name: Entity name // Attributes: attributes dict
        """
        global data
        try:
            data = json.loads(self.request.body)

        except:
            logs.logger.error("Malformed JSON")
            self.response.status_int = 403
            self.response.write("Malformed JSON")

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
            self.insert_data(entities)
            return entities

        except:
            logs.logger.error("An exception occurred")
            self.response.status_int = 403
            self.response.write("An exception occurred")

    # Insert data in file
    def insert_data(self, entities):
        """
        Inserts data coming from Context Broker into the Spreadsheet previously created

        return:None
        """
        client = get_client_credentials('sheets')
        spreadsheet_key = get_spreadsheet_key()
        worksheet_id = 'od6'  # default

        ### CONTEXT BROKER DATA  ###
        attributes = []
        headers = {}  # Attributes dict
        rows = []  # Data
        try:
            logs.logger.info("Extracting entities")

            for entity in entities:
                # Extract headers for columns in spreadsheet
                for attrib in entity['attributes']:
                    attributes.append(str(attrib))

                # Append rows to insert
                row = {'id': str(entity['entity_name']), 'date': time.strftime('%m/%d/%Y'), 'time': time.strftime('%H:%M:%S')}
                for key, value in entity['attributes'].iteritems():
                    row[str(key)] = "' " + str(value) # ' to avoid formatting errors when inserting data into spreadsheet's columns
                rows.append(row)
                logs.logger.info("Row: " + str(row))

            logs.logger.info("Formatting headers")

            # Format headers
            attributes = dict.fromkeys(attributes).keys()
            attributes.insert(0, 'id')
            attributes.append('date')
            attributes.append('time')
            for i, attrib in enumerate (attributes):
                headers[i+1] = attrib

            # Check headers in file
            current_headers = check_headers()

            # If No headers in file
            if not current_headers:
                logs.logger.info("Inserting headers")
                for i, header in enumerate(headers.values()):
                    entry = client.UpdateCell(row=1, col=i + 1, inputValue=header, key=spreadsheet_key, wksht_id=worksheet_id)
                    if not isinstance(entry, gdata.spreadsheet.SpreadsheetsCell):
                        logs.logger.warn("Header insert failed: '{0}'".format(entry))
            else:
                logs.logger.info("Adjusting headers")
                # If New Headers differ from Existing Headers
                if headers != current_headers:
                    # Detect missing headers
                    missing_headers = set(headers.values()) - set(current_headers.values())
                    # Columns to move
                    col_to_move = []
                    # Detect column number for each missing header
                    for h in missing_headers:
                        h = [key for key, value in headers.iteritems() if value == h][0]
                        col_to_move.append(h)
                    col_to_move.sort()

                    if len(col_to_move) == 1:
                        for i in range(col_to_move[0]+1, max(current_headers.keys())+1):
                            col_to_move.append(i)
                        # Higher value (last value) first --> next value...
                        for col in reversed(col_to_move):
                            move_column(col,col+1)
                    else:
                        #Compare col_to_move vs current_headers
                        matching_col = [item for item in current_headers if item in col_to_move]
                        for col in reversed(matching_col):
                            move_column(col,col+len(col_to_move))

                    # Insert headers in file
                    for i, header in enumerate(headers.values()):
                        entry = client.UpdateCell(row=1, col=i + 1, inputValue=header, key=spreadsheet_key, wksht_id=worksheet_id)
                        if not isinstance(entry, gdata.spreadsheet.SpreadsheetsCell):
                            logs.logger.warn("Header insert failed: '{0}'".format(entry))

            # Insert rows in file
            logs.logger.info("Inserting rows")
            for row in rows:
                try: ###-Consider batch request-###
                    entry = client.InsertRow(row, spreadsheet_key, worksheet_id)
                    if not isinstance(entry, gdata.spreadsheet.SpreadsheetsList):
                        logs.logger.warn("Row insert failed: '{0}'".format(entry))

                except Exception as e:
                    logs.logger.error("An error occurred: " + str(e))

            #logs.logger.info(str(len(rows)) + " rows inserted")

        except:
            logs.logger.warn("An error occurred while inserting data")

### FUNCTIONS ###

# Listen to Context Broker requests TEST. TO BE DELETED IN FINAL RELEASE
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

# Clients Auth and Credentials
def get_client_credentials(client):
    """
    Makes Google Oauth flow and store credentials in file.
    Creates an authenticated Drive client/ Spreadsheets client
    depending on the client param introduced.

    param client: "drive" for Drive client / "sheets" for Spreadsheets client
    type client: string

    return: Authenticated Drive/Spreadsheet client Object
    """
    try:
        logs.logger.info("Creating storage for credentials")
        storage = Storage("creds.dat")
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            logs.logger.info("Obtaining credentials")
            flags = tools.argparser.parse_args(args=[])
            flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)
            credentials = tools.run_flow(flow, storage, flags)

        if credentials.access_token_expired:
            logs.logger.info("Refreshing credentials")
            credentials.refresh(httplib2.Http())

        if client == "drive":
            try:
                logs.logger.info("Creating Drive client")
                dr_client = build('drive', 'v2', http=http)
                return dr_client
            except:
                logs.logger.warn("An error occurred while creating Drive client")

        elif client == "sheets":
            try:
                logs.logger.info("Creating Spreadsheets client")
                sp_client = gdata.spreadsheet.service.SpreadsheetsService(
                additional_headers={'Authorization': 'Bearer %s' % credentials.access_token})
                return sp_client
            except:
                logs.logger.warn("An error occurred while creating Spreadsheets client")
    except:
        logs.logger.warn("An error occurred while obtaining credentials")

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
            logs.logger.info("Checking file Spreadsheet Key")
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


### MAIN ###

# User account Auth for Google Drive + Spreadsheet creation
#insert_file(auth())

# Insert Incoming Data in Google Spreadsheet
a = DefaultHandler()
a.insert_data(post_TEST())

#def main():
    #application = webapp2.WSGIApplication([(r'/.*', DefaultHandler)], debug=True)
    #httpserver.serve(application, host=properties["orion2gsp_host"], port=properties["orion2gsp_port"])

#if __name__ == '__main__':
    #main()
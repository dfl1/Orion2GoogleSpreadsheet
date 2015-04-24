__author__ = 'Daniel Local'

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
    '''Catch Context Broker data'''

    # Listen to Context Broker requests
    def post(self):
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
        client = auth_client()
        spreadsheet_key = get_spreadsheet_key()
        worksheet_id = 'od6'  # default

        ### CONTEXT BROKER DATA  ###
        attributes = []
        headers = {}  # Attributes dict
        rows = []  # Data

        for entity in entities:
            # Extract headers for columns in spreadsheet
            for attrib in entity['attributes']:
                attributes.append(str(attrib))

            # Append rows to insert
            row = {'id': str(entity['entity_name']), 'date': time.strftime('%m/%d/%Y'), 'time': time.strftime('%H:%M:%S')}
            for key, value in entity['attributes'].iteritems():
                row[str(key)] = str(value)
            rows.append(row)
            print 'Row: ', row

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
            for i, header in enumerate(headers.values()):
                client.UpdateCell(row=1, col=i + 1, inputValue=header, key=spreadsheet_key, wksht_id=worksheet_id)
        else:
            # If New Headers != to Existing Headers
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
                    client.UpdateCell(row=1, col=i + 1, inputValue=header, key=spreadsheet_key, wksht_id=worksheet_id)

        # Insert rows in file
        for row in rows:
            try: #Consider batch request
                client.InsertRow(row, spreadsheet_key, worksheet_id)
            except Exception as e:
                print e
        print '{} Rows Inserted'.format(len(rows))


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

# OAuth flow and retrieve credentials
def auth():
    flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)
    authorize_url = flow.step1_get_authorize_url()
    print 'Go to the following link in your browser: ' + authorize_url
    code = raw_input('Enter verification code: ').strip()
    credentials = flow.step2_exchange(code)
    return credentials

# Spreadsheets Client Auth
def auth_client():
    storage = Storage("creds.dat")
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        flags = tools.argparser.parse_args(args=[])
        flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)
        credentials = tools.run_flow(flow, storage, flags)

    if credentials.access_token_expired:
        credentials.refresh(httplib2.Http())

    client = gdata.spreadsheet.service.SpreadsheetsService(
        additional_headers={'Authorization': 'Bearer %s' % credentials.access_token})
    return client

# Insert new file
def insert_file(credentials):
    http = credentials.authorize(httplib2.Http())
    drive_service = build('drive', 'v2', http=http)

    body = {
        'title': 'Orion 2 GSP',
        'description': 'description',
        'mimeType': 'application/vnd.google-apps.spreadsheet'
    }

    try:
        file = drive_service.files().insert(body=body).execute()
        with open('spreadsheet_key.yaml', 'w') as f:
            yaml.safe_dump(file['id'], f, default_flow_style=False)
        print 'File Created'
        return None

    except errors.HttpError, error:
        print 'An error occured: %s' % error
        return None

# Get Spreadsheet Key from file
def get_spreadsheet_key():
    if 'spreadsheet_key.yaml':
        try:
            with open('spreadsheet_key.yaml') as f:
                spreadsheet_key = yaml.load(f)
            return spreadsheet_key
        except:
            # Get spreadsheet_key
            credentials = auth()
            http = credentials.authorize(httplib2.Http())
            drive_service = build('drive', 'v2', http=http)
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
                print 'An error occurred: %s' % error

# Check Headers
def check_headers():
    client = auth_client()
    spreadsheet_key = get_spreadsheet_key()
    worksheet_id = 'od6'  # default
    headers = {}

    query = gdata.spreadsheet.service.CellQuery()
    query.max_row = '1'
    cells = client.GetCellsFeed(spreadsheet_key, wksht_id=worksheet_id, query=query)
    for i, entry in enumerate(cells.entry):
        headers[i+1] = entry.cell.text

    return headers

# Move colums in Spreadsheet
def move_column(origin, destination):
    from collections import OrderedDict
    client = auth_client()
    spreadsheet_key = get_spreadsheet_key()
    worksheet_id = 'od6'  # default
    col_values = OrderedDict() #Ordered Dict

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

    #handle empty cells to do destination query
    col_values_clean = {}
    for i in enumerate(col_values.items()):
        if i[1][1] != None:
            col_values_clean[i[0]] = i[1][1]

    for x in range(0,max(col_values_clean.keys())+1):
        if x not in col_values_clean.iterkeys():
            col_values_clean[x] = '-'

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

    client.ExecuteBatch(batch_request_orig, cells_orig.GetBatchLink().href)
    client.ExecuteBatch(batch_request_dest, cells_dest.GetBatchLink().href)


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
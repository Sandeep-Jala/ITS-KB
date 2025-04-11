from google.oauth2 import service_account
from googleapiclient.discovery import build
import jaydebeapi as dbdriver
import credential  # Ensure this contains your credentials like db_user, db_password
import pandas as pd
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from bs4 import BeautifulSoup
import re
from socket import gethostname
import os
from googleapiclient.errors import HttpError
import json
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd


# SCOPES for Google Docs and Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/spreadsheets']

# Authenticate to Google API
def authenticate():
    credentials = service_account.Credentials.from_service_account_file(
        'service_account.json', scopes=SCOPES)
    return credentials

def create_google_doc(folder_id, title, content):
    try:
        creds = authenticate()
        # Initialize services
        doc_service = build('docs', 'v1', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)

        # Create the Google Doc
        document = {'title': title}
        doc = doc_service.documents().create(body=document).execute()
        document_id = doc.get('documentId')

        # Update the document content
        doc_service.documents().batchUpdate(
            documentId=document_id,
            body={
                'requests': [{
                    'insertText': {
                        'location': {'index': 1},
                        'text': content
                    }
                }]
            }
        ).execute()

        # Move the document to the shared drive folder
        drive_service.files().update(
            fileId=document_id,
            addParents=folder_id,
            fields='id, parents',
            supportsAllDrives=True  # Important for shared drives
        ).execute()

        print(f'Created document with ID: {document_id}')
        return document_id

    except HttpError as error:
        print(f"An error occurred while creating Google Doc: {error}")
        return None

def update_google_doc(doc_id, content):
    try:
        creds = authenticate()
        doc_service = build('docs', 'v1', credentials=creds)
        
        doc = doc_service.documents().get(documentId=doc_id).execute()
        end_index = doc.get('body').get('content')[-1].get('endIndex')
        requests = [
            {
                'deleteContentRange': {
                    'range': {
                        'startIndex': 1,
                        'endIndex': end_index - 1
                    }
                }
            },
            {
                'insertText': {
                    'location': {'index': 1},
                    'text': content
                }
            }
        ]
        
        doc_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()
        
        return True
    except HttpError as error:
        print(f"An error occurred while updating Google Doc: {error}")
        return False

def load_tracking_dict_from_spreadsheet(spreadsheet_title, folder_id=None):
    try:
        # Authenticate and connect to Google Sheets
        creds = authenticate()
        gc = gspread.authorize(creds)

        # Search for the spreadsheet in the specified folder
        if folder_id:
            drive_service = build('drive', 'v3', credentials=creds)
            query = f"'{folder_id}' in parents and name = '{spreadsheet_title}' and mimeType = 'application/vnd.google-apps.spreadsheet'"
            results = drive_service.files().list(
    q=query,
    spaces='drive',
    fields='files(id, name)',
    supportsAllDrives=True,
    includeItemsFromAllDrives=True
).execute()
            files = results.get('files', [])

            if not files:
                print(f"Spreadsheet '{spreadsheet_title}' not found in folder ID {folder_id}.")
                return {}

            # Get the file ID of the spreadsheet
            file_id = files[0]['id']
            spreadsheet = gc.open_by_key(file_id)
        else:
            # Open the spreadsheet by its title (assumes it's in "My Drive")
            spreadsheet = gc.open(spreadsheet_title)

        # Load data from the spreadsheet into a DataFrame
        sheet = spreadsheet.sheet1
        df = get_as_dataframe(sheet, dtype=str, na_values=[]).dropna(how='all')  # Clean empty rows
        df.set_index('Article ID', inplace=True)

        # Convert the DataFrame to a dictionary
        tracking_dict = df.to_dict(orient='index')
        return tracking_dict
    except Exception as e:
        print(f"An error occurred while loading the tracking dictionary: {e}")
        return {}



def save_tracking_dict_to_spreadsheet(spreadsheet_title, tracking_dict, folder_id=None):
    try:
        # Convert the dictionary to a DataFrame
        df = pd.DataFrame.from_dict(tracking_dict, orient='index').reset_index()
        df.rename(columns={'index': 'Article ID'}, inplace=True)

        # Authenticate and connect to Google Sheets and Drive
        creds = authenticate()
        gc = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)

        # Open or create the spreadsheet
        try:
            spreadsheet = gc.open(spreadsheet_title)
            sheet = spreadsheet.sheet1
        except gspread.exceptions.SpreadsheetNotFound:
            # Create the spreadsheet if it doesn't exist
            spreadsheet = gc.create(spreadsheet_title)
            sheet = spreadsheet.sheet1

        # Move the spreadsheet to the folder if folder_id is provided
        if folder_id:
            # Get current parents
            file = drive_service.files().get(
                fileId=spreadsheet.id,
                fields='parents',
                supportsAllDrives=True
            ).execute()
            current_parents = file.get('parents', [])
            previous_parents = ",".join(current_parents)

            # Move the file only if it's not already in the desired folder
            if folder_id not in current_parents:
                drive_service.files().update(
                    fileId=spreadsheet.id,
                    addParents=folder_id,
                    removeParents=previous_parents,
                    fields='id, parents',
                    supportsAllDrives=True
                ).execute()

        # Write the DataFrame to the spreadsheet
        sheet.clear()
        set_with_dataframe(sheet, df)
        print(f"Tracking dictionary saved to spreadsheet: {spreadsheet_title}")
        print(f"Spreadsheet id: {spreadsheet.id}")
    except Exception as e:
        print(f"An error occurred while saving the tracking dictionary: {e}")

def document_exists(doc_id, folder_id):
    try:
        creds = authenticate()
        drive_service = build('drive', 'v3', credentials=creds)
        # Get the file's parents (folders) and trashed status
        file_metadata = drive_service.files().get(
            fileId=doc_id,
            fields='parents, trashed',
            supportsAllDrives=True
        ).execute()
        if file_metadata.get('trashed'):
            return False
        parents = file_metadata.get('parents', [])
        if parents == [folder_id]:
            return True
        else:
            return False
    except HttpError as error:
        if error.resp.status == 404:
            return False
        else:
            print(f"An error occurred while checking if document exists: {error}")
            raise



def create_docs_for_rows(folder_id, results, tracking_dict, spreadsheet_title, spreadsheet_folder_id):
    # Extract the current article IDs from the results
    current_article_ids = set(str(row[0]) for row in results)

    # Identify articles that are in the tracking_dict but not in current results
    articles_to_delete = set(tracking_dict.keys()) - current_article_ids

    # Delete documents and remove entries from tracking_dict for deleted articles
    for article_id in articles_to_delete:
        doc_id = tracking_dict[article_id]['doc_id']
        delete_status = delete_google_doc(doc_id)
        if delete_status:
            print(f"Deleted document for Article ID: {article_id}")
        else:
            print(f"Failed to delete document for Article ID: {article_id}")
        # Remove the article from the tracking dictionary
        del tracking_dict[article_id]
    for i, row in enumerate(results):
        article_id, title, body, summary, revision_number, url = row

        content = f"{url}\n\n"
        content += f"Title: {title}\n\n"
        content += f"Body:\n{body}\n\n"
        content += f"Summary:\n{summary}"
        doc_title = str(article_id)

        if str(article_id) in tracking_dict:
            doc_id = tracking_dict[str(article_id)]['doc_id']
            # Check if the document exists in the specified folder
            if not document_exists(doc_id, folder_id):
                print(f"Document with ID {doc_id} is not in the folder {folder_id}. Recreating the document.")
                del tracking_dict[str(article_id)]
                # Proceed to create a new document
                doc_id = create_google_doc(folder_id, doc_title, content)
                if doc_id:
                    tracking_dict[str(article_id)] = {'doc_id': doc_id, 'revision_number': revision_number}
                    save_tracking_dict_to_spreadsheet(spreadsheet_title, tracking_dict, spreadsheet_folder_id)  # Save the updated tracking dict
                    print(f"Successfully created document for Article ID: {article_id}")
                else:
                    print(f"Failed to create document for Article ID: {article_id}")
                continue  # Move on to the next article
            else:
                # Document exists in the folder, check revision number
                if tracking_dict[str(article_id)]['revision_number'] == revision_number:
                    # No update needed
                    print(f"Document for Article ID {article_id} is up to date.")
                    continue
                else:
                    # Need to update the document
                    update_status = update_google_doc(doc_id, content)
                    if update_status:
                        tracking_dict[str(article_id)]['revision_number'] = revision_number
                        save_tracking_dict_to_spreadsheet(spreadsheet_title, tracking_dict,spreadsheet_folder_id)  # Save the updated tracking dict
                        print(f"Updated document for Article ID: {article_id}")
                    else:
                        print(f"Failed to update document for Article ID: {article_id}")
        else:
            # Need to create new document
            doc_id = create_google_doc(folder_id, doc_title, content)
            if doc_id:
                # Save to tracking dict
                tracking_dict[str(article_id)] = {'doc_id': doc_id, 'revision_number': revision_number}
                save_tracking_dict_to_spreadsheet(spreadsheet_title, tracking_dict, spreadsheet_folder_id)  # Save the updated tracking dict
                print(f"Successfully created document for Article ID: {article_id}")
            else:
                print(f"Failed to create document for Article ID: {article_id}")

def delete_google_doc(doc_id):
    try:
        creds = authenticate()
        drive_service = build('drive', 'v3', credentials=creds)

        drive_service.files().delete(
            fileId=doc_id,
            supportsAllDrives=True
        ).execute()

        return True
    except HttpError as error:
        print(f"An error occurred while deleting Google Doc: {error}")
        return False


# Denodo Database connection and query execution
def denodo_database(driver_path, credential_user_id, credential_password, server_name, jdbc_port, server_database, query):
    conn_uri = f"jdbc:denodo://{server_name}:{jdbc_port}/{server_database}?userAgent={dbdriver.__name__}-{gethostname()}"
    cnxn = dbdriver.connect(
        "com.denodo.vdp.jdbc.Driver",
        conn_uri,
        driver_args={"useKerberos": "true", "user": credential_user_id, "password": credential_password, "ssl": "true"},
        jars=driver_path
    )

    cur = cnxn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cnxn.close()
    return results

# Convert raw results into a DataFrame
def creating_dataframe(results):
    df = pd.DataFrame(results, columns=["Article ID", "Title", "Body", "Summary", "Revision Number"])
    df["Body"] = df["Body"].apply(clean_html)
    df["Summary"] = df["Summary"].apply(clean_html)
    df["URL"] = df["Article ID"].apply(lambda x: f'https://teamdynamix.umich.edu/TDClient/30/Portal/KB/ArticleDet?ID={x}')
    return df

# Clean HTML from text
def clean_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

# Print the results (for debugging)
def print_results(results):
    print(results)


def main():
    # Set the folder ID where the spreadsheets are located
    spreadsheet_folder_id = '0ADjQe-gJ6HwnUk9PVA'
    spreadsheet_title_public = "Public Tracking"
    spreadsheet_title_um_login = "UM-Login Tracking"
    spreadsheet_title_support_staff = "Support Staff Tracking"

    # Load tracking dictionaries from Google Spreadsheets
    tracking_dict_for_public = load_tracking_dict_from_spreadsheet("Public Tracking", spreadsheet_folder_id)
    tracking_dict_for_um_login = load_tracking_dict_from_spreadsheet("UM-Login Tracking", spreadsheet_folder_id)
    tracking_dict_for_support_staff = load_tracking_dict_from_spreadsheet("Support Staff Tracking", spreadsheet_folder_id)

    # Set the folder ID where the documents will be created in Google Drive

    folder_id_public = "10EeZLQcNr9QIpH-IxcV9J_I8VKv-eiG9"  # Replace with your actual folder ID
    folder_id_um_login = "1QSxzyZEiybMIA7sATs7Mh03T6iQ3nQ-9"  # Replace with your actual folder ID
    folder_id_support_staff = "1XPj8BzKWm5IKxeaizH5lOfTwAYNT5Bpt" 
    
    query_public = """
    SELECT articleid, articlesubject, articlebody, articlesummary, revisionnumber
    FROM dw_tdx.knowledgebasearticlesreportview 
    WHERE articlestatusid = 3 AND ispublic = 1 AND clientappid = 30 ORDER BY articleid;
    """
    query_um_login = """
    SELECT articleid, articlesubject, articlebody, articlesummary, revisionnumber
    FROM dw_tdx.knowledgebasearticlesreportview 
    WHERE articlestatusid = 3 AND ispublic = 0 AND clientappid = 30 AND categorypathnames = 'U-M Login' ORDER BY articleid;
    """
    query_support_staff = """
    SELECT articleid, articlesubject, articlebody, articlesummary, revisionnumber
    FROM dw_tdx.knowledgebasearticlesreportview 
    WHERE articlestatusid = 3 AND ispublic = 0 AND clientappid = 30 AND categorypathnames <> 'U-M Login' ORDER BY articleid;
    """
    
    # Denodo connection details (make sure these are correctly defined in credential.py)
    credential_password = credential.db_password
    credential_user_id = credential.db_user
    denododriver_path = "./denodo-vdp-jdbcdriver-8.0-update-20240306.jar"
    denodoserver_name = "denodo.it.umich.edu"
    denodoserver_jdbc_port = "9999"
    denodoserver_database = "gateway"

    # Fetch data from the Denodo database
    results_public = denodo_database(denododriver_path, credential_user_id, credential_password, 
                             denodoserver_name, denodoserver_jdbc_port, denodoserver_database, query_public)
    results_um_login = denodo_database(denododriver_path, credential_user_id, credential_password,
                               denodoserver_name, denodoserver_jdbc_port, denodoserver_database, query_um_login)
    results_support_staff = denodo_database(denododriver_path, credential_user_id, credential_password,
                                denodoserver_name, denodoserver_jdbc_port, denodoserver_database, query_support_staff)
    
    #Convert the results to a DataFrame
    df_results_public = creating_dataframe(results_public)
    #print_results(df_results_public)
    df_results_um_login = creating_dataframe(results_um_login)
    #print_results(df_results_um_login)
    df_results_support_staff = creating_dataframe(results_support_staff)
    #print_results(df_results_support_staff)

    # Create documents and update tracking dictionaries
    create_docs_for_rows(folder_id_public, df_results_public.values.tolist(), tracking_dict_for_public, spreadsheet_title_public,spreadsheet_folder_id )
    create_docs_for_rows(folder_id_um_login, df_results_um_login.values.tolist(), tracking_dict_for_um_login, spreadsheet_title_um_login,spreadsheet_folder_id)
    create_docs_for_rows(folder_id_support_staff, df_results_support_staff.values.tolist(), tracking_dict_for_support_staff, spreadsheet_title_support_staff,spreadsheet_folder_id)
    

if __name__ == "__main__":
    main()

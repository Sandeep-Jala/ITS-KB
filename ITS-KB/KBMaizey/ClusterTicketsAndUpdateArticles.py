import os
import re
from socket import gethostname
import pandas as pd
import numpy as np
import jaydebeapi as dbdriver
import gspread
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from bs4 import BeautifulSoup
import hdbscan
import umap.umap_ as umap
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import StandardScaler

# ====== CONFIG ======
# Denodo connection
DENODO_HOST = "denodo.it.umich.edu"
DENODO_JDBC_PORT = "9999"
DENODO_DB = "gateway"
JDBC_DRIVER_JAR = "denodo-vdp-jdbcdriver-8.0-update-20240306.jar"

# Google credentials
GOOGLE_CRED_FILE = "still-resource-439600-p0-3de546fdb758.json"
SPREADSHEET_FOLDER_ID = "0AIl6WpKmR6tsUk9PVA"

# Query parameters
QUERY = """
SELECT tv.ticketid, tv.title, tv.description, tv.kbarticlesubject, tv.kbarticleid
  FROM dw_tdx.ticketsview tv
 WHERE tv.kbarticlesubject IS NOT NULL
   AND tv.appid = 31
   AND tv.classificationname IN ('Incident', 'Request')
   AND tv.closeddate > TIMESTAMP '2024-01-01 00:00:00.000'
   AND tv.sourcename IN ('Systems', 'Email', 'Web');
"""
MIN_GROUP_SIZE = 30
CLUSTER_OUTPUT_FOLDER = "clustered_output"
FINAL_OUTPUT = os.path.join(CLUSTER_OUTPUT_FOLDER, "final_clustered_dataset.csv")
TRACKING_SHEETS = [
    "Public Tracking",
    "UM-Login Tracking",
    "Support Staff Tracking",
]

# ============================================================================
# Utility functions
# ============================================================================

def connect_denodo(db_user, db_password):
    useragent = f"{dbdriver.__name__}-{gethostname()}"
    conn_uri = f"jdbc:denodo://{DENODO_HOST}:{DENODO_JDBC_PORT}/{DENODO_DB}?userAgent={useragent}"
    return dbdriver.connect(
        "com.denodo.vdp.jdbc.Driver",
        conn_uri,
        driver_args={"useKerberos":"true","user":db_user,"password":db_password,"ssl":"true"},
        jars=JDBC_DRIVER_JAR
    )


def fetch_ticket_kb_articles(db_user, db_password, output_csv="ticket_kb_articles.csv"):
    os.makedirs(CLUSTER_OUTPUT_FOLDER, exist_ok=True)
    cnxn = connect_denodo(db_user, db_password)
    cur = cnxn.cursor()
    cur.execute(QUERY)
    rows = [list(r) for r in cur.fetchall()]
    for row in rows:
        row.append(f"https://teamdynamix.umich.edu/TDClient/30/Portal/KB/ArticleDet.aspx?ID={row[4]}")
    cols = ["Ticket ID","Title","Description","Knowledge Base Article","KB Article ID","Knowledge Base Article Links"]
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(output_csv, index=False)
    cnxn.close()
    return df


def cluster_articles(input_csv):
    df = pd.read_csv(input_csv)
    df = df[:25000]
    counts = df['KB Article ID'].value_counts()
    ids_gt = counts[counts > MIN_GROUP_SIZE].index
    ids_le = counts[counts <= MIN_GROUP_SIZE].index
    df_gt = df[df['KB Article ID'].isin(ids_gt)]
    df_le = df[df['KB Article ID'].isin(ids_le)]
    model = SentenceTransformer("all-MiniLM-L6-v2")
    clustered = []
    for aid in ids_gt:
        grp = df_gt[df_gt['KB Article ID']==aid].copy()
        try:
            et = model.encode(grp['Title'].astype(str).tolist(), show_progress_bar=False)
            ed = model.encode(grp['Description'].astype(str).tolist(), show_progress_bar=False)
            emb = np.hstack([et, ed])
            um = umap.UMAP(n_neighbors=15,n_components=30,metric="cosine",init="random").fit_transform(emb)
            sc = StandardScaler().fit_transform(um)
            lbls = hdbscan.HDBSCAN(min_cluster_size=5,min_samples=3,metric="euclidean").fit_predict(sc)
            grp['Cluster']=lbls
            clustered.append(grp)
        except Exception:
            grp['Cluster']=-1
            clustered.append(grp)
    df_clustered = pd.concat(clustered + [df_le.assign(Cluster=-1)], ignore_index=True)
    os.makedirs(CLUSTER_OUTPUT_FOLDER, exist_ok=True)
    df_clustered.to_csv(FINAL_OUTPUT, index=False)
    print(f"All clustering done. Saved to {FINAL_OUTPUT}")
    return df_clustered


SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/spreadsheets']

# Authenticate to Google API
def authenticate():
    credentials = service_account.Credentials.from_service_account_file(
        'service_account.json', scopes=SCOPES)
    return credentials


def clean_html(html):
    return BeautifulSoup(html or '', 'html.parser').get_text(separator=' ', strip=True)


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


def update_google_doc(doc_id, content):
    try:
        creds = authenticate()
        doc_service = build('docs', 'v1', credentials=creds)
        
        doc = doc_service.documents().get(documentId=doc_id).execute()
        end_index = doc.get('body').get('content')[-1].get('endIndex')
        requests = [
            {
                'insertText': {
                    'location': {'index':  end_index-1 },
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


def update_docs_from_clusters():
    df = pd.read_csv(FINAL_OUTPUT)
    #tracking_public = load_tracking_dict_from_spreadsheet("Public Tracking", SPREADSHEET_FOLDER_ID)
    tracking_um = load_tracking_dict_from_spreadsheet("UM-Login Tracking", SPREADSHEET_FOLDER_ID)
    #tracking_support = load_tracking_dict_from_spreadsheet("Support Staff Tracking", SPREADSHEET_FOLDER_ID)
    #sources = [tracking_public, tracking_um, tracking_support]
    sources = [tracking_um]
    for (article, link), group in df.groupby(['Knowledge Base Article', 'Knowledge Base Article Links']):
        match = re.search(r'ID=([\w-]+)', link)
        if not match:
            print(f"Skipping article without valid ID: {article} | Link: {link}")
            continue

        article_id = match.group(1)

        doc_id = None
        for tracking_dict in sources:
            if article_id in tracking_dict:
                doc_id = tracking_dict[article_id]['doc_id']
                print(f"Found article ID '{article_id}' in tracking dict. Using Google Doc ID: {doc_id}")
                break

        if not doc_id:
            #print(f"Article ID '{article_id}' not found in any tracking dict. Skipping: {article}")
            continue

        # Compose content to append
        content_lines = [f"\nExample Queries that use this article:\n"]
        for _, row in group.iterrows():
            content_lines.append(f"- Title: {row['Title']}\n")
            desc_raw = row['Description']
            Clean_Description = clean_html(desc_raw) if pd.notna(desc_raw) else "  "

            content_lines.append(f"  Description: {Clean_Description}\n")
        content = ''.join(content_lines)
        try:
            update_google_doc(doc_id, content)
            print(f"Successfully updated Google Doc (ID: {doc_id}) for article '{article}'")
        except Exception as e:
            print(f"Failed to update Google Doc (ID: {doc_id}) for article '{article}': {e}")

if __name__=='__main__':
    from credential import db_user, db_password
    print("Fetching...")
    fetch_ticket_kb_articles(db_user, db_password)
    print("Clustering...")
    cluster_articles("ticket_kb_articles.csv")
    print("Updating docs...")
    update_docs_from_clusters()
    print("Done.")
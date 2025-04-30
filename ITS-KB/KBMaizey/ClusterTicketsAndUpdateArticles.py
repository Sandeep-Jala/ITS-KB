import os
import re
import time
from socket import gethostname

import pandas as pd
import numpy as np
import jaydebeapi as dbdriver
import gspread
from google.oauth2 import service_account
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
GOOGLE_CRED_FILE = "service_account.json"
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

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/spreadsheets'
]


# ====== UTILITY FUNCTIONS ======

def connect_denodo(db_user, db_password):
    useragent = f"{dbdriver.__name__}-{gethostname()}"
    conn_uri = f"jdbc:denodo://{DENODO_HOST}:{DENODO_JDBC_PORT}/{DENODO_DB}?userAgent={useragent}"
    return dbdriver.connect(
        "com.denodo.vdp.jdbc.Driver",
        conn_uri,
        driver_args={
            "useKerberos": "true",
            "user": db_user,
            "password": db_password,
            "ssl": "true"
        },
        jars=JDBC_DRIVER_JAR
    )


def fetch_ticket_kb_articles(db_user, db_password,
                             output_csv="ticket_kb_articles.csv"):
    os.makedirs(CLUSTER_OUTPUT_FOLDER, exist_ok=True)
    cnxn = connect_denodo(db_user, db_password)
    cur = cnxn.cursor()
    cur.execute(QUERY)
    rows = [list(r) for r in cur.fetchall()]
    # append the link
    for row in rows:
        row.append(f"https://teamdynamix.umich.edu/TDClient/30/Portal/KB/ArticleDet.aspx?ID={row[4]}")
    cols = [
        "Ticket ID", "Title", "Description",
        "Knowledge Base Article", "KB Article ID",
        "Knowledge Base Article Links"
    ]
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
        grp = df_gt[df_gt['KB Article ID'] == aid].copy()
        try:
            et = model.encode(grp['Title'].astype(str).tolist(),
                              show_progress_bar=False)
            ed = model.encode(grp['Description'].astype(str).tolist(),
                              show_progress_bar=False)
            emb = np.hstack([et, ed])
            um = umap.UMAP(
                n_neighbors=15,
                n_components=30,
                metric="cosine",
                init="random"
            ).fit_transform(emb)
            sc = StandardScaler().fit_transform(um)
            lbls = hdbscan.HDBSCAN(
                min_cluster_size=5,
                min_samples=3,
                metric="euclidean"
            ).fit_predict(sc)
            grp['Cluster'] = lbls
        except Exception:
            grp['Cluster'] = -1
        clustered.append(grp)

    df_clustered = pd.concat(clustered + [df_le.assign(Cluster=-1)],
                             ignore_index=True)
    os.makedirs(CLUSTER_OUTPUT_FOLDER, exist_ok=True)
    df_clustered.to_csv(FINAL_OUTPUT, index=False)
    print(f"All clustering done. Saved to {FINAL_OUTPUT}")
    return df_clustered


def authenticate():
    return service_account.Credentials.from_service_account_file(
        GOOGLE_CRED_FILE, scopes=SCOPES
    )


def clean_html(html):
    return BeautifulSoup(html or '', 'html.parser') \
        .get_text(separator=' ', strip=True)


def load_tracking_dict_from_spreadsheet(spreadsheet_title, folder_id=None):
    try:
        creds = authenticate()
        gc = gspread.authorize(creds)

        if folder_id:
            drive_service = build('drive', 'v3', credentials=creds)
            query = (
                f"'{folder_id}' in parents and "
                f"name = '{spreadsheet_title}' and "
                "mimeType = 'application/vnd.google-apps.spreadsheet'"
            )
            results = drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            files = results.get('files', [])
            if not files:
                print(f"Spreadsheet '{spreadsheet_title}' not found in folder.")
                return {}
            spreadsheet = gc.open_by_key(files[0]['id'])
        else:
            spreadsheet = gc.open(spreadsheet_title)

        sheet = spreadsheet.sheet1
        df = get_as_dataframe(sheet, dtype=str, na_values=[]).dropna(how='all')
        df.set_index('Article ID', inplace=True)
        return df.to_dict(orient='index')
    except Exception as e:
        print(f"Error loading tracking sheet '{spreadsheet_title}': {e}")
        return {}


# ====== RETRY HELPER ======

def batch_update_with_retries(doc_service, document_id, body,
                              max_attempts=2, wait_seconds=10):
    """
    Calls docs().batchUpdate() up to `max_attempts`. On HTTP 429, waits
    `wait_seconds` then retries. Returns the response or None.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return doc_service.documents().batchUpdate(
                documentId=document_id,
                body=body
            ).execute()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 429 and attempt < max_attempts:
                print(f"[Attempt {attempt}] Rate limit hit; retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                print(f"[Attempt {attempt}] Error: {e}")
                return None


def update_google_doc(doc_id, content):
    creds = authenticate()
    doc_service = build('docs', 'v1', credentials=creds)
    doc = doc_service.documents().get(documentId=doc_id).execute()
    end_index = doc['body']['content'][-1]['endIndex']
    body = {
        'requests': [{
            'insertText': {
                'location': {'index': end_index - 1},
                'text': content
            }
        }]
    }
    return batch_update_with_retries(doc_service, doc_id, body) is not None


def update_docs_from_clusters():
    df = pd.read_csv(FINAL_OUTPUT)
    creds = authenticate()
    doc_service = build('docs', 'v1', credentials=creds)
    tracking_um = load_tracking_dict_from_spreadsheet(
        "UM-Login Tracking", SPREADSHEET_FOLDER_ID
    )
    sources = [tracking_um]

    start_marker = "Example Requests and Incidents that were resolved using the above article"
    end_marker   = "end"

    for (article, link), group in df.groupby([
        'Knowledge Base Article', 'Knowledge Base Article Links'
    ]):
        m = re.search(r'ID=([\w-]+)', link)
        if not m: 
            continue
        article_id = m.group(1)

        # look up doc_id
        doc_id = next(
            (t[article_id]['doc_id'] for t in sources if article_id in t),
            None
        )
        if not doc_id:
            continue

        # fetch the doc once
        doc = doc_service.documents().get(documentId=doc_id).execute()
        content = doc.get('body', {}).get('content', [])
        end_index = doc['body']['content'][-1]['endIndex']

        # find delete range
        start_index = end_index_to_delete = None
        for elem in content:
            if 'paragraph' not in elem:
                continue
            for pe in elem['paragraph']['elements']:
                tr = pe.get('textRun')
                if not tr:
                    continue
                text = tr['content']
                idx  = pe['startIndex']
                if start_marker in text:
                    start_index = idx + text.index(start_marker) + len(start_marker)
                if end_marker in text:
                    end_index_to_delete = idx + text.index(end_marker)

        requests = []
        # if we found an old section to clear, add the deleteContentRange request
        if start_index is not None and end_index_to_delete and end_index_to_delete > start_index:
            requests.append({
                'deleteContentRange': {
                    'range': {
                        'startIndex': start_index,
                        'endIndex': end_index_to_delete
                    }
                }
            })
            insert_at = start_index
            header, footer = "\n", ""
        else:
            # no old section → append at the very end
            insert_at = end_index - 1
            header = f"\n{start_marker}\n"
            footer = f"{end_marker}\n"

        # build the insertText request
        text_lines = [header, "\n\n"]
        for _, row in group.iterrows():
            desc = clean_html(row['Description']) if pd.notna(row['Description']) else ''
            text_lines.append(f"- Title: {row['Title']}\n")
            text_lines.append(f"  Description: {desc}\n")
        text_lines.append(footer)

        requests.append({
            'insertText': {
                'location': {'index': insert_at},
                'text': ''.join(text_lines)
            }
        })

        # send both in one RPC
        resp = batch_update_with_retries(doc_service, doc_id,
                                         {'requests': requests})
        if resp:
            print(f"Updated Doc {doc_id} for article '{article}'")
        else:
            print(f"Skipped Doc {doc_id} after retry attempts.")


if __name__ == '__main__':
    from credential import db_user, db_password

    print("Fetching tickets & KB articles…")
    fetch_ticket_kb_articles(db_user, db_password)

    print("Clustering articles…")
    cluster_articles("ticket_kb_articles.csv")

    print("Updating Google Docs from clusters…")
    update_docs_from_clusters()

    print("All done!")

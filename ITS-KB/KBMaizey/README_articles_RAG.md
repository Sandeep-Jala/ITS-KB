# Article Processing and RAG Model Testing

In this project, we work with a knowledge base of articles, create Google Docs from them, perform clustering on these articles, update them when needed, and finally test a Retrieval-Augmented Generation (RAG) model with new data.

## Project Overview
1. **Create Google Docs of Articles**: We fetch article data from a database and create Google Docs for each article (if they don't already exist). We also track each doc's status using Google Spreadsheets.
2. **Cluster Articles**: We use a notebook (DataClusteringForKB.ipynb) to cluster articles for better organization and understanding.
3. **Update KB Articles**: We use a separate notebook (UpdateTicketsInKBArticles.ipynb) to update existing Google Docs or the database with any changes or relevant new information.
4. **Test RAG Model**: Finally, we use a notebook (TestingKBMaizey.ipynb) to test a RAG model on fresh data.

## Prerequisites
- **Python 3.8+** recommended.
- Required Python libraries: `pandas`, `numpy`, `jaydebeapi`, `beautifulsoup4`, `gspread`, `gspread-dataframe`, `google-api-python-client`, `google-auth`, `scikit-learn`, etc.

You can install them via:

```bash
pip install pandas numpy jaydebeapi beautifulsoup4 gspread gspread-dataframe google-api-python-client google-auth scikit-learn
```

## Environment Setup

1. **.env File**: Create a file named `.env` at the root of your project:
   ```env
   token=""
   ```
   (Fill in the `token` with any secret token you require for your project.)

2. **Database Credentials**: In `credential.py` , add your database username and password:
   ```python
db_user = 'your_db_username'
db_password = 'your_db_password'
```

3. **Google Service Account**:
   - Obtain and place your `service_account.json`  in the project root.
   - Ensure it has the correct project credentials and keys.
   - Give mangaerial permission to the service account email address.

## File Descriptions

1. **credential.py**: Stores database credentials.
2. **CreatingGdocForArticles.py**:
   - Connects to the Denodo database.
   - Creates or updates Google Docs for each article.
   - Uses Google Sheets to track existing doc IDs.
3. **DataClusteringForKB.ipynb**:
   - Contains scripts to cluster articles.
4. **UpdateTicketsInKBArticles.ipynb**:
   - Updates relevant Google Docs/KB articles.
5. **TestingKBMaizey.ipynb**:
   - Tests the RAG model with newly introduced data.

## Usage

1. **Create Google Docs**:
   ```bash
   python CreatingGdocForArticles.py
   ```
   - This will create new Google Docs for each relevant article or update existing docs.
   - Requires valid credentials and service account files.

2. **Cluster Articles**:
   - Open and run `DataClusteringForKB.ipynb` in Jupyter to cluster or group the articles.

3. **Update KB Articles**:
   - Open and run `UpdateTicketsInKBArticles.ipynb` to update existing Google Docs with new or revised article info.

4. **Test RAG Model**:
   - Open and run `TestingKBMaizey.ipynb` to see how the RAG model performs on the newly updated or clustered articles.

## Notes & Troubleshooting
- Ensure that the `.env` file and `credential.py` are **not** shared publicly (they contain sensitive info).
- If you see permission or authentication errors, verify `service_account.json` has correct roles and scopes.

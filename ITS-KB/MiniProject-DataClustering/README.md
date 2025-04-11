# Service-Based Data Clustering Project

This project performs clustering on service-related data extracted from Denodo, a data virtualization platform. The clustering algorithm organizes data according to services, which helps in categorizing and analyzing service tickets efficiently.

## Project Structure

```
project/
├── credential.py
├── DataClusteringByService.ipynb
├── Denodo_Create_data_by_services.py
├── denodo-vdp-jdbcdriver-8.0-update-20240306.jar (required)
└── ticketsview_Services.csv (required)
```

## Prerequisites

### Python
- Python 3.8 or newer

### Python Dependencies
Install using pip:

```bash
pip install pandas jaydebeapi beautifulsoup4 numpy scikit-learn notebook
```

### Additional Files
- `denodo-vdp-jdbcdriver-8.0-update-20240306.jar`: JDBC driver for Denodo.
- `ticketsview_Services.csv`: CSV file containing service names under the column `servicename`.

## Credential Configuration

Update `credential.py` with your Denodo credentials:

```python
db_user = 'your_denodo_username'
db_password = 'your_denodo_password'
```

## Step-by-Step Instructions

### Step 1: Setup

- Ensure all files are in the same directory.
- Place the JDBC driver `.jar` file and the CSV file `ticketsview_Services.csv` in the project root directory.

### Step 2: Generate Service Data

Run the Python script to generate data CSV files from Denodo:

```bash
python Denodo_Create_data_by_services.py
```

- The script generates CSV files in the `Denodo_services` folder, each corresponding to a service.

### Step 3: Run Clustering Notebook

- Open `DataClusteringByService.ipynb` with Jupyter Notebook:

```bash
jupyter notebook DataClusteringByService.ipynb
```

- Follow the notebook's instructions step by step. Ensure the generated service data files are correctly referenced in the notebook.

### Step 4: Review and Analyze Results

- Clustering results will be displayed and saved within the notebook, providing insights into ticket grouping based on service similarity.


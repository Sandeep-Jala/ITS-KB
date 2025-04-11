import credential
import pandas as pd
import jaydebeapi as dbdriver
from bs4 import BeautifulSoup
from socket import gethostname
import pandas as pd
import os
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
    df = pd.DataFrame(results, columns=["Ticket_ID", "Title", "Servicename", "Description", "Feed"])
    df["Description"] = df["Description"].apply(clean_html)
    df["Feed"] = df["Feed"].apply(clean_html)
    return df

# Clean HTML from text
def clean_html(html):
    if pd.isna(html) or html is None:  # Handle None or NaN values
        return ""
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

# Print the results (for debugging)
def print_results(results):
    print(results)

def main():
# Load the service names from the CSV file
    service_csv_file = "ticketsview_Services.csv"  # Update with the correct file path
    service_df = pd.read_csv(service_csv_file)
    service_names = service_df['servicename'].tolist()  # Assuming column name is 'servicename'
 # Example list of service names
    # Denodo connection details (ensure credentials are correctly defined in credential.py)
    credential_password = credential.db_password
    credential_user_id = credential.db_user
    denododriver_path = "denodo-vdp-jdbcdriver-8.0-update-20240306.jar"
    denodoserver_name = "denodo.it.umich.edu"
    denodoserver_jdbc_port = "9999"
    denodoserver_database = "gateway"

    # Function to execute the query for each service
    for service_name in service_names:
        output_folder = "Denodo_services"


        os.makedirs(output_folder, exist_ok=True)
        query = f"""
        SELECT tv.ticketid, tv.title, tv.servicename, tv.description, 
            LISTAGG(iu.iu_body, ' || ') WITHIN GROUP (ORDER BY iu.iu_datecreated) AS aggregated_iu_body
        FROM dw_tdx.ticketsview tv
        Left JOIN dw_tdx.itemupdates iu
        ON tv.ticketid = iu.iu_itemid
        WHERE tv.appid = 31 AND tv.servicename = '{service_name}' AND tv.closeddate > TIMESTAMP '2023-01-01 00:00:00.000'
        GROUP BY tv.ticketid, tv.title, tv.servicename, tv.description;
        """

        # Execute query
        results = denodo_database(denododriver_path, credential_user_id, credential_password, 
                                denodoserver_name, denodoserver_jdbc_port, denodoserver_database, query)
        
        # Convert results to DataFrame
        df_results = creating_dataframe(results)
        
        # Save results to a CSV file, named dynamically based on the service
        output_file = os.path.join(output_folder, f"{service_name}.csv".replace(" ", ""))  # Replacing spaces with underscores for filenames
        df_results.to_csv(output_file, index=False)

        print(f"Saved results for service: {service_name} -> {output_file}")

    print("All queries executed successfully!")

    
if __name__ == "__main__":
    main()
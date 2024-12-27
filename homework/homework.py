import pandas as pd
import zipfile
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortgage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_date: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - cons_price_idx
    - euribor_three_months
    """

    input_folder = 'files/input/'
    output_folder = 'files/output/'

    os.makedirs(output_folder, exist_ok=True)

    zip_files = [f for f in os.listdir(input_folder) if f.endswith('.zip')]

    client_data = []
    campaign_data = []
    economics_data = []

    for zip_file in zip_files:
        with zipfile.ZipFile(os.path.join(input_folder, zip_file), 'r') as z:
            for file_name in z.namelist():
                with z.open(file_name) as f:
                    df = pd.read_csv(f)
                    client_data.append(df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']])
                    campaign_data.append(df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'day', 'month']])
                    economics_data.append(df[['client_id', 'cons_price_idx', 'euribor_three_months']])

    client_df = pd.concat(client_data)
    campaign_df = pd.concat(campaign_data)
    economics_df = pd.concat(economics_data)

    client_df['job'] = client_df['job'].str.replace('.', '').str.replace('-', '_')
    client_df['education'] = client_df['education'].str.replace('.', '_').replace('unknown', pd.NA)
    client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    client_df['mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
    campaign_df['last_contact_date'] = pd.to_datetime(campaign_df['day'].astype(str) + '-' + campaign_df['month'].astype(str) + '-2022', format='%d-%b-%Y')
    campaign_df = campaign_df.drop(columns=['day', 'month'])

    client_df.to_csv(os.path.join(output_folder, 'client.csv'), index=False)
    campaign_df.to_csv(os.path.join(output_folder, 'campaign.csv'), index=False)
    economics_df.to_csv(os.path.join(output_folder, 'economics.csv'), index=False)

if __name__ == "__main__":
    clean_campaign_data()

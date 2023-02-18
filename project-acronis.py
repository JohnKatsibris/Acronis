import os
import requests
from base64 import b64encode
import pandas as pd


CLIENT_ID = 'client id'
CLIENT_SECRET = 'client secret'
DATACENTER_URL = 'https://eu8-cloud.acronis.com'
BASE_URL = f'{DATACENTER_URL}/api/2'
AUTH_URL = f'{BASE_URL}/idp/token'
TASK_MANAGER_URL = f'{DATACENTER_URL}/api/task_manager/v2'
TENANT_USAGE_URL = f'{TASK_MANAGER_URL}/tenants/usages'

# Set the tenant IDs
TENANT_ID_PARAMS = ['tenant ids'
]



def create_token():
    encoded_client_creds = b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode('ascii'))
    basic_auth = {'Authorization': 'Basic ' + encoded_client_creds.decode('ascii')}
    headers = {'Content-Type': 'application/x-www-form-urlencoded', **basic_auth}
    response = requests.post(AUTH_URL, headers=headers, data={'grant_type': 'client_credentials'})
    return response.json()['access_token']

def make_request(token):
    auth = {'Authorization': 'Bearer ' + token}
    params = {'tenants': ','.join(TENANT_ID_PARAMS)}
    response = requests.get(TENANT_USAGE_URL, headers=auth, params=params)
    status_code = response.status_code
    return response.json(), status_code

def make_another_request(token):
    auth = {'Authorization': 'Bearer ' + token}
    params = {'tenants': ','.join(TENANT_ID_PARAMS)}
    response = requests.get(f'{BASE_URL}/tenants/usages', headers=auth, params=params)
    status_code = response.status_code
    return response.json(), status_code

def get_tenant_name(token, tenant_id):
    auth = {'Authorization': 'Bearer ' + token}
    response = requests.get(f'{BASE_URL}/tenants', headers=auth)
    status_code = response.status_code
    return response.json()['name'], status_code

def main():
    token = create_token()
    response, status_code = make_request(token)
    another_response, another_status_code = make_another_request(token)
    return another_response

if __name__ == "__main__":
    content = main()
    conversion_factor = 9.31 * 10**-10
    df_final = pd.DataFrame()  # Create an empty dataframe to store the final merged data
    for tenant in range(0, len(content['items'])):
        tenant = content['items'][tenant]['usages']
        df = pd.DataFrame.from_dict(tenant)
        df = df[df['absolute_value'] != 0]
        df = df[df['value'] != 0]
        df = df[df['measurement_unit'].isin(['bytes', 'quantity'])]  # Filter the data to include 'bytes' and 'quantity'
        num_rows = df.shape[0]  # Get the number of rows
        df_bytes = df[df['measurement_unit'] == 'bytes']  # Filter the data to only include 'bytes'
        df_bytes['absolute_value'] = df_bytes['absolute_value'] * conversion_factor
        df_bytes['value'] = df_bytes['value'] * conversion_factor
        df = pd.concat([df[df['measurement_unit'] == 'quantity'], df_bytes])  # Combine the filtered data frames
        df.drop(['offering_item', 'infra_id'], axis=1, inplace=True)  # Drop the unused columns

        tenant_id = tenant[0]['tenant_id']
        filename = f'tenant_{tenant_id}.csv'
        df.to_csv(filename, index=False)
        print(f"Number of rows for tenant {tenant_id}: {num_rows}")

        # Add the processed data for the current tenant to the final dataframe
        df_final = pd.concat([df_final, df], ignore_index=True)

        # Remove the individual tenant CSV file
        os.remove(filename)

        # Save the final merged dataframe to a single CSV file
        df_final.to_csv('merged_tenants.csv', index=False)

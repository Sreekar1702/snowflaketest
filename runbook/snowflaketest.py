#!/usr/bin/env python

#!/usr/bin/env python

import os
import requests
import json
import snowflake.connector

def update_storage_firewall():
    # --- 1. GET IPs FROM SNOWFLAKE ---
    ctx = snowflake.connector.connect(
        user='SREEKAR',
        password='Sreekar1702300798',
        account='FPXSZNN-XX15283',
        warehouse='COMPUTE_WH',
        database='USERDATA',
        schema='USERDETAILS'
    )
    
    try:
        cs = ctx.cursor()
        cs.execute("SELECT IP_ADDRESS FROM TEMP_SNOWFLAKE_IPS")
        # Storage Account expects raw IPs or CIDR (e.g., "163.116.214.115")
        ip_list = [row[0].strip() for row in cs.fetchall()]
        print(f"Retrieved {len(ip_list)} IPs from Snowflake.")
    finally:
        cs.close()
        ctx.close()

    # --- 2. GET AZURE AUTH TOKEN ---
    endpoint = os.environ["IDENTITY_ENDPOINT"]
    header = os.environ["IDENTITY_HEADER"]
    token_url = f"{endpoint}?resource=https://management.azure.com/&api-version=2019-08-01"
    
    token_res = requests.get(token_url, headers={"X-IDENTITY-HEADER": header})
    token = token_res.json()['access_token']

    # --- 3. CONFIGURE STORAGE API DETAILS ---
    sub_id = "944bb283-424c-471b-84f4-ff74cb4408b5"
    rg = "SnowflakeRG"
    storage_account = "azurestorage1702"
    
    # API URL for Microsoft.Storage
    url = f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{storage_account}?api-version=2023-05-01"

    # --- 4. BUILD THE IP RULES PAYLOAD ---
    # This specifically targets 'Enabled from selected networks'
    ip_rules = []
    for ip in ip_list:
        ip_rules.append({
            "value": ip,
            "action": "Allow"
        })

    payload = {
        "properties": {
            "publicNetworkAccess": "Enabled", # Ensures Public Access is not 'Disabled'
            "networkAcls": {
                "defaultAction": "Deny",       # This triggers 'Selected Networks' mode
                "bypass": "Logging, Metrics, AzureServices",
                "ipRules": ip_rules,
                "virtualNetworkRules": []      # Keeps VNet rules empty or unchanged
            }
        }
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # --- 5. EXECUTE THE UPDATE ---
    print(f"Updating Storage Firewall for {storage_account}...")
    response = requests.patch(url, data=json.dumps(payload), headers=headers)

    if response.status_code in [200, 202]:
        print("Success! 'Selected Networks' firewall updated with Snowflake IPs.")
    else:
        print(f"Failed! Status: {response.status_code}")
        print(f"Response: {response.text}")

if __name__ == "__main__":
    update_storage_firewall()

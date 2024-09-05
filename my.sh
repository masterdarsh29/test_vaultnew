#!/bin/bash
 
if [ -z "$VAULT_ADDR" ]; then
  echo "Error: VAULT_ADDR is not set."
  exit 1
fi
 
if [ -z "$VAULT_TOKEN" ]; then
  echo "Error: VAULT_TOKEN is not set."
  exit 1
fi
 
SECRET_PATH="secret/myapp"  # Update this path to the appropriate one you want to query
 
# Retrieve and display secrets from the specified path
echo "Retrieving secrets from path: $SECRET_PATH"
vault kv get $SECRET_PATH
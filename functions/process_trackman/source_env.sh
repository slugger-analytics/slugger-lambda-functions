#!/bin/bash

# Check if .env file exists
if [ ! -f .env ]; then
  echo ".env file not found!"
  exit 1
fi

# Export each line from the .env file
while IFS='=' read -r key value; do
  # Skip empty lines or comments
  if [[ -z "$key" || "$key" =~ ^# ]]; then
    continue
  fi

  # Remove quotes if any
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"

  export "$key=$value"
done < .env

echo "Environment variables set from .env"

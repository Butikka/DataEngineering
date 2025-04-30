# utils/azure_utils.py
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.certificates import CertificateClient
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings

# utility
def get_secret(vault_url, secret_name):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value

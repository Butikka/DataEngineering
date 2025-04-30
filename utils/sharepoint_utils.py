# utils/sharepoint_utils.py
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

# utility
def get_sharepoint_context(site_url, client_id, client_secret):
    creds = ClientCredential(client_id, client_secret)
    return ClientContext(site_url).with_credentials(creds)

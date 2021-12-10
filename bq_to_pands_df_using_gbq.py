import pandas_gbq
from google.oauth2 import service_account
# TODO: Set project_id to your Google Cloud Platform project ID.
# project_id = "my-project"
credentials = service_account.Credentials.from_service_account_info(<credential file content>)
sql = """
SELECT * FROM `project-id.curated.ata_curated` LIMIT 1000
"""
df = pandas_gbq.read_gbq(sql, project_id="project_id", credentials=credentials)
print (df)
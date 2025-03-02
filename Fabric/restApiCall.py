from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import regexp_replace, sha2, concat_ws, lit, current_timestamp, col, when, to_date, to_timestamp, date_format, format_number, lpad
from notebookutils import mssparkutils
from azure.keyvault.secrets import SecretClient
import requests
import time
from pyspark.sql import SparkSession
import jwt


# Initialize Spark session
spark = SparkSession.builder.appName("FetchData").getOrCreate()


# Variables for API usage.
keyVaultUrl = 'https://myazureresource.vault.azure.net/'
token = notebookutils.credentials.getSecret(f'{keyVaultUrl}','ApiToken')
companyId = 10000 # this is company id and it is used in almost every api call.
# projectId = 10000 # example projectId
baseUrl = 'https://api.domain.com'
apiEndpoint = f"/v0.1/companies/{companyId}/projects/projectId"
urlEndpoint = f"{baseUrl}{apiEndpoint}"

# Create variable for auth headers.
headers = {
    "Authorization": f"Bearer {token}"
}


def fetch_assignments_for_project(apiUrl, projectId, requestHeaders, max_retries=3, timeout=10):
    # Construct the URL
    url = apiUrl.replace("projectId", str(projectId))
    
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(url, headers=requestHeaders, timeout=timeout)
            response.raise_for_status()  # Raise an error for HTTP failures (4xx, 5xx)

            # Attempt to parse JSON response
            try:
                data = response.json()
                if not isinstance(data, (dict, list)):  # Ensure valid JSON
                    print("Error: Unexpected response format. Expected JSON object or list.")
                    return [] 
                
                assignments = data.get("assignments", [])
                return assignments 

            except ValueError:
                print(f"Error: Received non-JSON response for projectId {projectId}. Check API.")
                return []

        except requests.exceptions.Timeout:
            print(f"Timeout error on attempt {retry_count + 1} for projectId {projectId}. Retrying...")
        except requests.exceptions.ConnectionError:
            print(f"Connection error on attempt {retry_count + 1} for projectId {projectId}. Retrying...")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error for projectId {projectId}: {e}. Status code: {response.status_code}")
            return []
        except requests.exceptions.RequestException as e:
            print(f"Unexpected error for projectId {projectId}: {e}")
            return []

        retry_count += 1
        wait_time = 2**retry_count  # Exponential backoff (2, 4, 8 sec)
        print(f"Retrying in {wait_time} seconds...")
        time.sleep(wait_time)

    print(f"Max retries reached. Failed to fetch assignments for projectId {projectId}.")
    return []


# Step 1: Read the Delta table containing projects
projects_df = spark.read.format("delta").load("Tables/projects")
# Extract project IDs
project_ids = [row["projectId"] for row in projects_df.select("projectId").distinct().collect()]


# Step 2: Fetch assignments data for each project
all_assignments = []

for project_id in project_ids:
    # Fetch data from the API for the current project ID
    assignments = fetch_assignments_for_project(urlEndpoint, project_id, headers)
    # Check the response type: is it a list, tuple or something else else.
    # print(f"Debug: Response for projectId {project_id}: {type(assignments)} -> {assignments}")

    # Ensure the response is a list
    if not isinstance(assignments, list):
        print(f"Warning: Unexpected response format for projectId {project_id}, expected a list.")
        continue  # Skip to the next project_id

    # Extract projectId and id from each assignment
    for assignment in assignments:
        if isinstance(assignment, dict):
            all_assignments.append({
                "projectId": assignment.get("projectId", project_id),  
                "projectAssignmentId": assignment.get("id", "NA") 
            })
        else:
            print(f"Warning: Skipping invalid assignment format in projectId {project_id}.")

# Step 3: Define schema for the DataFrame
schema = StructType([
    StructField("projectId", StringType(), True),
    StructField("projectAssignmentId", StringType(), True)
])

# Step 4: Create a single PySpark DataFrame from the aggregated data
df_projects_assignments = spark.createDataFrame(all_assignments, schema=schema)

# Show the final DataFrame
df_projects_assignments.show()


# Table name for link table
tableName = 'link_project_assignment'
# Write the delta table to delta lake
df_projects_assignments.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + tableName)
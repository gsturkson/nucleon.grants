import os
import requests
from datetime import datetime, timedelta
from zipfile import ZipFile
import pandas as pd
import xml.etree.ElementTree as ET

# Step 1: Define the URL and file names
grantsExtractURL = 'https://prod-grants-gov-chatbot.s3.amazonaws.com/extracts/'
dateString = datetime.today().strftime('%Y%m%d')
fileStem = 'GrantsDBExtract'
fileEnd = 'v2.zip'
fullFileName = fileStem + dateString + fileEnd
queryURL = grantsExtractURL + fullFileName

# Step 2: Define file paths
zip_file_path = os.path.join('grantsRaw/', fullFileName)
xml_file_path = os.path.join('grantsOutput/', f'{fileStem}{dateString}v2.xml')

# Step 3: Check if files already exist
if not os.path.exists(zip_file_path) or not os.path.exists(xml_file_path):
    # Step 4: Download the ZIP file with progress indication if it doesn't exist
    print(queryURL)
    try:
        response = requests.get(queryURL, stream=True)
        response.raise_for_status()  # Check if the request was successful

        # Ensure the response contains a ZIP file
        if 'application/zip' in response.headers.get('Content-Type', ''):
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024  # 1 Kilobyte
            progress_bar = 0

            with open(zip_file_path, 'wb') as file:
                for data in response.iter_content(block_size):
                    progress_bar += len(data)
                    file.write(data)
                    done = int(50 * progress_bar / total_size)
                    print(f"\r[{'=' * done}{' ' * (50 - done)}] {progress_bar / total_size:.2%}", end='\r')
            print("\nZIP file downloaded successfully.")
        else:
            print("Error: The URL did not return a ZIP file.")
            exit()

    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")
        exit()

    # Step 5: Extract the ZIP file if XML file doesn't exist
    try:
        with ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('grantsOutput/')
        print("ZIP file extracted successfully.")
    except Exception as e:
        print(f"Error extracting ZIP file: {e}")
        exit()
else:
    print("ZIP and XML files already exist, skipping download and extraction.")

# Step 6: Parse the XML file
try:
    tree = ET.parse(xml_file_path)  # Replace with the actual XML file path if different
    root = tree.getroot()
except ET.ParseError as e:
    print(f"Error parsing XML file: {e}")
    exit()

# Define the namespace
namespace = {'ns': 'http://apply.grants.gov/system/OpportunityDetail-V1.0'}

# Define a list to store the extracted data
data = []

# Iterate through the XML tree and extract relevant information
for grant in root.findall('ns:OpportunitySynopsisDetail_1_0', namespace):
    grant_info = {
        'Opportunity ID': grant.findtext('ns:OpportunityID', default='', namespaces=namespace),
        'Opportunity Title': grant.findtext('ns:OpportunityTitle', default='', namespaces=namespace),
        'Opportunity Number': grant.findtext('ns:OpportunityNumber', default='', namespaces=namespace),
        'Opportunity Category': grant.findtext('ns:OpportunityCategory', default='', namespaces=namespace),
        'Funding Instrument Type': grant.findtext('ns:FundingInstrumentType', default='', namespaces=namespace),
        'Category of Funding Activity': grant.findtext('ns:CategoryOfFundingActivity', default='', namespaces=namespace),
        'Category Explanation': grant.findtext('ns:CategoryExplanation', default='', namespaces=namespace),
        'CFDA Number(s)': grant.findtext('ns:CFDANumbers', default='', namespaces=namespace),
        'Eligible Applicants': grant.findtext('ns:EligibleApplicants', default='', namespaces=namespace),
        'Additional Information on Eligibility': grant.findtext('ns:AdditionalInformationOnEligibility', default='', namespaces=namespace),
        'Agency Code': grant.findtext('ns:AgencyCode', default='', namespaces=namespace),
        'Agency Name': grant.findtext('ns:AgencyName', default='', namespaces=namespace),
        'Post Date': grant.findtext('ns:PostDate', default='', namespaces=namespace),
        'Close Date': grant.findtext('ns:CloseDate', default='', namespaces=namespace),
        'Last Updated Date': grant.findtext('ns:LastUpdatedDate', default='', namespaces=namespace),
        'Award Ceiling': grant.findtext('ns:AwardCeiling', default='', namespaces=namespace),
        'Award Floor': grant.findtext('ns:AwardFloor', default='', namespaces=namespace),
        'Estimated Total Program Funding': grant.findtext('ns:EstimatedTotalProgramFunding', default='', namespaces=namespace),
        'Expected Number of Awards': grant.findtext('ns:ExpectedNumberOfAwards', default='', namespaces=namespace),
        'Description': grant.findtext('ns:Description', default='', namespaces=namespace),
        'Cost Sharing or Matching Requirement': grant.findtext('ns:CostSharingOrMatchingRequirement', default='', namespaces=namespace),
        'Archive Date': grant.findtext('ns:ArchiveDate', default='', namespaces=namespace),
        'Grantor Contact Email': grant.findtext('ns:GrantorContactEmail', default='', namespaces=namespace),
        'Grantor Contact Email Description': grant.findtext('ns:GrantorContactEmailDescription', default='', namespaces=namespace),
        'Grantor Contact Text': grant.findtext('ns:GrantorContactText', default='', namespaces=namespace),
        'Version': grant.findtext('ns:Version', default='', namespaces=namespace)
    }
    data.append(grant_info)

# Convert the list of dictionaries to a pandas DataFrame
df = pd.DataFrame(data)

# Convert 'Post Date' to datetime format
df['Post Date'] = pd.to_datetime(df['Post Date'], format='%m%d%Y', errors='coerce')

# Define filter functions
def filter_by_opportunity_category(df, categories):
    return df[df['Opportunity Category'].isin(categories)]

def filter_by_funding_instrument_type(df, types):
    return df[df['Funding Instrument Type'].isin(types)]

def filter_by_category_of_funding_activity(df, activities):
    return df[df['Category of Funding Activity'].isin(activities)]

def filter_by_eligible_applicants(df, applicants):
    return df[df['Eligible Applicants'].isin(applicants)]

def filter_by_post_date(df, days):
    today = datetime.today()
    if days == 'today':
        return df[df['Post Date'].dt.date == today.date()]
    elif days == 'last_3_days':
        start_date = today - timedelta(days=3)
        return df[(df['Post Date'] >= start_date) & (df['Post Date'] <= today)]
    elif days == 'one_week':
        start_date = today - timedelta(weeks=1)
        return df[(df['Post Date'] >= start_date) & (df['Post Date'] <= today)]
    elif days == 'four_weeks':
        start_date = today - timedelta(weeks=4)
        return df[(df['Post Date'] >= start_date) & (df['Post Date'] <= today)]
    else:
        return df

# Apply filters
filtered_df = filter_by_opportunity_category(df, ['D', 'M', 'C', 'E', 'O'])
filtered_df = filter_by_funding_instrument_type(filtered_df, ['G', 'CA', 'O', 'PC'])
filtered_df = filter_by_category_of_funding_activity(filtered_df, ['ACA', 'AG', 'AR', 'BC', 'CD', 'CP', 'DPR', 'ED', 'ELT', 'EN', 'ENV', 'FN', 'HL', 'HO', 'HU', 'ISS', 'IS', 'LJL', 'NR', 'RA', 'RD', 'ST', 'T', 'O'])
filtered_df = filter_by_eligible_applicants(filtered_df, ['99', '00', '01', '02', '04', '05', '06', '07', '08', '11', '12', '13', '20', '21', '22', '23', '25'])

# Example: Filter by 'Post Date' for the last 3 days
filtered_df = filter_by_post_date(filtered_df, 'last_3_days')

# Display the filtered DataFrame
print(filtered_df.head())

# Optionally, save the filtered DataFrame to a CSV file
filtered_df.to_csv(f'{fileStem}{dateString}v2.csv', index=False)

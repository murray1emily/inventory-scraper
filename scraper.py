import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Set up Google Drive API client
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = 'creds.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('drive', 'v3', credentials=credentials)

# Step 1: Check for the latest file in Google Drive, excluding today's file

def get_latest_inventory_file(service, folder_id, exclude_filename):
    query = f"'{folder_id}' in parents and name contains 'current_inventory_' and mimeType = 'text/csv'"
    results = service.files().list(q=query, orderBy="createdTime desc", pageSize=10, fields="files(id, name)").execute()
    items = results.get('files', [])
    
    latest_file = None

    for item in items:
        # Exclude today's file
        if item['name'] != exclude_filename:
            latest_file = item
            print(f"Latest valid file found: {item['name']} with ID: {item['id']}")
            print(f"Retrieved file {item['name']} for comparison")
            return latest_file

    print("No appropriate inventory files found in the Drive folder.")
    return None

def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    with open(file_name, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")

# Define the Google Drive folder ID
folder_id = '1XDaMo6auBQz4uieAiMrbfmr3Y-qte6GR'

# Get today's date in MM_DD_YYYY format
today_str = datetime.now().strftime('%m_%d_%Y')

# Generate the name of today's file
today_filename = f'current_inventory_{today_str}.csv'

# Check for the latest inventory file in Google Drive, excluding today's file
latest_file = get_latest_inventory_file(service, folder_id, today_filename)

if latest_file:
    local_filename = latest_file['name']
    # Extract the date from the latest file name (last 10 characters)
    last_run_date = local_filename[-14:-4].replace('_', '-')
    
    # Check if the file already exists locally
    if not os.path.exists(local_filename):
        print(f"File {local_filename} does not exist locally. Downloading...")
        download_file(service, latest_file['id'], local_filename)
        print(f"Downloaded {local_filename} from Google Drive.")
    else:
        print(f"File {local_filename} already exists locally.")

# Step 2: Web Scraping the Yacht Listings

# URL of the inventory page
url = 'https://yachts360.com/boats-for-sale/?page=1&pp=250&view=list'

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Referer': 'https://yachts360.com/',
    'Accept-Language': 'en-US,en;q=0.9',
}

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"Failed to retrieve data: {response.status_code}")
    exit()

soup = BeautifulSoup(response.content, 'html.parser')

# Find the data on the page
listings = soup.find_all('div', class_='bfl-info')  # Adjust the selector as needed

data = []
for listing in listings:
    yacht_name = listing.find('h2', class_='bfl-title').text.strip()
    price = listing.find('h3', class_='bfl-price').text.strip()
    location = listing.find('h3', class_='bfl-location').text.strip()
    listing_url = listing.find('h2', class_='bfl-title').find('a')['href']

    # Extract the listing ID from the URL
    listing_id = listing_url.split('/')[4]  # This assumes the ID is always in this position

    data.append({
        'Listing ID': listing_id,
        'URL': listing_url,
        'Yacht Name': yacht_name,
        'Price': price,
        'Location': location,
    })

# Convert the list of dictionaries into a DataFrame
current_df = pd.DataFrame(data)

# Create the filename using the current date
filename = f'current_inventory_{today_str}.csv'

# Save the DataFrame to a CSV file
current_df.to_csv(filename, index=False)

print(f"Data saved to {filename}")

# Step 3: Identify and save added, removed, and changed listings
if latest_file:
    # Load the last file into a DataFrame
    last_df = pd.read_csv(local_filename)
    
    # Convert Listing IDs to strings and strip whitespace
    last_df['Listing ID'] = last_df['Listing ID'].astype(str).str.strip()
    current_df['Listing ID'] = current_df['Listing ID'].astype(str).str.strip()

    # Identify added listings
    added_listings_df = current_df[~current_df['Listing ID'].isin(last_df['Listing ID'])]
    added_file = f'{today_str}_listings_added.csv'
    added_listings_df.to_csv(added_file, index=False)
    
    # Identify removed listings
    removed_listings_df = last_df[~last_df['Listing ID'].isin(current_df['Listing ID'])]
    removed_file = f'{today_str}_listings_removed.csv'
    removed_listings_df.to_csv(removed_file, index=False)

    # Identify changed listings
    merged_df = pd.merge(last_df, current_df, on='Listing ID', suffixes=('_prev', '_new'))
    changed_listings_df = merged_df[
        (merged_df['Yacht Name_prev'] != merged_df['Yacht Name_new']) |
        (merged_df['Price_prev'] != merged_df['Price_new']) |
        (merged_df['Location_prev'] != merged_df['Location_new'])
    ]
    changed_file = f'{today_str}_listings_changed.csv'
    changed_listings_df.to_csv(changed_file, index=False)

    print(f"Listings added saved to '{added_file}'.")
    print(f"Listings removed saved to '{removed_file}'.")
    print(f"Listings with changes saved to '{changed_file}'.")

# Step 4: Generate the HTML message

# Generate filenames with the current date
html_file = f"{today_str}_listing_changes_message.html"

# Check if the files exist and have any rows
added_exists = os.path.exists(added_file) and pd.read_csv(added_file).shape[0] > 0
removed_exists = os.path.exists(removed_file) and pd.read_csv(removed_file).shape[0] > 0
changed_exists = os.path.exists(changed_file) and pd.read_csv(changed_file).shape[0] > 0

# Generate HTML content
html_content = f"<p><strong>Listings changes since {last_run_date}:</strong></p>"

# Add listings added section
if added_exists:
    html_content += "<p><strong>Listings added:</strong></p><ul>"
    
    added_df = pd.read_csv(added_file)
    for _, row in added_df.iterrows():
        html_content += f'<li><a href="{row["URL"]}">{row["Yacht Name"]}</a> (ID #{row["Listing ID"]})</li>'
    
    html_content += "</ul>"
else:
    html_content += "<p><strong>No new listings added.</strong></p>"

# Add listings removed section
if removed_exists:
    html_content += "<p><strong>Listings removed:</strong></p><ul>"
    
    removed_df = pd.read_csv(removed_file)
    for _, row in removed_df.iterrows():
        html_content += f'<li><a href="{row["URL"]}">{row["Yacht Name"]}</a> (ID #{row["Listing ID"]})</li>'
    
    html_content += "</ul>"
else:
    html_content += "<p><strong>No listings removed.</strong></p>"

# Add listings changed section
if changed_exists:
    html_content += "<p><strong>Listings changed:</strong></p><ul>"
    
    changed_df = pd.read_csv(changed_file)
    for _, row in changed_df.iterrows():
        # Only add the listing if there are actual changes and skip unchanged values
        changes = []
        if row['Yacht Name_prev'] != row['Yacht Name_new'] and pd.notna(row['Yacht Name_new']):
            changes.append(f"<li><strong>Update name:</strong> {row['Yacht Name_new']}</li>")
        if row['Price_prev'] != row['Price_new'] and pd.notna(row['Price_new']):
            changes.append(f"<li><strong>Update price:</strong> {row['Price_new']}</li>")
        if row['Location_prev'] != row['Location_new'] and pd.notna(row['Location_new']):
            changes.append(f"<li><strong>Update location:</strong> {row['Location_new']}</li>")
        
        if changes:
            html_content += f'<li><a href="{row["URL_new"]}">{row["Yacht Name_prev"]}</a> (ID #{row["Listing ID"]})<ul>'
            html_content += ''.join(changes)
            html_content += "</ul></li>"
    
    html_content += "</ul>"
else:
    html_content += "<p><strong>No listings changed.</strong></p>"

# Save the HTML to a file
with open(html_file, "w") as f:
    f.write(html_content)

print(f"HTML message saved to {html_file}.")

# Step 5: Upload files to Google Drive, overwriting if they exist

def upload_or_replace_file(service, file_name, folder_id, mime_type='text/csv'):
    # Check if a file with the same name already exists in the folder
    query = f"'{folder_id}' in parents and name = '{file_name}'"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = response.get('files', [])
    
    if items:
        # File with the same name exists, so we delete it
        file_id = items[0]['id']
        service.files().delete(fileId=file_id).execute()
        print(f"Deleted existing file: {file_name} (ID: {file_id})")
    
    # Upload the new file
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_name, mimetype=mime_type)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Uploaded file: {file_name} (ID: {file.get('id')})")

# Upload CSV files and the HTML message to Google Drive if they exist
if filename:
    upload_or_replace_file(service, filename, folder_id)
if added_exists:
    upload_or_replace_file(service, added_file, folder_id)
if removed_exists:
    upload_or_replace_file(service, removed_file, folder_id)
if changed_exists:
    upload_or_replace_file(service, changed_file, folder_id)
upload_or_replace_file(service, html_file, folder_id, mime_type='text/html')

print("All files uploaded successfully.")



# Step 6: Send email.

def send_email(html_content, recipient_email):
    # Your email credentials
    sender_email = "info@brandfueled.com"  # Replace with your email
    sender_password = "mtjj toft ptte cslv"  # Replace with your email password

    # Set up the email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Yacht Listings Changes"
    msg["From"] = sender_email
    msg["To"] = recipient_email

    # Attach the HTML content
    msg.attach(MIMEText(html_content, "html"))

    # Set up the SMTP server connection
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:  # Adjust for your SMTP server
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
            print(f"Email sent successfully to {recipient_email}.")
    except Exception as e:
        print(f"Error sending email: {e}")

# Load the HTML content from the file
html_file = f"{today_str}_listing_changes_message.html"
with open(html_file, "r") as f:
    html_content = f.read()

# Send the email to Kyle
send_email(html_content, "marketing@yachts360.com")

# Step 7: Clean up local files
def cleanup_files(files):
    for file in files:
        try:
            os.remove(file)
            print(f"Deleted file: {file}")
        except OSError as e:
            print(f"Error deleting file {file}: {e}")

# Delete all local files
attachments = [filename, local_filename, added_file, removed_file, changed_file, html_file]

cleanup_files(attachments)

print("All local files deleted.")

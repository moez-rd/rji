import json
import os
import csv
import requests
from bs4 import BeautifulSoup
import re

def sanitize_filename(filename):
    # Replace any invalid characters with an underscore
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def download_pdf_from_relation(url, download_folder, file_name):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the PDF download link within the HTML content
            download_link = soup.find('a', {'class': 'download'})  # Adjust this to match the actual HTML structure
            
            if download_link and 'href' in download_link.attrs: # type: ignore
                pdf_url = download_link['href'] # type: ignore
                if not pdf_url.startswith('http'): # type: ignore
                    pdf_url = os.path.join(os.path.dirname(url), pdf_url)  # type: ignore # Handle relative links
                
                pdf_response = requests.get(url.replace("view", "download")) # type: ignore
                if pdf_response.status_code == 200:
                    sanitized_file_name = sanitize_filename(file_name)
                    pdf_path = os.path.join(download_folder, sanitized_file_name)
                    with open(pdf_path, 'wb') as pdf_file:
                        pdf_file.write(pdf_response.content)
                    return True
                else:
                    print(f"Failed to download PDF from {pdf_url}")
            else:
                print(f"No PDF link found in {url}")
        else:
            print(f"Failed to access {url}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# Folder containing JSON files
folder_path = 'journals_v4'  # Change this to your folder path
output_folder = 'pdf'  # Folder to save downloaded PDFs

# Collect all JSON files in the folder
json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]

# Prepare CSV to log articles without a download link
csv_file_path = 'missing_relations_pdf.csv'
csv_header = ['jid', 'status']

with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(csv_header)

    # Process each JSON file in the folder
    for json_file in json_files:
        with open(os.path.join(folder_path, json_file), 'r') as file:
            try:
                journal_data = json.load(file)
                jid = journal_data['jid']

                for article in journal_data['articles']:
                    title = article['title'] if isinstance(article['title'], str) else 'No Title'
                    if 'relation' in article:
                        relation_url = article['relation']
                        pdf_name = f"{title.replace(' ', '_')}.pdf"
                        if download_pdf_from_relation(relation_url, output_folder, pdf_name):
                            print(f"Downloaded {relation_url} successfully.")
                        else:
                            csv_writer.writerow([jid, 'Download failed'])
                    else:
                        csv_writer.writerow([jid, 'No relation link'])

            except json.JSONDecodeError:
                print(f"Error reading JSON file: {json_file}")

print("Process completed. Check 'missing_relations.csv' for articles without downloadable PDF links.")

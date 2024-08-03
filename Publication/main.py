import os

from src.check_oai_links import run as check_oai_links
from src.scrape_xml import run as scrape_xml
from src.extract_journals import run as extract_journals
from src.check_relation_links import run as check_relation_links
from src.download_pdf import run as download_pdf

def main():
    if not os.path.exists("data"): 
        os.makedirs("data")

    check_oai_links()
    scrape_xml()
    extract_journals()
    check_relation_links()
    download_pdf()

if __name__ == "__main__":
    main()
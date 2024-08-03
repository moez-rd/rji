import os

from src.check_oai_links import run as check_oai_links
from src.scrape_xml import run as scrape_xml

def main():
    if not os.path.exists("data"): 
        os.makedirs("data")

    check_oai_links()
    scrape_xml()

if __name__ == "__main__":
    main()
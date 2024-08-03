from termcolor import colored
import json
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import datetime
import os
from definitions import ROOT_DIR
import pandas as pd

config = {
    "max_workers": 100,
    # Checked OAI links dataset
    "input_directory": f"{ROOT_DIR}/data/xml/period_1_2024-08-03 142502",
    "input_dataset": f"{ROOT_DIR}/data/all_garuda_journalweb_scrapped.xlsx",
    "output_directory": f"{ROOT_DIR}/data/json/period_1_2024-08-03 142502",
}

# =================================================================
def extract_metadata(article_xml) -> dict:
    metadata = dict()

    for tag in article_xml.children:
        if tag.name:
            key = tag.name
            value = tag.text

            if key in metadata:
                if type(metadata[key]) == list:
                    metadata[key].append(value)
                else:
                    temp = metadata[key]
                    metadata[key] = [temp, value]
            else:
                metadata[key] = value

    return metadata


# =================================================================
def extract_articles(soup) -> tuple[list, int, int]:
    record_xml = None
    index = 0
    article_deleted_count = 0
    article_count = 0
    articles = []

    while True:
        # =================================================================
        if record_xml is None:
            record_xml = soup.find("ns0:record")
        else:
            record_xml = record_xml.find_next("ns0:record")
            if record_xml is None:
                break

        # =================================================================
        article = dict()

        article_id = record_xml.find("ns0:identifier").text
        article["article_id"] = article_id

        article_xml = record_xml.find("ns2:dc")

        if article_xml is not None:
            article["deleted"] = False
            article["metadata"] = extract_metadata(article_xml)
            article_count += 1
        else:
            article["deleted"] = True
            article_deleted_count += 1


        articles.append(article)
            
        # =================================================================
        index += 1

    return articles, article_count, article_deleted_count


# =================================================================
def print_results(results) -> None:
    print(
        f"{colored(f" {results["timestamp"]} ", "white", "on_dark_grey", attrs=["bold"])}\t"
        f"{colored("journal", "blue")} {results["index"] + 1}/{results["df_len"]}\t"
        f"{colored("jid:", "blue")} {results["jid"]}\t"
        f"{colored("total articles:", "blue")} {results["article_size"]}\t"
        f"{colored("found", "green")}: {results["fetched"]}\t"
        f"{colored("deleted", "red")}: {results["deleted"]}\t"
    )


# =================================================================
def extract_journal(xml_directories, jid, eissn, df_len, index) -> None:
    total_article_size = 0
    total_article_count = 0
    total_article_deleted_count = 0
    all_articles = []

    for xml_file in os.listdir(xml_directories):
        file = open(os.path.join(xml_directories, xml_file), "r")
        content = file.read()

        # =================================================================
        soup = BeautifulSoup(content, features='xml')

        article_size = len(soup.find_all('record'))
        total_article_size += article_size

        # =================================================================
        articles, article_count, article_deleted_count = extract_articles(soup)
        total_article_count += article_count
        total_article_deleted_count += article_deleted_count
        all_articles += articles

    # =================================================================
    now = datetime.datetime.now()
    timestamp = now.strftime("%d-%m-%Y %H%M%S")

    # =================================================================
    journal = {
        "timestamp": timestamp,
        "jid": jid,
        "eissn": eissn,
        "article_count": total_article_count,
        "article_deleted_count": total_article_deleted_count,
        "articles": articles
    }

    with open(f"{config["output_directory"]}/{timestamp}_{jid}.json", 'w') as f:
        json.dump(journal, f)

    # =================================================================
    print_results({
        "timestamp": timestamp,
        "index": index,
        "df_len": df_len,
        "jid": jid,
        "article_size": total_article_size,
        "fetched": total_article_count,
        "deleted": total_article_deleted_count
    })


def run() -> None:
    print(colored(" RJI PUBLICATION: EXTRACT_JOURNALS ", "black", "on_dark_grey", attrs=["bold"]))

    # =================================================================
    # Create new result directory if not exist
    if not os.path.exists(config["output_directory"]): 
        os.makedirs(config["output_directory"])

    print(colored(" ==== IMPORTING DATASET ", "black", "on_green", attrs=["bold"]))
    df = pd.read_excel(config["input_dataset"])
    df.replace("-", None, inplace=True)

    # =================================================================
    # Start scraping with concurrent execution
    print(colored(" ==== START EXTRACTING ", "black", "on_green", attrs=["bold"]))

    xml_folders = [xml_folder for xml_folder in os.listdir(config["input_directory"])]

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []

        for i in range(len(xml_folders)):
            futures.append(executor.submit(
                extract_journal,
                os.path.join(config["input_directory"], xml_folders[i]),
                xml_folders[i],
                df[df["jid"] == int(xml_folders[i])]["eissn"].values[0],
                len(xml_folders),
                i
            ))
            
    print(colored(" ==== END EXTRACTING ", "black", "on_green", attrs=["bold"]))
import pandas as pd
from termcolor import colored
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import datetime
import os
import xml.etree.ElementTree as ET
from definitions import ROOT_DIR
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

config = {
    "max_workers": 200,
    # Checked OAI links dataset
    "input_dataset": f"{ROOT_DIR}/data/checked_oai_links.csv",
    "output_directory": f"{ROOT_DIR}/data/xml",
    "output_dataset": f"{ROOT_DIR}/data/scraped_xml_stasues.csv"
}

# =================================================================
def print_results(results, index) -> None:
    print(
        f"{colored(f" {results["timestamp"]} ", "black", "on_yellow", attrs=["bold"])} "
        f"{colored(f" {index + 1}/{results["df_len"]} ", "black", "on_dark_grey", attrs=["bold"])} "
        f"{colored("journal", "blue")} {results["index"] + 1}/{results["df_len"]}, \t"
        f"{colored("jid:", "blue")} {results["jid"]}, \t"
        f"{colored("total articles:", "blue")} {results["article_size"]}, \t"
        f"{colored("time taken", "blue")}: {round(results["time_delta"], 6)}s \t"
        f"{colored("average", "blue")}: {results["average"]}s"
    )

def save_xml(xml, xml_directory, page) -> None:
    tree = ET.ElementTree(ET.fromstring(xml))
    tree.write(f"{xml_directory}/page{page}.xml")

def scrape_xml(index, output_directory, oai, jid, df_len, times) -> None:
    now = datetime.datetime.now()
    timestamp = now.strftime("%d-%m-%Y %H%M%S")

    xml_directory = f"{output_directory}/{jid}"

    if not os.path.exists(xml_directory):
        os.makedirs(xml_directory)

    page = 1
    errors = 0
    total_article_size = 0

    initial_url = f"{oai}?verb=ListRecords&metadataPrefix=oai_dc"
    url = initial_url
    soup = None

    while True:

        session = requests.Session()

        retry_strategy = Retry(
            total=10,
            backoff_factor=1
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, features='xml')
            save_xml(str(soup), xml_directory, page)
            article_size = len(soup.find_all('record'))
            total_article_size += article_size

            if soup is not None:
                token_xml = soup.find("resumptionToken")

                if token_xml is None or not token_xml.text:
                    break
                else:
                    url =f"{oai}?verb=ListRecords&resumptionToken={token_xml.text}"
                    page += 1
                    continue
            else:
                break
        except Exception as e:
            print(colored(f" {timestamp}  Scraping Error: {e.__class__.__name__},\tjid: {jid},\tpage: {page},\t{e}", "red"))
            errors += 1
            break
            


    time_delta = datetime.datetime.now() - now
    time_delta = time_delta.seconds + (time_delta.microseconds / 1000000)
    
    times.append(time_delta)
    average = round(sum(times) / len(times), 6)

    print_data = {
        "time_delta": time_delta,
        "timestamp": timestamp,
        "index": index,
        "df_len": df_len,
        "jid": jid,
        "article_size": total_article_size,
        "average": average,
    }

    return index, page, errors, print_data


def run() -> None:
    
    if not os.path.exists(config["output_directory"]): 
        os.makedirs(config["output_directory"])

    now = datetime.datetime.now()
    xml_period_folders = [xml_period_folder for xml_period_folder in os.listdir(config["output_directory"])]
    output_directory = f"{config["output_directory"]}/period_{len(xml_period_folders)+1}_{now.strftime("%Y-%m-%d %H%M%S")}"
    os.makedirs(output_directory)

    times = []

    print(colored(" ==== IMPORTING DATASET ", "black", "on_green", attrs=["bold"]))
    df = pd.read_csv(config["input_dataset"], dtype={"status_code": str})
    df = df[df["error"].isnull()]
    df_xml_200 = df[(df["status_code"] == "200") & (df["content_type"].str.contains("text/xml"))]

    print(colored(" ==== START SCRAPING ", "black", "on_green", attrs=["bold"]))
    now = datetime.datetime.now()

    result_df = pd.DataFrame(columns=["jid", "eissn", "pages", "errors"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        futures = []

        for i in range(len(df_xml_200)):
            futures.append(executor.submit(
                scrape_xml,
                i,
                output_directory,
                df_xml_200["oai"].to_list()[i],
                df_xml_200["jid"].to_list()[i],
                len(df_xml_200),
                times
            ))

        index = 0 
            
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                
                result_df.loc[len(result_df)] = [
                    df_xml_200["jid"].to_list()[result[0]],
                    df_xml_200["eissn"].to_list()[result[0]],
                    result[1],
                    result[2],
                ]

            except Exception as e:
                print(colored(f"Thread Error: {e.__class__.__name__}, {e}", "red"))

            print_results(result[3], index)

            index += 1

    result_df.to_csv(config["output_dataset"], index=False)

    time_delta = datetime.datetime.now() - now
    time_delta = time_delta.seconds + (time_delta.microseconds / 1000000)
    
    average = round(sum(times) / len(times), 6)

    print(colored(" ==== END SCRAPING ", "black", "on_green", attrs=["bold"]), f"Toke {time_delta}s", f"average {average}")
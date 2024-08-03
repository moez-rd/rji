import os
import json
import pandas as pd
from termcolor import colored
from definitions import ROOT_DIR
from concurrent.futures import ThreadPoolExecutor, as_completed



config = {
    "input_directory": f"{ROOT_DIR}/data/json/period_1_2024-08-03 142502",
    "output_dataset": f"{ROOT_DIR}/data/checked_relation_links.csv"
}

def check_relation_link(article, index):
    result = None

    metadata = article["article"]["metadata"]

    found_print = False
    relation_print = None

    if "relation" not in metadata:
        result = {
            "jid": article["jid"],
            "eissn": article["eissn"],
            "article_id": article["article"]["article_id"],
            "relation": None,
            "has_view": False,
            "pdf_download_url": None
        }
    else:
        if type(metadata["relation"]) == str:
            result = {
                "jid": article["jid"],
                "eissn": article["eissn"],
                "article_id": article["article"]["article_id"],
                "relation": metadata["relation"],
                "has_view": "/view/" in metadata["relation"],
                "pdf_download_url": str(metadata["relation"]).replace("/view/", "/download/") if "/view/" in metadata["relation"] else None
            }

            found_print = "/view/" in metadata["relation"]
            relation_print = metadata["relation"]
        else:
            relation = [item for item in metadata["relation"] if '/view/' in item]

            if len(relation) == 0:
                result = {
                    "jid": article["jid"],
                    "eissn": article["eissn"],
                    "article_id": article["article"]["article_id"],
                    "relation": None,
                    "has_view": False,
                    "pdf_download_url": None
                }
            else:
                result = {
                    "jid": article["jid"],
                    "eissn": article["eissn"],
                    "article_id": article["article"]["article_id"],
                    "relation": relation[0],
                    "has_view": True,
                    "pdf_download_url": str(relation[0]).replace("view", "download")
                }
                
                found_print = True
                relation_print = relation[0]
                
        index += 1

    print_data = {
        "found": found_print,
        "relation": relation_print
    }

    return result, print_data



def run() -> None:
    print(colored(" RJI PUBLICATION: CHECK RELATION LINKS ", "black", "on_dark_grey", attrs=["bold"]))

    json_files = [json_file for json_file in os.listdir(config["input_directory"]) if json_file.endswith('.json')]

    df = pd.DataFrame(columns=["jid", "eissn", "article_id", "relation", "has_view", "pdf_download_url"])

    articles = []

    for json_file in json_files:
        with open(os.path.join(config["input_directory"], json_file), 'r') as file:
            try:
                journal = json.load(file)

                for article in journal['articles']:
                    if article["deleted"]:
                        continue

                    articles.append({
                        "jid": journal["jid"],
                        "eissn": journal["eissn"],
                        "article": article
                    })

            except Exception as e:
                print(colored(f"Error: {e.__class__.__name__}, {e}", "red"))


    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []

        for i in range(len(articles)):
            futures.append(executor.submit(
                check_relation_link,
                articles[i],
                i
            ))

        index = 0

        for future in as_completed(futures):
            try:
                result = future.result()

                if result[0] is not None:
                    df.loc[len(df)] = result[0]

                print(colored("FOUND", "green") if result[1]["found"] else colored("MISSING", "red"), f"{index+1}/{len(articles)}", result[1]["relation"])
            except Exception as e:
                print(colored(f"Thread Error: {e.__class__.__name__}, {e}", "red"))

            index += 1


    df.to_csv(config["output_dataset"], index=False)

            
    print(colored(" ==== END EXTRACTING ", "black", "on_green", attrs=["bold"]))
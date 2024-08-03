import os
import requests
from definitions import ROOT_DIR
from termcolor import colored
import pandas as pd
import concurrent.futures
import datetime


config = {
    "max_workers": 100,
    "input_dataset": f"{ROOT_DIR}/data/checked_relation_links.csv",
    "output_folder": f"{ROOT_DIR}/data/pdf",
    "output_dataset": f"{ROOT_DIR}/data/downloaded_pdf_stasues.csv"
}

def print_results(results, index) -> None:
    print(
        f"{colored(f" {results["timestamp"]} ", "black", "on_yellow", attrs=["bold"])} "
        f"{colored(f" {index + 1}/{results["df_len"]} ", "black", "on_dark_grey", attrs=["bold"])} "
        f"{colored("jid:", "blue")} {results["jid"]}, \t"
        f"{colored("article_id:", "blue")} {results["article_id"]}, \t"
        f"{colored("time taken", "blue")}: {round(results["time_delta"], 6)}s \t"
        f"{colored("average", "blue")}: {results["average"]}s"
    )

def download_pdf(url, jid, eissn, article_id, df_len, times):
    now = datetime.datetime.now()
    timestamp = now.strftime("%d-%m-%Y %H%M%S")
    status = None

    try:
        response = requests.get(url)

        if response.status_code == 200:
            output_path = os.path.join(config["output_folder"], f"{jid}_{str(article_id).split("/")[-1]}.pdf")
            with open(output_path, 'wb') as file:
                file.write(response.content)

            status = "Downloaded"
        else:
            print(colored(f"Failed download, staus code: {response.status_code}", "red"))
            status = f"Failed: {response.status_code}"
    except Exception as e:
        print(colored(f"Error: {e.__class__.__name__}, {e}", "red"))
        status = f"Error: {e.__class__.__name__}"

    time_delta = datetime.datetime.now() - now
    time_delta = time_delta.seconds + (time_delta.microseconds / 1000000)
    
    times.append(time_delta)
    average = round(sum(times) / len(times), 6)

    print_data = {
        "time_delta": time_delta,
        "timestamp": timestamp,
        "df_len": df_len,
        "jid": jid,
        "article_id": str(article_id).split("/")[-1],
        "average": average,
    }

    return print_data, jid, eissn, article_id, status

def run():
    print(colored(" RJI PUBLICATION: DOWNLOAD PDF ", "black", "on_dark_grey", attrs=["bold"]))

    if not os.path.exists(config["output_folder"]):
        os.makedirs(config["output_folder"])

    df = pd.read_csv(config["input_dataset"])

    result_df = pd.DataFrame(columns=["jid", "eissn", "article_id", "status"])

    times = []


    with concurrent.futures.ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        futures = []

        for i in range(len(df)):
            if df["has_view"]:
                futures.append(executor.submit(
                    download_pdf,
                    df["pdf_download_url"].to_list()[i],
                    df["jid"].to_list()[i],
                    df["eissn"].to_list()[i],
                    df["article_id"].to_list()[i],
                    len(df),
                    times
                ))

        index = 0 
            
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                
                result_df.loc[len(result_df)] = [
                    result[1],
                    result[2],
                    result[3],
                    result[4],
                ]

                print_results(result[0], index)

            except Exception as e:
                print(colored(f"Thread Error: {e.__class__.__name__}, {e}", "red"))

            index += 1

    result_df.to_csv(config["output_dataset"], index=False)

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from termcolor import colored
from definitions import ROOT_DIR

config = {
    "max_workers": 100,
    "input_dataset": f"{ROOT_DIR}/data/all_garuda_journalweb_scrapped.xlsx",
    "output_dataset": f"{ROOT_DIR}/data/checked_oai_links.csv",
}


def check_oai_link(index, oai)-> tuple[int, int, str, str]:
    """
    This function sends a GET request to the provided OAI link with specific parameters.
    It captures the status code, content type, and any exceptions that occur during the request.

    Parameters:
    index (int): The index of the current row in the DataFrame.
    oai (str): The OAI link to be checked.

    Returns:
    tuple[int, int, str, str]: A tuple containing the index, status code, content type, and error message.
    If no exception occurs, the error message will be None.
    """
    status_code = None
    content_type = None
    error = None
    
    try:
        response = requests.get(f"{oai}?verb=ListRecords&metadataPrefix=oai_dc", timeout=240)
        status_code = str(response.status_code)
        content_type = response.headers['content-type']
    except Exception as e:
        error = e.__class__.__name__

    return index, status_code, content_type, error


def run()-> None:
    print("IMPORTING DATASET")
    df = pd.read_excel(config["input_dataset"])
    df.replace("-", None, inplace=True)

    print("START CHECKING")
    index = 0
    exceptions = []

    result_df = pd.DataFrame(columns=["jid", "eissn", "oai", "status_code", "content_type", "error"])

    with ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        futures = []
        
        for i in range(len(df)):
            futures.append(executor.submit(check_oai_link, i, df["oai"].to_list()[i]))

        for future in as_completed(futures):
            try:
                result = future.result()
                
                result_df.loc[len(result_df)] = [
                    df["jid"].to_list()[result[0]],
                    df["eissn"].to_list()[result[0]],
                    df["oai"].to_list()[result[0]],
                    result[1],
                    result[2],
                    result[3]
                ]

                str_print = f"{index}/{len(df)}\t" \
                    f"{result[0]+1}/{len(df)}\t" \
                    f"{str(df["jid"].to_list()[result[0]]).ljust(12)}\t" \
                    f"{result[1]}\t" \
                    f"{str(result[2]).ljust(35)}\t" \
                    f"{result[3]}\t"

                if result[1] == '200':
                    print(str_print)
                else:
                    print(colored(str_print, "yellow"))

            except Exception as e:
                print(colored(f"Exeption: {e.__class__.__name__}", "red"))
                exceptions.append(e.__class__.__name__)

            index += 1

    result_df.to_csv(config["output_dataset"], index=False)
    
    print(f"FINISHED CHECKING, saved to {config["output_dataset"]}")
    print(f"Exceptions, {exceptions}")
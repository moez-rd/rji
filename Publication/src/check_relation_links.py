import os
import json
import pandas as pd
from termcolor import colored


folder_path = 'journals_v4'  # Ubah path ini ke lokasi folder Anda

# Mengumpulkan semua file JSON dalam folder
json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]


df = pd.DataFrame(columns=["jid", "eissn", "relation", "has_view", "pdf_download_url"])

index = 0

for json_file in json_files:
    with open(os.path.join(folder_path, json_file), 'r') as file:
        try:
            journal_data = json.load(file)

            for article in journal_data['articles']:
                if "relation" not in article:
                    df.loc[len(df)] = {
                        "jid": journal_data["jid"],
                        "eissn": journal_data["eissn"],
                        "relation": None,
                        "has_view": False,
                        "pdf_download_url": None
                    }
                    
                    print(colored("MISSING", "red"), f"{index}/2091456")

                else:
                    if type(article["relation"]) == str:
                        df.loc[len(df)] = {
                            "jid": journal_data["jid"],
                            "eissn": journal_data["eissn"],
                            "relation": article["relation"],
                            "has_view": "view" in article["relation"],
                            "pdf_download_url": str(article["relation"]).replace("view", "download") if "view" in article["relation"] else None
                        }

                        print(colored("FOUND", "green") if "view" in article["relation"] else colored("MISSING", "red"), f"{index}/2091456", article["relation"])
                    else:
                        relation = [item for item in article["relation"] if 'view' in item]

                        if len(relation) == 0:
                            df.loc[len(df)] = {
                                "jid": journal_data["jid"],
                                "eissn": journal_data["eissn"],
                                "relation": None,
                                "has_view": False,
                                "pdf_download_url": None
                            }

                            print(colored("MISSING", "red"), f"{index}/2091456")

                        else:
                            df.loc[len(df)] = {
                                "jid": journal_data["jid"],
                                "eissn": journal_data["eissn"],
                                "relation": relation[0],
                                "has_view": True,
                                "pdf_download_url": str(relation[0]).replace("view", "download")
                            }
                            
                            print(colored("FOUND", "green"), f"{index}/2091456", relation[0])
                            
                    index += 1




        except json.JSONDecodeError:
            print(f"Error dalam membaca file JSON: {json_file}")

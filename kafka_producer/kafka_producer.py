import requests
import json
import time
from pyspark.sql import SparkSession

# Configuration Loki
LOKI_URL = "http://loki:3100/loki/api/v1/push"

def send_logs_to_loki(logs_df):
    """Envoyer les journaux vers Loki."""
    logs = logs_df.collect()
    for log in logs:
        log_dict = log.asDict()
        loki_payload = {
            "streams": [
                {
                    "stream": {
                        "dataset": log_dict.get("Dataset", "unknown"),
                        "level": log_dict.get("Score", "unknown")
                    },
                    "values": [[
                        str(int(time.time() * 1e9)),  # Horodatage en nanosecondes
                        json.dumps(log_dict)
                    ]]
                }
            ]
        }
        # Envoi de la requête POST vers Loki
        response = requests.post(LOKI_URL, json=loki_payload)
        if response.status_code == 204:
            print(f"Journal envoyé à Loki : {log_dict}")
        else:
            print(f"Échec de l’envoi du journal à Loki : {response.status_code}, {response.text}")

if __name__ == "__main__":
    # Lecture des journaux depuis un fichier Parquet
    spark = SparkSession.builder \
        .appName("LokiPusher") \
        .getOrCreate()
    
    logs_df = spark.read.parquet("parsed_logs.parquet")
    send_logs_to_loki(logs_df)

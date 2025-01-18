from pyspark.sql import SparkSession
import re

# Initializing Spark Session
spark = SparkSession.builder \
    .appName("LogParser") \
    .getOrCreate()

ANOMALY_KEYWORDS = ["fail", "error", "exception", "unexpected", "critical", 
                    "severe", "abort", "crash", "panic", "fault", "timeout", 
                    "denied", "refused", "unavailable", "down", "overflow", 
                    "deadlock", "corrupt", "lost", "interrupt", "halt", 
                    "violation", "malformed", "illegal", "broken", "disconnect"]

def is_anomaly(content):
    """Determine if a log entry contains anomaly keywords."""
    content_lower = content.lower()
    return "Anomaly" if any(keyword in content_lower for keyword in ANOMALY_KEYWORDS) else "Not anomaly"

def parse_logs_with_spark(datasets):
    """Parse logs from multiple datasets using Spark."""
    all_logs = []
    for dataset_name, (file_path, pattern, fields) in datasets.items():
        # Reading raw log file
        log_rdd = spark.sparkContext.textFile(file_path)

        # Parsing each log line using regex
        def parse_line(line):
            match = re.match(pattern, line)
            if match:
                groups = match.groups()
                content = groups[-1]  # Assume the last field is the content
                return dict(zip(fields, groups)) | {
                    "Dataset": dataset_name,
                    "Score": is_anomaly(content)
                }
            return None

        # Transforming and filter parsed logs
        parsed_rdd = log_rdd.map(parse_line).filter(lambda x: x is not None)

        # Converting parsed logs to DataFrame
        log_df = spark.createDataFrame(parsed_rdd)
        all_logs.append(log_df)

    # Combining DataFrames for all datasets
    combined_df = all_logs[0]
    for df in all_logs[1:]:
        combined_df = combined_df.union(df)

    return combined_df

if __name__ == "__main__":
    datasets = {
        "Android": ("datasets/Android/Android_2k.log", 
                    r"(\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2}\.\d{3})\s(\d+)\s(\d+)\s([A-Z])\s([\w.$]+):\s(.+)", 
                    ["Date", "Time", "Pid", "Tid", "Level", "Component", "Content"]),
        "Apache": ("datasets/Apache/Apache_2k.log", 
                   r"\[(.*?)\]\s\[(.*?)\]\s(.+)", 
                   ["Time", "Level", "Content"]),
        "BGL": ("datasets/BGL/BGL_2k.log", 
                r"-\s(\d+)\s([\d.]+)\s(\S+)\s(\S+)\s(\S+)\s(\S+)\s([\w]+)\s([\w]+)\s([A-Z]+)\s(.+)", 
                ["Label", "Timestamp", "Date", "Node", "Time", "NodeRepeat", "Type", "Component", "Level", "Content"]),
        "HealthApp": ("datasets/HealthApp/HealthApp_2k.log", 
                      r"(\d{8}-\d{2}:\d{2}:\d{2}:\d{3})\|(\w+)\|(\d+)\|(.+)", 
                      ["Time", "Component", "Pid", "Content"]),
        "HPC": ("datasets/HPC/HPC_2k.log", 
                r"(\d+)\s([\w-]+)\s([\w.]+)\s([\w.]+)\s(\d+)\s(\d+)\s(.+)", 
                ["LogId", "Node", "Component", "State", "Time", "Flag", "Content"]),
        "Linux": ("datasets/Linux/Linux_2k.log", 
                  r"([A-Za-z]{3})\s(\d{1,2})\s(\d{2}:\d{2}:\d{2})\s([\w.]+)\s([\w()]+)\[(\d+)\]:\s(.+)", 
                  ["Month", "Date", "Time", "Level", "Component", "PID", "Content"]),
        "Mac": ("datasets/Mac/Mac_2k.log", 
                r"([A-Za-z]{3})\s(\d{1,2})\s(\d{2}:\d{2}:\d{2})\s([\w.-]+)\s([\w]+)\[(\d+)\]:\s(.+)", 
                ["Month", "Date", "Time", "User", "Component", "PID", "Content"]),
        "OpenSSH": ("datasets/OpenSSH/OpenSSH_2k.log", 
                    r"([A-Za-z]{3})\s(\d{1,2})\s(\d{2}:\d{2}:\d{2})\s([\w.-]+)\[(\d+)\]:\s(.+)", 
                    ["Date", "Day", "Time", "Component", "Pid", "Content"]),
        "OpenStack": ("datasets/OpenStack/OpenStack_2k.log", 
                      r"([\w.-]+)\s(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2}\.\d{3})\s(\d+)\s([A-Z]+)\s([\w.]+)\s(.+)", 
                      ["Logrecord", "Date", "Time", "Pid", "Level", "Component", "Content"]),
        "Proxifier": ("datasets/Proxifier/Proxifier_2k.log", r"\[(.*?)\]\s([\w.-]+)\s(.+)", 
                      ["Time", "Program", "Content"]),
        "Spark": ("datasets/Spark/Spark_2k.log", r"(\d{2}/\d{2}/\d{2})\s(\d{2}:\d{2}:\d{2})\s([A-Z]+)\s([\w.]+):\s(.+)", 
                  ["Date", "Time", "Level", "Component", "Content"]),
        "Thunderbird": ("datasets/Thunderbird/Thunderbird_2k.log", r"-\s(\d+)\s([\d.]+)\s([\d.-]+)\s([A-Za-z]+)\s(\d+)\s(.+)", 
                        ["Label", "Timestamp", "Date", "User", "PID", "Content"]),
        "Windows": ("datasets/Windows/Windows_2k.log", r"(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2}),\s([A-Za-z]+)\s+([A-Za-z]+)\s+(.+)", 
                    ["Date", "Time", "Level", "Component", "Content"]),
        "Zookeeper": ("datasets/Zookeeper/Zookeeper_2k.log", 
                      r"(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2},\d{3})\s-\s([A-Z]+)\s+([\w./:@-]+)\s-\s(.+)", 
                      ["Date", "Time", "Level", "Node", "Content"])
    }

    # Parsing logs from all datasets
    combined_logs_df = parse_logs_with_spark(datasets)

    # Saving combined logs to a Parquet file for persistence
    combined_logs_df.write.mode("overwrite").parquet("parsed_logs.parquet")

    print("Logs have been parsed and saved to 'parsed_logs.parquet'.")

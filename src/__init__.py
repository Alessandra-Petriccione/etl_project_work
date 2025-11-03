import yaml
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[1]

with open(ROOT_DIR/"config/config.yml", "r") as file:
    conf = yaml.safe_load(file)
raw = conf["files"]["raw_path"]
wip = conf["files"]["wip_path"]
processed = conf["files"]["processed_path"]


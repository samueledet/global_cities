import logging
import json
import pickle


FORMAT = "%(asctime)s  - %(name)s - %(levelname)s | %(message)s"

logging.basicConfig(level=logging.DEBUG, format=FORMAT)


def load_json(path):
    with open(path) as f:
        data = json.load(f)
    return data


def dump_json(obj, path):
    with open(path, "w") as f:
        json.dump(obj, f)


def load_pickle(path):
    with open(path, "rb") as f:
        data = pickle.load(f)
    return data


def dump_pickle(obj, path):
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def create_pbar(instantiate):
    if instantiate:
        from tqdm import tqdm

        return tqdm
    else:

        def progress(x):
            return x

        return progress

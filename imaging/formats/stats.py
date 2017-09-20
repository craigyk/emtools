

import pyfs
import yaml


NAMES = ["yaml"]


def load(path):
    with pyfs.aopen(path, "rb") as src:
        return yaml.load(src)


def save(stats, path, **kwargs):
    with pyfs.aopen(path, "wb") as dst:
        yaml.dump(stats, dst, default_flow_style=False)

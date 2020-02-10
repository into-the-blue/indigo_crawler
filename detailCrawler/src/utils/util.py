from pathlib import Path


def get_root_pth():
    return (Path(__file__)/'..'/'..'/'..'/'..').resolve()


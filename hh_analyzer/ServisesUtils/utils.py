import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

CONFIG_PATH = str(Path(__file__).parent /'../HHAnalyzer_configure.json')
SPECIALIZATIONS_PATH = str(Path(__file__).parent / '../data/specializations_keywords.txt')


def load_config(path: str = CONFIG_PATH) -> Dict:
    data = {}
    with open(path, 'r') as f:
        data = json.load(f)
    return data


def parse_specializations() -> Tuple[set, set]:
    with open(SPECIALIZATIONS_PATH, 'r') as f:
        data = f.readlines()
    add_keyword = set()
    delete_keyword = set()
    for spec in data:
        if spec.startswith('+'):
            add_keyword.add(spec[2:-1].lower())
        elif spec.startswith('-'):
            delete_keyword.add(spec[2:-1].lower())
    return add_keyword, delete_keyword


add_keyword, delete_keyword = parse_specializations()


def matched_specializations(vac_specializations: List) -> bool:
    if len(vac_specializations) == 0 or (len(delete_keyword) + len(add_keyword)) == 0:
        return True

    def filtering(spec_word):
        matched = filter(lambda word: (re.match(word, spec_word) is not None), add_keyword)
        return list(filter(lambda m: (all(map(lambda word: re.match(word, m) is None, delete_keyword))), matched))

    return all([
        len(filtering(spec["name"].lower())) > 0 or len(filtering(spec["profarea_name"].lower())) > 0
        for spec in vac_specializations
    ])

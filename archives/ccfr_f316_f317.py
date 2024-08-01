from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import get_referentiels, Rbxbib2dict

referentiels = get_referentiels()

marc_file =  "/home/fpichenot/dev/ccfr/data/ccfr14/595126101-Roubaix-BM-Patrimoine_musical.mrc"
with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.ccfr_f316_f317()
        metadatas.append(bib2dict.metadatas)

    df = pd.DataFrame(metadatas)
    df.to_csv("results/ccfr14-595126101-Roubaix-BM-Patrimoine_musical-f316-f317.csv", index=False)

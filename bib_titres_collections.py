from os.path import join

import pandas as pd
from pymarc import MARCReader

from rbxmarc import Rbxbib2dict, Rbxmrc

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

# marc_file =  "sample_data/marc_sample_biblio_20240519_238.mrc"
marc_file = "data/2025-11-09-notices_total.mrc"

with open(marc_file, "rb") as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.titres_collections()
        metadatas.append(bib2dict.metadatas)
        i += 1
        if i % 10000 == 0:
            print(i)
        print(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
df.to_csv(
    join("extractions", "extract_titres_collections_20251109.csv.gz"), index=False
)

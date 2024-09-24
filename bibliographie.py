from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxbib2dict

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

marc_file =  "../../data/2024-05-12-notices_total.mrc"
with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.rbx_bibliographie()
        metadatas.append(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
df.to_csv("bibliographie_20240512.csv.gz", index=False)

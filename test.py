from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxbib2dict

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

marc_file =  "sample_data/marc_sample_biblio_20240519_238.mrc"
with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.extraction_complete()
        metadatas.append(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
print(df)
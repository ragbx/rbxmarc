from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxbib2dict
from os.path import join


rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

marc_file = join("data", "2025-03-09-notices_total.mrc")

with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.extraction_complete()
        metadatas.append(bib2dict.metadatas)
        i += 1
        if i % 10000 == 0:
            print(i)

df = pd.DataFrame(metadatas)
df.to_csv(join("extractions", "extract_complete_20250309.csv.gz"), index=False)
#print(df)

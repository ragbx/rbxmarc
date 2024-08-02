"""
Script qui permet d'extraire les champs importants pour la qualité de la notice.
Prend en argument la date d'export au format AAAA-MM-JJ.
"""

from os.path import join
import pandas as pd
from sys import argv

from pymarc import MARCReader
from rbxmarc import get_referentiels, Rbxbib2dict

marc_file =  "sample_data/marc_sample_biblio_20240519_238.mrc"

referentiels = get_referentiels()

with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        i += 1
        if i % 1000 == 0:
            print(i)
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.get_public()
        metadatas.append(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
print(df)
#df.to_csv(join("extractions", f"rbx_bib_qual_{date_export2}.csv.gz"), index=False)

from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxbib2dict

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

marc_file =  "extractions/umatic.mrc"
with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.rbx_bibliographie()
        metadatas.append(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
df.columns = ['biblionumber', 'support', 'titre',
       'lieu_publication', 'éditeur', 'date_publication',
       'description_matérielle', 'responsabilités', 'sujets', 'cote']
df['lien_koha'] = 'http://koha.ntrbx.local/cgi-bin/koha/catalogue/detail.pl?biblionumber=' + df['biblionumber'].astype(str)
df.to_excel("extractions/umatic_bibliographie_20240512_v2.xlsx", index=False)
print(df.columns)

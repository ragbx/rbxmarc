from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxbib2dict

date_export = "2024-07-28"
date_export2 = date_export.replace("-", "")
marc_file =  f"../../data/{date_export}-notices_total.mrc"

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        i += 1
        if i % 1000 == 0:
            print(i)
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.rbx_vdg()
        metadatas.append(bib2dict.metadatas)


df = pd.DataFrame(metadatas)
df = df[['bib_record_id', 'bib_alignement_bnf', 'bib_rbx_date_creation_notice',
       'bib_rbx_vdg_action', 'bib_rbx_vdg_date', 'bib_rbx_support', 'bib_agence_cat',
       'bib_pat']]

#df.to_csv(join("extractions", f"rbx_vdg_{date_export2}.csv.gz"), index=False)
print(df)

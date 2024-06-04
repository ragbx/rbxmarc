from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import get_referentiels, Rbxbib2dict

date_export = "2024-05-26"
date_export2 = date_export.replace("-", "")
marc_file =  f"../../data/{date_export}-notices_total.mrc"

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
        bib2dict.rbx_qual()
        metadatas.append(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
df.to_excel(join("results", f"rbx_qual_{date_export}.xlsx"), index=False)

from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxauth2dict

date_export = "2024-08-04"
date_export2 = date_export.replace("-", "")
marc_file =  f"../../data/{date_export}-auths_total.mrc"
#marc_file =  f"sample_data/marc_sample_auth_20240519_1585.mrc"

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        i += 1
        if i % 10000 == 0:
            print(i)
        auth2dict = Rbxauth2dict(record, referentiels=referentiels)
        auth2dict.extraction_complete()
        metadatas.append(auth2dict.metadatas)

df = pd.DataFrame(metadatas)
print(df)
df.to_csv(join("extractions", f"auth_qual_{date_export2}.csv.gz"), index=False)

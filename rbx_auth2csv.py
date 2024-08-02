from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import get_referentiels, Rbxauth2dict

referentiels = get_referentiels()

marc_file =  "sample_data/marc_sample_auth_20240519_158.mrc"
with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    for record in reader:
        auth2dict = Rbxauth2dict(record, referentiels=referentiels)
        auth2dict.analyse_complete()
        metadatas.append(auth2dict.metadatas)

df = pd.DataFrame(metadatas)
print(df)

from os.path import join

import pandas as pd
from pymarc import MARCReader

from rbxmarc import Rbxbib2dict, Rbxmrc

rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

# marc_file = "extractions/bnf/all_bnf_records.mrc"
marc_file = "data/2025-11-30-notices_total.mrc"

with open(marc_file, "rb") as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.rbx_vdg()
        metadatas.append(bib2dict.metadatas)
        i += 1
        if i % 10000 == 0:
            print(i)
        # print(bib2dict.metadatas)

df = pd.DataFrame(metadatas)
df.to_csv(join("extractions", "bib_vdg_20251130.csv.gz"), index=False)

df.groupby(
    [
        "bib_rbx_support",
        "bib_agence_cat",
        "bib_pat",
        "bib_alignement_bnf",
        "bib_rbx_vdg_action",
        "bib_rbx_vdg_resultat",
    ]
)["bib_record_id"].count().reset_index().to_csv(
    join("extractions", "bib_vdg_20251130_gr.csv"), index=False
)

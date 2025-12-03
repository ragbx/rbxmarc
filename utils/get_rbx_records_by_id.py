from os.path import join

import pandas as pd
from pymarc import MARCReader

date_export = "2025-11-30"
date_export2 = date_export.replace("-", "")

id_df = pd.read_csv("bbs/bbs_2025-11-30_id2export_v2.csv")
ids = id_df["bib_record_id"].astype(str).to_list()
n = len(ids)

marc_file = f"data/{date_export}-notices_total.mrc"
# marc_file =  f"../../../data/{date_export}-auths_total.mrc"

record_type = "bib"

with open(marc_file, "rb") as fh:
    records2export = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    j = 0
    for record in reader:
        i += 1
        bib_record_id = record["001"].value()
        if bib_record_id in ids:
            records2export.append(record)
            j += 1
        print(f"{n} : {j} / {i}")


export_file = join("bbs", f"bbs_2025-11-30_v2")
marc_file = export_file + ".mrc"

with open(marc_file, "wb") as out:
    for record in records2export:
        out.write(record.as_marc())

# command = f"yaz-marcdump {marc_file} > {txt_file}"
# subprocess.run(command, shell=True, executable="/bin/bash")

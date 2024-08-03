from os.path import join
import subprocess

from pymarc import MARCReader

date_export = "2024-05-19"
date_export2 = date_export.replace("-", "")

#marc_file =  f"../../../data/{date_export}-notices_total.mrc"
#marc_file =  f"../../../data/{date_export}-auths_total.mrc"
marc_file = "../../ccfr/data/ccfr24/595126101-Roubaix-BM-20240526-Patrimoine_musical.mrc"
record_type = "bib"

with open(marc_file, 'rb') as fh:
    records2export = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    j = 0
    for record in reader:
        i += 1
        if i % 23 == 0:
            j += 1
            #print(i)
            records2export.append(record)

export_file = join("sample_data", f"marc_sample_{record_type}_{date_export2}_{j}")
marc_file = export_file + ".mrc"
txt_file = export_file + ".txt"

print(f"nb notices : {len(records2export)}")
with open(marc_file, 'wb') as out:
    i = 0
    for record in records2export:
        i +=1
        if i <= 100:
            out.write(record.as_marc())

command = f"yaz-marcdump {marc_file} > {txt_file}"
subprocess.run(command, shell=True, executable="/bin/bash")

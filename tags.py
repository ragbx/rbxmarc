from os.path import join
import pandas as pd

from pymarc import MARCReader
from rbxmarc import Rbxmrc, Rbxbib2dict
from os.path import join


rbxmrc = Rbxmrc()
referentiels = rbxmrc.referentiels

#marc_file =  "sample_data/marc_sample_biblio_20240519_238.mrc"
marc_file = join("data", "2025-08-31-notices_total.mrc")

with open(marc_file, 'rb') as fh:
    metadatas = []
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    i = 0
    for record in reader:
        bib2dict = Rbxbib2dict(record, referentiels=referentiels)
        bib2dict.get_all_tags()
        #print(bib2dict.metadatas)
        metadatas.append(bib2dict.metadatas)

        i += 1
        if i % 10000 == 0:
            print(i)


df.to_csv(join("extractions", "stat_tags_20250831.csv.gz"), index=False)
#print(df)

results = []
for metadata in metadatas:
    tags = metadata['tags']
    tags = sorted(list(set(tags)))

    for tag in tags:
        result = {}
        result['bib_record_id'] = metadata['bib_record_id']
        result['bib_rbx_support'] = metadata['bib_rbx_support']
        result['bib_agence_cat'] = metadata['bib_agence_cat']
        result['bib_pat'] = metadata['bib_pat']
        result['tag'] = "B" + tag
        results.append(result)

df = pd.DataFrame(results)
df.loc[df['bib_pat'] == False, 'bib_pat'] = 'non pat'
df.loc[df['bib_pat'] == True, 'bib_pat'] = 'pat'
df.pivot_table(index='tag', columns=['bib_rbx_support', 'bib_pat'], values='bib_record_id', aggfunc='count').to_excel("resultats/tags_20250831.xlsx")

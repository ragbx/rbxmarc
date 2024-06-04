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
        bib2dict.rbx_vdg()
        metadatas.append(bib2dict.metadatas)


df = pd.DataFrame(metadatas)
df = df[['record_id', 'alignement_bnf', 'rbx_date_creation_notice',
       'rbx_vdg_action', 'rbx_vdg_date', 'rbx_support', 'agence_cat',
       'pat']]

#df.to_excel(join("results", f"rbx_vdg_{date_export2}.xlsx"), index=False)

# erreurs sur type action
# vdg_type_action_ko = df[df['get_rbx_vdg_action'] != 'bib & aut']
# vdg_type_action_ko = vdg_type_action_ko[vdg_type_action_ko['pat'] == False]
# vdg_type_action_ko = vdg_type_action_ko[vdg_type_action_ko['alignement_bnf'] == False]
# vdg_type_action_ko = vdg_type_action_ko[vdg_type_action_ko['rbx_support'].isin(['livre',
#     'partition', 'livre en gros caractères', 'livre audio', 'carte routière'])]


vdg_ko = df[df['alignement_bnf'] == False]
vdg_ko = vdg_ko[vdg_ko['rbx_vdg_action'] == 'bib & aut']
vdg_ko['rbx_annee_creation_notice'] = vdg_ko['rbx_date_creation_notice'].str[0:4].astype(int)
vdg_ko = vdg_ko[vdg_ko['rbx_annee_creation_notice'] >= 2022]
vdg_ko[['record_id']].to_csv(join('results', f"rbx2bbs_{date_export2}.csv"), index=False, header=False)

"""
Script qui permet d'extraire des notices marc depuis un fichier iso2709
et un fichier csv contenant les identifiants de notice (le nom de colonne
doit être "record_id").
"""

from os.path import join
import pandas as pd

from pymarc import MARCReader

def extract_records(marc_file_in, record_ids_file, marc_file_out, export2txt=False):
    record_ids_df = pd.read_csv(record_ids_file)
    record_ids2export = record_ids_df['record_id'].astype(str).to_list()
    
    with open(marc_file_in, 'rb') as fh:
        records2export = []
        reader = MARCReader(fh, to_unicode=True, force_utf8=True)
        for record in reader:
            record_id = record.get_fields('001')
            record_id = record_id[0].data
            if record_id in record_ids2export:
                records2export.append(record)
    
    with open(marc_file_out, 'wb') as out:
        for record in records2export:
            out.write(record.as_marc())
        
        
marc_file_in = "../sample_data/marc_sample_biblio_20240519_238.mrc"
record_ids_file = "../sample_data/test_export.txt"
marc_file_out = "../sample_data/test_export.mrc"

extract_records(marc_file_in, record_ids_file, marc_file_out, export2txt=False)
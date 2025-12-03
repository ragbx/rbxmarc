from get_sru import Sru2iso
from os import listdir
from os.path import join, basename
from pathlib import Path

in_path = join('extractions', 'bnf', 'ark2extract')
out_path = join('extractions', 'bnf', 'ark_extracted')
files = [f for f in listdir(in_path)]
for f in files:
    print(f)
    out_filename = Path(f).stem + ".mrc"
    s = Sru2iso(
        ark_file= join(in_path, f),
        file_out = join(out_path, out_filename))
    s.ark_file2array()
    s.get_records_from_sru()

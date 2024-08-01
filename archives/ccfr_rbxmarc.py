import re

def get_type_notice(record):
    result = get_marc_values(record, ["LDR"])
    result = result[6]
    type_notice_codes = {
        "a": "texte",
        "b": "manuscrit",
        "c": "partition",
        "d": "partition manuscrite",
        "e": "carte",
        "f": "carte manuscrite",
        "g": "video",
        "i": "son - non musical",
        "j": "son - musique",
        "k": "image, dessin",
        "l": "ressource électronique",
        "m": "multimedia",
        "r": "objet"
    }
    if result in type_notice_codes.keys():
        result = type_notice_codes[result]
    return result

def get_niveau_bib(record):
    result = get_marc_values(record, ["LDR"])
    result = result[7]
    niveau_bib_codes = {
        "a": "analytique",
        "i": "ressource intégratrice",
        "m": "monographie",
        "s": "périodique",
        "c": "collection"
    }
    if result in niveau_bib_codes.keys():
        result = niveau_bib_codes[result]
    return result

def get_record_id(record):
    result = get_marc_values(record, ["001"])
    return result

def get_ark(record):
    result = get_marc_values(record, ["033a"])
    return result

def get_alignement_bnf(metadata):
    result = False
    if 'ark:/12148' in metadata['ark']:
        result = True
    return result

def get_frbnf(record):
    result = get_marc_values(record, ["035a"])
    return result

def get_rbx_vdg(record):
    result = get_marc_values(record, ["091a"])
    vdg_codes = {'0': 'rien', '1': 'aut', '2': 'bib & aut'}
    if result in vdg_codes.keys():
        result = vdg_codes[result]
    return result

def get_record_datecreation(record):
    result = get_marc_values(record, ["099c"])
    return result

def get_record_datemodif(record):
    result = get_marc_values(record, ["099d"])
    return result

def get_support(record):
    result = get_marc_values(record, ["099t"])
    support_codes = {
            'AP': 'périodique - article',
            'CA': 'carte routière',
            'CR': 'cd-rom',
            'DC': 'disque compact',
            'DG': 'disque gomme-laque',
            'DV': 'disque microsillon',
            'IC': 'document iconographique',
            'JE': 'jeu',
            'K7': 'cassette audio',
            'LG': 'livre en gros caractères',
            'LI': 'livre',
            'LN': 'livre numérique',
            'LS': 'livre audio',
            'ML': 'méthode de langue',
            'MT': 'matériel',
            'PA': 'partition',
            'PE': 'périodique',
            'VD': 'dvd',
            'VI': 'vhs, umatic ou film'
        }
    if result in support_codes.keys():
        result = support_codes[result]
    return result

def get_title(record):
    result = get_marc_values(record, ["200ae"])
    return result

def get_responsability(record):
    result = get_marc_values(record, ["700abf", "701abf", "702abf", "710af", "711af", "712af"])
    if result == '':
        get_marc_values(record, ["200f"])
    return result

def get_subject(record):
    result = get_marc_values(record, ["600abcdefghijklmnopqrstuvwxyz",
                                    "601abcdefghijklmnopqrstuvwxyz",
                                    "602abcdefghijklmnopqrstuvwxyz",
                                    "604abcdefghijklmnopqrstuvwxyz",
                                    "605abcdefghijklmnopqrstuvwxyz",
                                    "606abcdefghijklmnopqrstuvwxyz",
                                    "607abcdefghijklmnopqrstuvwxyz",
                                    "608abcdefghijklmnopqrstuvwxyz",
                                    "609abcdefghijklmnopqrstuvwxyz"])
    return result

def get_publication_date(record):
    result = get_marc_values(record, ["100a"])
    result = result[9:13]
    res_ok = re.match(r"^\d{4}$", result)
    if res_ok == None:
        result = result[13:17]
    res_ok = re.match(r"^\d{4}$", result)
    if res_ok == None:
        result = get_marc_values(record, ["214d"])
    if result == '':
        result = get_marc_values(record, ["210d"])
    if result == '':
        result = get_marc_values(record, ["219d"])

    res_ok = re.match(r"^\d{4}$", result)
    if res_ok == None:
        if result != '':
            match = re.search(r'\d{3}.', result)
            if match:
                result = match.group()
                result = re.sub(r'.$', '0', result)

    res_ok = re.match(r"^\d{4}$", result)
    if res_ok == None:
        if result != '':
            match = re.search(r'\d{2}.{2}', result)
            if match:
                result = match.group()
                result = re.sub(r'.{2}$', '00', result)
    return result

def get_publisher(record):
    result = get_marc_values(record, ["214c"])
    if result == '':
        result = get_marc_values(record, ["210c"])
    if result == '':
        result = get_marc_values(record, ["219c"])
    return result

def get_marc_values(record, tags, aslist=False):
    result = []
    for tag in tags:
        # cas du label
        if tag == 'LDR':
            result.append(record.leader)
        else:
            fields = record.get_fields(tag[:3])
            for field in fields:
                field_value = []
                # cas du controlfield
                if tag[:2] == '00':
                    field_value.append(field.data)
                # cas du datafield
                else:
                    if hasattr(field, "subfields"):
                        for subfield in field.subfields:
                            if subfield.code in tag[3:]:
                                field_value.append(subfield.value)
                result.append(" ".join(field_value))
    if aslist:
        return result
    else:
        return " ; ".join(result)

def get_subfield_values(field, subfield_tag, values = None):
    result = None
    if hasattr(field, "subfields"):
        for subfield in field.subfields:
            if subfield.code == subfield_tag:
                if values:
                    if subfield.value in values:
                        result = subfield.value
                else:
                    result = subfield.value
    return result

def get_record_id(record):
    result = get_marc_values(record, ["001"])
    return result

def get_f930a(record):
    result = get_marc_values(record, ["930a"])
    return result

def get_f9305(record):
    result = get_marc_values(record, ["9305"])
    return result

def get_f930b(record):
    result = get_marc_values(record, ["930b"])
    return result

def get_f930e(record):
    result = get_marc_values(record, ["930eb"])
    return result

def get_ccfr_pat(record):
    result = None
    ccodes = get_marc_values(record, ["995h"])
    ccodes = ccodes.split(" ; ")
    for ccode in ccodes:
        if ccode in ['PENCVZZ', 'PENRSZZ', 'PPELGZZ', 'PPEPMZZ', 'PPEFGZZ', 'PPIPIZZ']:
            result = ccode
            break
    return result

def get_ccodes(record):
    result = get_marc_values(record, ["995h"])
    return result

def get_unique_ccodes(record):
    result = get_marc_values(record, ["995h"], aslist=True)
    result = list(set(result))
    return " ; ".join(result)

def get_items_pat(record):
    result = []
    #pat_ccodes = ['PENCVZZ', 'PENRSZZ', 'PPELGZZ', 'PPEPMZZ', 'PPEFGZZ', 'PPIPIZZ'] # incomplet
    ccfr_ccodes = ['PENCVZZ', 'PENRSZZ', 'PPELGZZ', 'PPEPMZZ', 'PPEFGZZ', 'PPIPIZZ']
    fields = record.get_fields("995")
    for field in fields:
        ccode = None
        ccode = get_subfield_values(field, 'h', values = ccfr_ccodes)
        if ccode:
            date_creation_item = get_subfield_values(field, '5')
            itemnumber = get_subfield_values(field, '9')
            code_barres = get_subfield_values(field, 'f')
            biblionumber = get_record_id(record)

            data = {
                "biblionumber": biblionumber,
                "itemnumber": itemnumber,
                "code_barres": code_barres,
                "date_creation_item": date_creation_item,
                "ccode": ccode
            }
            result.append(data)
    return result

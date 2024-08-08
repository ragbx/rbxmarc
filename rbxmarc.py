import pandas as pd
import json
from os.path import join

from pymarc import MARCReader

def extract_records(marc_file_in, record_ids2export, marc_file_out=None, export2txt=False):
    """
    Fonction qui permet d'extraire des notices marc depuis un fichier iso2709
    et une liste d'identifiants contenant les identifiants de notice (le nom de colonne
    doit être "record_id").
    """
    #record_ids_df = pd.read_csv(record_ids_file)
    #record_ids2export = record_ids_df['record_id'].astype(str).to_list()

    with open(marc_file_in, 'rb') as fh:
        records2export = []
        reader = MARCReader(fh, to_unicode=True, force_utf8=True)
        i = 0
        for record in reader:
            i += 1
            if i % 10000 == 0:
                print(i)
            record_id = record.get_fields('001')
            record_id = record_id[0].data
            record_id = str(record_id)
            if record_id in record_ids2export:
                records2export.append(record)
    if marc_file_out:
        with open(marc_file_out, 'wb') as out:
            for record in records2export:
                out.write(record.as_marc())

    return records2export

class Rbxmrc():
    """
    Classe mère permettant de récupérer les réferentiels et une
    fonction d'extraction des champs marc
    """

    def __init__(self, **kwargs):
        if 'referentiels' in kwargs:
            self.referentiels = kwargs.get('referentiels')
        else:
            self.get_referentiels()

    def get_referentiels(self):
        """
        Fonction qui permet de transformer le fichier referentiel en dictionnaire.
        """
        referentiels = {}
        referentiels_df = pd.read_csv("utils/referentiels/referentiels.csv")
        for referentiel, referentiel_df in referentiels_df.groupby(['referentiel']):
            referentiel = referentiel[0]
            referentiels[referentiel] = {}
            zip_result = zip(referentiel_df['cle'].to_list(), referentiel_df['valeur'].to_list())
            referentiels[referentiel] = dict(zip_result)
        self.referentiels = referentiels

    def get_marc_values(self, tags, aslist=False):
        """
        Permet d'extraire la valeur d'un ou plusieurs champs/sous-champs.
        Les champs ("tags") sont saisis sous forme de list.
        Par exemple, ["7OOab", "701ab"]

        Le résultat de base est une liste. Par défaut, il retourné sous forme de
        chaîne de caractères avec comme séparateur d'élements " ; ". On peut annuler
        ce comportement grâce à l'argument "aslist=True".
        """
        result = []
        for tag in tags:
            # cas du label
            if tag == 'LDR':
                result.append(self.record.leader.leader)
            else:
                fields = self.record.get_fields(tag[:3])
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

class Rbxbib2dict(Rbxmrc):
    """
    Classe qui permet de transformer une notice bibliographique MARC en dictionnaire
    En entrée :
    - obligatoirement, un objet Pymarc record
    - optionnellement, sous forme de fichier csv, un référentiel de codes /
    valeurs correspondantes

    En sortie, on obtient un dictionnaire
    """
    def __init__(self, record, **kwargs):
        super().__init__(**kwargs)
        self.record = record
        self.metadatas = {}

    def extraction_complete(self):
        """
        Rassemble toutes les fonctions d'extraction existantes
        """
        self.get_bib_record_id()
        self.get_bib_statut_notice()
        self.get_bib_type_notice()
        self.get_bib_niveau_bib()
        self.get_bib_relation_hierarchique()
        self.get_bib_isbn()
        self.get_bib_issn()
        self.get_bib_ark_bnf()
        self.get_bib_alignement_bnf()
        self.get_bib_frbnf()
        self.get_bib_refcom()
        self.get_bib_ean()
        self.get_bib_rbx_vdg_action()
        self.get_bib_rbx_vdg_date()
        self.get_bib_rbx_support()
        self.get_bib_rbx_date_creation_notice()
        self.get_bib_rbx_date_modification_notice()
        self.get_bib_date_creation_notice_B100()
        self.get_bib_publication_date_B100()
        self.get_bib_langue()
        self.get_bib_langue_originale()
        self.get_bib_pays()
        self.get_bib_scale()
        self.get_bib_title()
        self.get_bib_key_title()
        self.get_bib_global_title()
        self.get_bib_part_title()
        self.get_bib_numero_tome()
        self.get_bib_responsability()
        self.get_bib_subject()
        self.get_bib_publication_date_B210()
        self.get_bib_publication_date_B214()
        self.get_bib_publication_date_B219()
        self.get_bib_publication_date()
        self.get_bib_publisher_B210()
        self.get_bib_publisher_B214()
        self.get_bib_publisher_B219()
        self.get_bib_publisher()
        self.get_bib_publication_place_B210()
        self.get_bib_publication_place_B214()
        self.get_bib_publication_place_B219()
        self.get_bib_publication_place()
        self.get_bib_public()
        self.get_bib_agence_cat()
        self.get_bib_pat()
        self.get_bib_nb_items()
        self.get_bib_links()

    # Extractions récurrentes
    def rbx_qual(self):
        """
        Pour étude de la qualité des notices
        """
        self.get_bib_record_id()
        self.get_bib_statut_notice()
        self.get_bib_type_notice()
        self.get_bib_niveau_bib()
        self.get_bib_relation_hierarchique()
        self.get_bib_alignement_bnf()
        self.get_bib_rbx_date_creation_notice()
        self.get_bib_rbx_vdg_action()
        self.get_bib_rbx_vdg_date()
        self.get_bib_rbx_support()
        self.get_bib_title()
        self.get_bib_publication_date_B100()
        self.get_bib_publication_date_B210()
        self.get_bib_publication_date_B214()
        self.get_bib_publication_date_B219()
        self.get_bib_publication_date()
        self.get_bib_public()
        self.get_bib_agence_cat()
        self.get_bib_pat()
        self.get_bib_nb_items()


    def rbx_vdg(self):
        """
        Pour étude du processus de vendange
        """
        self.get_bib_record_id()
        self.get_bib_alignement_bnf()
        self.get_bib_rbx_date_creation_notice()
        self.get_bib_rbx_vdg_action()
        self.get_bib_rbx_vdg_date()
        self.get_bib_rbx_support()
        self.get_bib_agence_cat()
        self.get_bib_pat()


    # Fonctions d'analyse
    def get_bib_alignement_bnf(self):
        """
        Si la notice est alignée sur la Bnf, on attribue une valeur vraie.
        """
        result = False
        if 'ark_bnf' not in self.metadatas:
            self.get_bib_ark_bnf()
        if 'ark:/12148' in self.metadatas['bib_ark_bnf']:
            result = True
        self.metadatas['bib_alignement_bnf'] = result

    def get_bib_pat(self):
        """
        Indique si une notice décrit au moins un exemplaire relevant du patrimoine
        (selon les codes de collection).
        """
        # liste des codes collections décrivant une collection patrimoniale
        pat_ccodes = [
            'PENACZZ', # Patrimoine sonore - Fonds Alfonso Cata
            'PENCVZZ', # Patrimoine sonore - Fonds Charles Verstraete
            'PENDEZZ', # Patrimoine sonore - Fonds Desette
            'PENHPZZ', # Patrimoine sonore - Fonds Heath et Payant - enregistrements
            'PENPDZZ', # Collection Patrice Desdoit # à vérifier
            'PENRSZZ', # Patrimoine sonore - FLRS
            'PPAFIZZ', # Patrimoine audiovisuel - Fonds local image
            'PPEFGZZ', # Patrimoine écrit - fonds général
            'PPELGZZ', # Patrimoine écrit - legs Destombes
            'PPEPMZZ', # Patrimoine musical imprimé
            'PPEPRZZ', # Périodiques patrimonaiux
            'PPIPIZZ' # Patrimoine iconographique
            # 'PRRFIZZ', # Films autour de Roubaix et sa région  # non : collections de prêts
            # 'PRRMEZZ', # FLRS de prêt # non : collections de prêts
            # 'PRRRGZZ', # Région # non : collections de prêts
            # 'PRRRXZZ'  # Roubaix # non : collections de prêts
        ]

        result = False
        ccodes = self.get_marc_values(["995h"])
        ccodes = ccodes.split(" ; ")
        for ccode in ccodes:
            if ccode in pat_ccodes:
                result = True
                break
        self.metadatas['bib_pat'] = result

    def get_bib_nb_items(self):
        """
        Renvoie le nb d'exemplaires (le nb de champs B995) à une notice bib
        """
        fields = self.record.get_fields('995')
        self.metadatas['bib_nb_items'] = len(fields)

    def get_bib_links(self):
        """
        Renvoie les identifiants des notices autorités liées à une notice bib.
        Identifiants interne ($9) et BnF ($3).
        """
        bnf_authnumbers = []
        koha_authnumbers = []

        tags = [tag for tag in range(600, 610)]
        for tag in range(700, 704):
            tags.append(tag)
        for tag in range(710, 714):
            tags.append(tag)

        for tag in tags:
            tag = str(tag)
            fields = self.record.get_fields(tag)
            for field in fields:
                numbers = field.get_subfields('3')
                for number in numbers:
                    bnf_authnumbers.append(number)
                numbers = field.get_subfields('9')
                for number in numbers:
                    koha_authnumbers.append(number)
        result = {'bnf_authnumbers': bnf_authnumbers, 'koha_authnumbers': koha_authnumbers}
        self.metadatas['bib_links']  = json.dumps(result)

    # Extraction de champs
    def get_bib_record_id(self):
        """
        Renvoie le numéro de la notice (champs B001)
        """
        self.metadatas['bib_record_id'] = self.get_marc_values(["001"])

    def get_bib_statut_notice(self):
        """
        On récupère la position 5 (statut de notice) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[5]
        bib_statut_notice_codes = self.referentiels['bib_statut_notice_codes']
        if result in bib_statut_notice_codes.keys():
            result = bib_statut_notice_codes[result]
        self.metadatas['bib_statut_notice'] = result

    def get_bib_type_notice(self):
        """
        On récupère la position 6 (type de notice) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[6]
        type_notice_bib_codes = self.referentiels['bib_type_notice_codes']
        if result in type_notice_bib_codes.keys():
            result = type_notice_bib_codes[result]
        self.metadatas['bib_type_notice'] = result

    def get_bib_niveau_bib(self):
        """
        On récupère la position 7 (niveau bibliographique) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[7]
        niveau_bib_codes = self.referentiels['bib_niveau_codes']
        if result in niveau_bib_codes.keys():
            result = niveau_bib_codes[result]
        self.metadatas['bib_niveau_bib'] = result

    def get_bib_relation_hierarchique(self):
        """
        On récupère la position 8 (relation hiérarchique) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[8]
        bib_relation_hierarch_codes = self.referentiels['bib_relation_hierarch_codes']
        if result in bib_relation_hierarch_codes.keys():
            result = bib_relation_hierarch_codes[result]
        self.metadatas['bib_relation_hierarchique'] = result

    def get_bib_isbn(self):
        """
        On récupère l'isbn en B010$a.
        """
        result = self.get_marc_values(["010a"])
        self.metadatas['bib_isbn'] = result

    def get_bib_issn(self):
        """
        On récupère l'issn en B011$a.
        """
        result = self.get_marc_values(["011a"])
        self.metadatas['bib_issn'] = result

    def get_bib_ark_bnf(self):
        """
        On récupère l'ark bnf en B033$a.
        """
        result = self.get_marc_values(["033a"])
        result = result.replace("http://catalogue.bnf.fr/", "")
        result = result.replace("https://catalogue.bnf.fr/", "")
        self.metadatas['bib_ark_bnf'] = result

    def get_bib_alignement_bnf(self):
        """
        Si la notice est alignée sur la Bnf, on attribue une valeur vraie.
        """
        result = False
        if 'ark_bnf' not in self.metadatas:
            self.get_bib_ark_bnf()
        if 'ark:/12148' in self.metadatas['bib_ark_bnf']:
            result = True
        self.metadatas['bib_alignement_bnf'] = result

    def get_bib_frbnf(self):
        """
        On récupère le numéro FRBNF en B035$a.
        """
        result = ''
        data = self.get_marc_values(["035a"])
        if 'FRBNF' in data:
            result = data
        self.metadatas['bib_frbnf'] = result

    def get_bib_refcom(self):
        """
        On récupère la référence commerciale en B071$ba.
        """
        result = self.get_marc_values(["071ba"])
        self.metadatas['bib_refcom'] = result

    def get_bib_ean(self):
        """
        On récupère l'ean en B073$a.
        """
        result = self.get_marc_values(["073a"])
        self.metadatas['bib_ean'] = result

    def get_bib_rbx_vdg_action(self):
        """
        On récupère en B091a l'action à mener par le vendangeur Koha
        et on la remplace par son libellé.
        Trois actions  possibles :
        - aucune action (valeur 0)
        - autorités uniquement (valeur 1)
        - notices bibliographiques et autorités (valeur 2)
        """
        result = self.get_marc_values(["091a"])
        vdg_codes = self.referentiels['koha_av_v091a']
        if result in vdg_codes.keys():
            result = vdg_codes[result]
        self.metadatas['bib_rbx_vdg_action'] = result

    def get_bib_rbx_vdg_date(self):
        """
        On récupère la date de dernière vendange en 091b
        """
        result = self.get_marc_values(["091b"])
        self.metadatas['bib_rbx_vdg_date'] = result

    def get_bib_rbx_support(self):
        """
        On récupère en B099t le code du support dans Koha
        et on le remplace par son libellé.
        """
        result = self.get_marc_values(["099t"])
        support_codes = self.referentiels['koha_av_typedoc']
        if result in support_codes.keys():
            result = support_codes[result]
        self.metadatas['bib_rbx_support'] = result

    def get_bib_rbx_date_creation_notice(self):
        """
        On récupère en 099$c la date de création de la notice biblio dans Koha.
        """
        result = self.get_marc_values(["099c"])
        self.metadatas['bib_rbx_date_creation_notice'] = result

    def get_bib_rbx_date_modification_notice(self):
        """
        On récupère en 099$d la date de dernière modification de la notice biblio
        dans Koha.
        """
        result = self.get_marc_values(["099c"])
        self.metadatas['bib_rbx_date_modification_notice'] = result

    def get_bib_date_creation_notice_B100(self):
        """
        On récupère en 100$a positions 0-7 la date de création de la notice dans
        le système d'origine.
        """
        result = self.get_marc_values(["100a"])
        result = result[0:8]
        self.metadatas['bib_date_creation_notice_B100'] = result

    def get_bib_publication_date_B100(self):
        """
        On récupère en 100$a positions 9-12 la première date de publication du
        document.
        """
        result = self.get_marc_values(["100a"])
        result = result[9:13]
        self.metadatas['bib_publication_date_B100'] = result

    def get_bib_langue(self):
        """
        On récupère en 101$a la langue du document.
        """
        result = self.get_marc_values(["101a"])
        self.metadatas['bib_langue_document'] = result

    def get_bib_langue_originale(self):
        """
        On récupère en 101$c la langue originale du document.
        """
        result = self.get_marc_values(["101c"])
        self.metadatas['bib_langue_originale_document'] = result

    def get_bib_pays(self):
        """
        On récupère en 102$a le pays de parution ou de production.
        """
        result = self.get_marc_values(["102a"])
        self.metadatas['bib_pays'] = result

    def get_bib_scale(self):
        """
        Pour les documents cartographiques, on récupère l'échelle prioritairement
        en B123$b, sinon en B206$b.
        """
        result = self.get_marc_values(["123b"])
        if result == '':
            result = self.get_marc_values(["206b"])
        self.metadatas['bib_scale'] = result

    def get_bib_title(self):
        """
        On récupère le titre en B200$ae.
        """
        result = self.get_marc_values(["200ae"])
        self.metadatas['bib_title'] = result

    def get_bib_key_title(self):
        """
        On récupère le titre clé (périodiques) en B530$a.
        Si absent, on va chercher en B200$ae.
        """
        result = self.get_marc_values(["530a"])
        if result == '':
            result = self.get_marc_values(["200ae"])
        self.metadatas['bib_key_title'] = result

    def get_bib_global_title(self):
        """
        On récupère le titre de collection en B225$a.
        Si absent, on va chercher en B200$ae.
        """
        result = self.get_marc_values(["225a"])
        if result == '':
            result = self.get_marc_values(["200ae"])
        self.metadatas['bib_global_title'] = result

    def get_bib_part_title(self):
        result = self.get_marc_values(["464t"])
        if result == '':
            result = self.get_marc_values(["200ae"])
        self.metadatas['bib_part_title'] = result

    def get_bib_numero_tome(self):
        result = self.get_marc_values(["200h"])
        if result == '':
            result = self.get_marc_values(["461v"])
        self.metadatas['bib_numero_tome'] = result

    def get_bib_responsability(self):
        result = self.get_marc_values(["700ab", "710ab", "701ab", "711ab", "702ab", "712ab"])
        if result == '':
            self.get_marc_values(["200f"])
        self.metadatas['bib_responsability'] = result

    def get_bib_subject(self):
        result = self.get_marc_values(["600abcdefghijklmnopqrstuvwxyz",
                                        "601abcdefghijklmnopqrstuvwxyz",
                                        "602abcdefghijklmnopqrstuvwxyz",
                                        "604abcdefghijklmnopqrstuvwxyz",
                                        "605abcdefghijklmnopqrstuvwxyz",
                                        "606abcdefghijklmnopqrstuvwxyz",
                                        "607abcdefghijklmnopqrstuvwxyz",
                                        "608abcdefghijklmnopqrstuvwxyz",
                                        "609abcdefghijklmnopqrstuvwxyz"])
        self.metadatas['bib_subject'] = result

    def get_bib_publication_date_B210(self):
        result = self.get_marc_values(["210d"])
        self.metadatas['bib_publication_date_B210'] = result

    def get_bib_publication_date_B214(self):
        result = self.get_marc_values(["214d"])
        self.metadatas['bib_publication_date_B214'] = result

    def get_bib_publication_date_B219(self):
        result = self.get_marc_values(["219d"])
        self.metadatas['bib_publication_date_B219'] = result

    def get_bib_publication_date(self):
        if 'publication_date_B100' in self.metadatas:
            result = self.metadatas['bib_publication_date_B100']
        else :
            result = self.get_bib_publication_date_B100()

        if result == '':
            if 'publication_date_B214' in self.metadatas:
                result = self.metadatas['bib_publication_date_B214']
            else :
                result = self.get_bib_publication_date_B214()

        if result == '':
            if 'publication_date_B210' in self.metadatas:
                result = self.metadatas['bib_publication_date_B210']
            else :
                result = self.get_bib_publication_date_B210()

        if result == '':
            if 'publication_date_B219' in self.metadatas:
                result = self.metadatas['bib_publication_date_B219']
            else :
                result = self.get_bib_publication_date_B219()

        self.metadatas['bib_publication_date'] = result

    def get_bib_publisher_B210(self):
        result = self.get_marc_values(["210c"])
        self.metadatas['bib_publisher_B210'] = result

    def get_bib_publisher_B214(self):
        result = self.get_marc_values(["214c"])
        self.metadatas['bib_publisher_B214'] = result

    def get_bib_publisher_B219(self):
        result = self.get_marc_values(["219c"])
        self.metadatas['bib_publisher_B219'] = result

    def get_bib_publisher(self):
        if 'publisher_B214' in self.metadatas:
            result = self.metadatas['bib_publisher_B214']
        else :
            result = self.get_bib_publisher_B214()

        if result == '':
            if 'publisher_B210' in self.metadatas:
                result = self.metadatas['bib_publisher_B210']
            else :
                result = self.get_bib_publisher_B210()

        if result == '':
            if 'publisher_B219' in self.metadatas:
                result = self.metadatas['bib_publisher_B219']
            else :
                result = self.get_bib_publisher_B219()

        self.metadatas['bib_publisher'] = result

    def get_bib_publication_place_B210(self):
        result = self.get_marc_values(["210a"])
        self.metadatas['bib_publication_place_B210'] = result

    def get_bib_publication_place_B214(self):
        result = self.get_marc_values(["214a"])
        self.metadatas['bib_publication_place_B214'] = result

    def get_bib_publication_place_B219(self):
        result = self.get_marc_values(["219a"])
        self.metadatas['bib_publication_place_B219'] = result

    def get_bib_publication_place(self):
        if 'publication_place_B214' in self.metadatas:
            result = self.metadatas['bib_publication_place_B214']
        else :
            result = self.get_bib_publication_place_B214()

        if result == '':
            if 'publication_place_B210' in self.metadatas:
                result = self.metadatas['bib_publication_place_B210']
            else :
                result = self.get_bib_publication_place_B210()

        if result == '':
            if 'publication_place_B219' in self.metadatas:
                result = self.metadatas['bib_publication_place_B219']
            else :
                result = self.get_bib_publication_place_B219()

        self.metadatas['bib_publication_place'] = result

    def get_bib_public(self):
        """
        Extraction du public cible.
        Champs spécifique à Roubaix.
        """
        result = self.get_marc_values(["339a"])
        koha_av_publicc = self.referentiels['koha_av_publicc']
        if result in koha_av_publicc.keys():
            result = koha_av_publicc[result]
        self.metadatas['bib_rbx_public'] = result

    def get_bib_agence_cat(self):
        """
        Extraction de l'agence catalographique, en B801b.
        """
        result = self.get_marc_values(["801b"])
        agence_cat_codes = self.referentiels['agence_cat_codes']
        if result in agence_cat_codes.keys():
            result = agence_cat_codes[result]
        else:
            if result:
                result = 'autre'
        self.metadatas['bib_agence_cat'] = result


class Rbxauth2dict(Rbxmrc):
    """
    Classe qui permet de transformer une notice autorité MARC en dictionnaire
    En entrée :
    - obligatoirement, un objet Pymarc record
    - optionnellement, sous forme de fichier csv, un référentiel de codes /
    valeurs correspondantes

    En sortie, on obtient un dictionnaire
    """
    def __init__(self, record, **kwargs):
        super().__init__(**kwargs)
        self.record = record
        self.metadatas = {}

    def extraction_complete(self):
        """
        Rassemble toutes les fonctions d'extraction existantes
        """
        self.get_auth_record_id()
        self.get_auth_statut_notice()
        self.get_auth_type_entite()
        self.get_auth_type_notice()
        self.get_auth_date_modification()
        self.get_auth_point_acces()
        self.get_auth_isni()
        self.get_auth_ark_bnf_A003()
        self.get_auth_ark_bnf_A009()
        self.get_auth_ark_bnf_A033()
        self.get_auth_frbnf_A035()
        self.get_auth_frbnf_A999a()
        self.get_auth_frbnf_A999b()

    # Extraction de champs
    def get_auth_record_id(self):
        """
        Renvoie le numéro de la notice (champs B001)
        """
        self.metadatas['auth_record_id'] = self.get_marc_values(["001"])

    def get_auth_statut_notice(self):
        """
        On récupère la position 5 (statut de notice) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[5]
        auth_statut_notice_codes = self.referentiels['auth_statut_notice_codes']
        if result in auth_statut_notice_codes.keys():
            result = auth_statut_notice_codes[result]
        self.metadatas['auth_statut_notice'] = result

    def get_auth_type_notice(self):
        """
        On récupère la position 6 (type de notice) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[6]
        auth_type_notice = self.referentiels['auth_type_notice']
        if result in auth_type_notice.keys():
            result = auth_type_notice[result]
        self.metadatas['auth_type_notice'] = result

    def get_auth_type_entite(self):
        """
        On récupère la position 9 (type d'entité) du label
        et on la remplace par son libellé.
        """
        result = self.get_marc_values(["LDR"])
        result = result[9]
        auth_type_entite = self.referentiels['auth_type_entite']
        if result in auth_type_entite.keys():
            result = auth_type_entite[result]
        self.metadatas['auth_type_entite'] = result

    def get_auth_date_modification(self):
        result = self.get_marc_values(["005"])
        self.metadatas['auth_date_modifcation'] = result

    def get_auth_point_acces(self):
        """
        On récupère le point d'accès.
        On renseigne un type de point d'accès (en principe, on obtient même
        résultat que pour le type d'entité.)
        """
        pa_tags = [
            # présents à Rbx, dans ordre fréquence
            ["200abcdefghijklmnpqrstuvwxy", "personne"],
            ["250abcdefghijklmnpqrstuvwxyz", "nom_commun"],
            ["210abcdefghijklmnpqrstuvwxyz", "collectivite"],
            ["215abcdefghijklmnpqrstuvwxyz", "nom_geographique"],
            ["240abcdefghijklmnpqrstuvwxyz", "auteur_titre"],
            ["280abcdefghijklmnpqrstuvwxyz", "genre_forme"],
            ["230abcdefghijklmnpqrstuvwxyz", "titre_uniforme"],
            ["220abcdefghijklmnpqrstuvwxyz", "famille"],
            ["260abcdefghijklmnpqrstuvwxyz", "lieu_edition"],

            # pas encore (?) utilisé à Rbx
            ["216abcdefghijklmnpqrstuvwxyz", "marque"],
            ["217abcdefghijklmnpqrstuvwxyz", "imprimeur-libraire"],
            ["223abcdefghijklmnpqrstuvwxyz", "personnage_fictif"],
            ["231abcdefghijklmnpqrstuvwxyz", "titre_oeuvre"],
            ["232abcdefghijklmnpqrstuvwxyz", "titre_expression"],
            ["235abcdefghijklmnpqrstuvwxyz", "rubrique_classement"],
            ["241abcdefghijklmnpqrstuvwxyz", "auteur_titre_oeuvre"],
            ["242abcdefghijklmnpqrstuvwxyz", "auteur_titre_expression"],
            ["243abcdefghijklmnpqrstuvwxyz", "auteur_titre_juridique_religieux"],
            ["245abcdefghijklmnpqrstuvwxyz", "auteur_rubrique_classement"]
        ]

        for pa_tag in pa_tags:
            result = self.get_marc_values([ pa_tag[0] ])
            if result != '':
                break
        self.metadatas['auth_point_acces'] = result
        self.metadatas['auth_point_acces_type'] = pa_tag[1]

    def get_auth_isni(self):
        result = self.get_marc_values(["010a"])
        self.metadatas['auth_isni'] = result

    def get_auth_ark_bnf_A003(self):
        result = self.get_marc_values(["003"])
        self.metadatas['auth_ark_bnf_A003'] = result

    def get_auth_ark_bnf_A009(self):
        result = self.get_marc_values(["009"])
        self.metadatas['auth_ark_bnf_A009'] = result

    def get_auth_ark_bnf_A033(self):
        result = self.get_marc_values(["033a"])
        self.metadatas['auth_ark_bnf_A033'] = result

    def get_auth_frbnf_A035(self):
        result = self.get_marc_values(["035a"])
        self.metadatas['auth_frbnf_A035'] = result

    def get_auth_frbnf_A999a(self):
        result = self.get_marc_values(["999a"])
        self.metadatas['auth_frbnf_A999a'] = result

    def get_auth_frbnf_A999b(self):
        result = self.get_marc_values(["999b"])
        self.metadatas['auth_frbnf_A999b'] = result

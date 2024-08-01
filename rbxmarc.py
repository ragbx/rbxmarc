import pandas as pd
import json

def get_referentiels():
    referentiels = {}
    referentiels_df = pd.read_csv("referentiels.csv")
    for referentiel, referentiel_df in referentiels_df.groupby(['referentiel']):
        referentiel = referentiel[0]
        referentiels[referentiel] = {}
        zip_result = zip(referentiel_df['cle'].to_list(), referentiel_df['valeur'].to_list())
        referentiels[referentiel] = dict(zip_result)
    return referentiels

class Rbxbib2dict():
    """
    Classe qui permet de transformer une notice bibliographique MARC en dictionnaire
    En entrée :
    - obligatoirement, un objet Pymarc record
    - optionnellement, sous forme de fichier csv, un référentiel de codes /
    valeurs correspondantes

    En sortie, on obtient un dictionnaire
    """
    def __init__(self, record, **kwargs):
        self.record = record
        if 'referentiels' in kwargs:
            self.referentiels = kwargs.get('referentiels')
        else:
            self.referentiels = get_referentiels()
        self.metadatas = {}

    def analyse_complete(self):
        """
        Rassemble toutes les fonctions d'extraction existantes
        """
        self.get_record_id()
        self.get_type_notice()
        self.get_niveau_bib()
        self.get_isbn()
        self.get_issn()
        self.get_ark_bnf()
        self.get_alignement_bnf()
        self.get_frbnf()
        self.get_refcom()
        self.get_ean()
        self.get_rbx_vdg_action()
        self.get_rbx_vdg_date()
        self.get_rbx_support()
        self.get_rbx_date_creation_notice()
        self.get_rbx_date_modification_notice()
        self.get_date_creation_notice_B100()
        self.get_publication_date_B100()
        self.get_langue()
        self.get_langue_originale()
        self.get_pays()
        self.get_scale()
        self.get_title()
        self.get_key_title()
        self.get_global_title()
        self.get_part_title()
        self.get_numero_tome()
        self.get_responsability()
        self.get_subject()
        self.get_publication_date_B210()
        self.get_publication_date_B214()
        self.get_publication_date_B219()
        self.get_publication_date()
        self.get_publisher_B210()
        self.get_publisher_B214()
        self.get_publisher_B219()
        self.get_publisher()
        self.get_publication_place_B210()
        self.get_publication_place_B214()
        self.get_publication_place_B219()
        self.get_publication_place()
        self.get_agence_cat()
        self.get_pat()
        self.get_nb_items()
        self.get_links()

    # Extractions récurrentes
    def rbx_qual(self):
        """
        Pour étude de la qualité des notices
        (à compléter)
        """
        self.get_record_id()
        self.get_type_notice()
        self.get_niveau_bib()


    def rbx_vdg(self):
        """
        Pour étude du processus de vendange
        """
        self.get_record_id()
        self.get_alignement_bnf()
        self.get_rbx_date_creation_notice()
        self.get_rbx_vdg_action()
        self.get_rbx_vdg_date()
        self.get_rbx_support()
        self.get_agence_cat()
        self.get_pat()


    # Fonctions d'analyse
    def get_alignement_bnf(self):
        """
        Si la notice est alignée sur la Bnf, on attribue une valeur vraie.
        """
        result = False
        if 'ark_bnf' not in self.metadatas:
            self.get_ark_bnf()
        if 'ark:/12148' in self.metadatas['ark_bnf']:
            result = True
        self.metadatas['alignement_bnf'] = result

    def get_pat(self):
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
        ccodes = self._get_marc_values(["995h"])
        ccodes = ccodes.split(" ; ")
        for ccode in ccodes:
            if ccode in pat_ccodes:
                result = True
                break
        self.metadatas['pat'] = result

    def get_nb_items(self):
        """
        Renvoie le nb d'exemplaires (le nb de champs B995) à une notice bib
        """
        fields = self.record.get_fields('995')
        self.metadatas['nb_items'] = len(fields)

    def get_links(self):
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
        self.metadatas['links']  = json.dumps(result)

    # Extraction de champs
    def get_record_id(self):
        """
        Renvoie le numéro de la notice (champs B001)
        """
        self.metadatas['record_id'] = self._get_marc_values(["001"])

    def get_type_notice(self):
        """
        On récupère la position 6 (type de notice) du label
        et on la remplace par son libellé.
        """
        result = self._get_marc_values(["LDR"])
        result = result[6]
        type_notice_codes = self.referentiels['type_notice_codes']
        if result in type_notice_codes.keys():
            result = type_notice_codes[result]
        self.metadatas['type_notice'] = result

    def get_niveau_bib(self):
        """
        On récupère la position 7 (niveau bibliographique) du label
        et on la remplace par son libellé.
        """
        result = self._get_marc_values(["LDR"])
        result = result[7]
        niveau_bib_codes = self.referentiels['niveau_bib_codes']
        if result in niveau_bib_codes.keys():
            result = niveau_bib_codes[result]
        self.metadatas['niveau_bib'] = result

    def get_isbn(self):
        """
        On récupère l'isbn en B010$a.
        """
        result = self._get_marc_values(["010a"])
        self.metadatas['isbn'] = result

    def get_issn(self):
        """
        On récupère l'issn en B011$a.
        """
        result = self._get_marc_values(["011a"])
        self.metadatas['issn'] = result

    def get_ark_bnf(self):
        """
        On récupère l'ark bnf en B033$a.
        """
        result = self._get_marc_values(["033a"])
        result = result.replace("http://catalogue.bnf.fr/", "")
        result = result.replace("https://catalogue.bnf.fr/", "")
        self.metadatas['ark_bnf'] = result

    def get_alignement_bnf(self):
        """
        Si la notice est alignée sur la Bnf, on attribue une valeur vraie.
        """
        result = False
        if 'ark_bnf' not in self.metadatas:
            self.get_ark_bnf()
        if 'ark:/12148' in self.metadatas['ark_bnf']:
            result = True
        self.metadatas['alignement_bnf'] = result

    def get_frbnf(self):
        """
        On récupère le numéro FRBNF en B035$a.
        """
        result = ''
        data = self._get_marc_values(["035a"])
        if 'FRBNF' in data:
            result = data
        self.metadatas['frbnf'] = result

    def get_refcom(self):
        """
        On récupère la référence commerciale en B071$ba.
        """
        result = self._get_marc_values(["071ba"])
        self.metadatas['refcom'] = result

    def get_ean(self):
        """
        On récupère l'ean en B073$a.
        """
        result = self._get_marc_values(["073a"])
        self.metadatas['ean'] = result

    def get_rbx_vdg_action(self):
        """
        On récupère en B091a l'action à mener par le vendangeur Koha
        et on la remplace par son libellé.
        Trois actions  possibles :
        - aucune action (valeur 0)
        - autorités uniquement (valeur 1)
        - notices bibliographiques et autorités (valeur 2)
        """
        result = self._get_marc_values(["091a"])
        vdg_codes = self.referentiels['koha_av_v091a']
        if result in vdg_codes.keys():
            result = vdg_codes[result]
        self.metadatas['rbx_vdg_action'] = result

    def get_rbx_vdg_date(self):
        """
        On récupère la date de dernière vendange en 091b
        """
        result = self._get_marc_values(["091b"])
        self.metadatas['rbx_vdg_date'] = result

    def get_rbx_support(self):
        """
        On récupère en B099t le code du support dans Koha
        et on le remplace par son libellé.
        """
        result = self._get_marc_values(["099t"])
        support_codes = self.referentiels['koha_av_typedoc']
        if result in support_codes.keys():
            result = support_codes[result]
        self.metadatas['rbx_support'] = result

    def get_rbx_date_creation_notice(self):
        """
        On récupère en 099$c la date de création de la notice biblio dans Koha.
        """
        result = self._get_marc_values(["099c"])
        self.metadatas['rbx_date_creation_notice'] = result

    def get_rbx_date_modification_notice(self):
        """
        On récupère en 099$d la date de dernière modification de la notice biblio
        dans Koha.
        """
        result = self._get_marc_values(["099c"])
        self.metadatas['rbx_date_modification_notice'] = result

    def get_date_creation_notice_B100(self):
        """
        On récupère en 100$a positions 0-7 la date de création de la notice dans
        le système d'origine.
        """
        result = self._get_marc_values(["100a"])
        result = result[0:8]
        self.metadatas['date_creation_notice_B100'] = result

    def get_publication_date_B100(self):
        """
        On récupère en 100$a positions 9-12 la première date de publication du
        document.
        """
        result = self._get_marc_values(["100a"])
        result = result[9:13]
        self.metadatas['publication_date_B100'] = result

    def get_langue(self):
        """
        On récupère en 101$a la langue du document.
        """
        result = self._get_marc_values(["101a"])
        self.metadatas['langue_document'] = result

    def get_langue_originale(self):
        """
        On récupère en 101$c la langue originale du document.
        """
        result = self._get_marc_values(["101c"])
        self.metadatas['langue_originale_document'] = result

    def get_pays(self):
        """
        On récupère en 102$a le pays de parution ou de production.
        """
        result = self._get_marc_values(["102a"])
        self.metadatas['pays'] = result

    def get_scale(self):
        """
        Pour les documents cartographiques, on récupère l'échelle prioritairement
        en B123$b, sinon en B206$b.
        """
        result = self._get_marc_values(["123b"])
        if result == '':
            result = self._get_marc_values(["206b"])
        self.metadatas['scale'] = result

    def get_title(self):
        """
        On récupère le titre en B200$ae.
        """
        result = self._get_marc_values(["200ae"])
        self.metadatas['title'] = result

    def get_key_title(self):
        """
        On récupère le titre clé (périodiques) en B530$a.
        Si absent, on va chercher en B200$ae.
        """
        result = self._get_marc_values(["530a"])
        if result == '':
            result = self._get_marc_values(["200ae"])
        self.metadatas['key_title'] = result

    def get_global_title(self):
        """
        On récupère le titre de collection en B225$a.
        Si absent, on va chercher en B200$ae.
        """
        result = self._get_marc_values(["225a"])
        if result == '':
            result = self._get_marc_values(["200ae"])
        self.metadatas['global_title'] = result

    def get_part_title(self):
        result = self._get_marc_values(["464t"])
        if result == '':
            result = self._get_marc_values(["200ae"])
        self.metadatas['part_title'] = result

    def get_numero_tome(self):
        result = self._get_marc_values(["200h"])
        if result == '':
            result = self._get_marc_values(["461v"])
        self.metadatas['numero_tome'] = result

    def get_responsability(self):
        result = self._get_marc_values(["700ab", "710ab", "701ab", "711ab", "702ab", "712ab"])
        if result == '':
            self._get_marc_values(["200f"])
        self.metadatas['responsability'] = result

    def get_subject(self):
        result = self._get_marc_values(["600abcdefghijklmnopqrstuvwxyz",
                                        "601abcdefghijklmnopqrstuvwxyz",
                                        "602abcdefghijklmnopqrstuvwxyz",
                                        "604abcdefghijklmnopqrstuvwxyz",
                                        "605abcdefghijklmnopqrstuvwxyz",
                                        "606abcdefghijklmnopqrstuvwxyz",
                                        "607abcdefghijklmnopqrstuvwxyz",
                                        "608abcdefghijklmnopqrstuvwxyz",
                                        "609abcdefghijklmnopqrstuvwxyz"])
        self.metadatas['subject'] = result

    def get_publication_date_B210(self):
        result = self._get_marc_values(["210d"])
        self.metadatas['publication_date_B210'] = result

    def get_publication_date_B214(self):
        result = self._get_marc_values(["214d"])
        self.metadatas['publication_date_B214'] = result

    def get_publication_date_B219(self):
        result = self._get_marc_values(["219d"])
        self.metadatas['publication_date_B219'] = result

    def get_publication_date(self):
        if 'publication_date_B100' in self.metadatas:
            result = self.metadatas['publication_date_B100']
        else :
            result = self.get_publication_date_B100()

        if result == '':
            if 'publication_date_B214' in self.metadatas:
                result = self.metadatas['publication_date_B214']
            else :
                result = self.get_publication_date_B214()

        if result == '':
            if 'publication_date_B210' in self.metadatas:
                result = self.metadatas['publication_date_B210']
            else :
                result = self.get_publication_date_B210()

        if result == '':
            if 'publication_date_B219' in self.metadatas:
                result = self.metadatas['publication_date_B219']
            else :
                result = self.get_publication_date_B219()

        self.metadatas['publication_date'] = result

    def get_publisher_B210(self):
        result = self._get_marc_values(["210c"])
        self.metadatas['publisher_B210'] = result

    def get_publisher_B214(self):
        result = self._get_marc_values(["214c"])
        self.metadatas['publisher_B214'] = result

    def get_publisher_B219(self):
        result = self._get_marc_values(["219c"])
        self.metadatas['publisher_B219'] = result

    def get_publisher(self):
        if 'publisher_B214' in self.metadatas:
            result = self.metadatas['publisher_B214']
        else :
            result = self.get_publisher_B214()

        if result == '':
            if 'publisher_B210' in self.metadatas:
                result = self.metadatas['publisher_B210']
            else :
                result = self.get_publisher_B210()

        if result == '':
            if 'publisher_B219' in self.metadatas:
                result = self.metadatas['publisher_B219']
            else :
                result = self.get_publisher_B219()

        self.metadatas['publisher'] = result

    def get_publication_place_B210(self):
        result = self._get_marc_values(["210a"])
        self.metadatas['publication_place_B210'] = result

    def get_publication_place_B214(self):
        result = self._get_marc_values(["214a"])
        self.metadatas['publication_place_B214'] = result

    def get_publication_place_B219(self):
        result = self._get_marc_values(["219a"])
        self.metadatas['publication_place_B219'] = result

    def get_publication_place(self):
        if 'publication_place_B214' in self.metadatas:
            result = self.metadatas['publication_place_B214']
        else :
            result = self.get_publication_place_B214()

        if result == '':
            if 'publication_place_B210' in self.metadatas:
                result = self.metadatas['publication_place_B210']
            else :
                result = self.get_publication_place_B210()

        if result == '':
            if 'publication_place_B219' in self.metadatas:
                result = self.metadatas['publication_place_B219']
            else :
                result = self.get_publication_place_B219()

        self.metadatas['publication_place'] = result

    def get_agence_cat(self):
        """
        Extraction de l'agence catalographique, en B801b.
        """
        result = self._get_marc_values(["801b"])
        agence_cat_codes = self.referentiels['agence_cat_codes']
        if result in agence_cat_codes.keys():
            result = agence_cat_codes[result]
        else:
            if result:
                result = 'autre'
        self.metadatas['agence_cat'] = result

    # def get_pat(self):
    #     result = False
    #     ccodes = self._get_marc_values(["995h"])
    #     ccodes = ccodes.split(" ; ")
    #     for ccode in ccodes:
    #         if ccode in ['PENPDZZ', 'PENRSZZ', 'PENACZZ', 'PENCVZZ',
    #                      'PENDEZZ', 'PENHPZZ', 'PPAFIZZ', 'PPEFGZZ',
    #                      'PPELGZZ', 'PPEPMZZ', 'PPEPRZZ', 'PPIPIZZ', 'PRRFIZZ']:
    #             result = True
    #             break
    #     return result
    #
    # def get_nb_items(self):
    #     fields = record.get_fields('995')
    #     return len(fields)
    #
    # def get_links(self):
    #     bnf_authnumbers = []
    #     koha_authnumbers = []
    #
    #     tags = [tag for tag in range(600, 610)]
    #     for tag in range(700, 704):
    #         tags.append(tag)
    #     for tag in range(710, 714):
    #         tags.append(tag)
    #
    #     for tag in tags:
    #         tag = str(tag)
    #         fields = record.get_fields(tag)
    #         for field in fields:
    #             numbers = field.get_subfields('3')
    #             for number in numbers:
    #                 bnf_authnumbers.append(number)
    #             numbers = field.get_subfields('9')
    #             for number in numbers:
    #                 koha_authnumbers.append(number)
    #     return [";".join(bnf_authnumbers), ";".join(koha_authnumbers)]

    # Fonctions de base

    def _get_marc_values(self, tags, aslist=False):
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
                result.append(self.record.leader)
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

    # def get_subfield_values(field, subfield_tag, values = None):
    #     result = None
    #     if hasattr(field, "subfields"):
    #         for subfield in field.subfields:
    #             if subfield.code == subfield_tag:
    #                 if values:
    #                     if subfield.value in values:
    #                         result = subfield.value
    #                 else:
    #                     result = subfield.value
    #     return result

import sqlite3
import re

'''
DatabaseLoader is a class created for BIOL4292: Programming and Databases for Biologists at the University of Glasgow.

Objectives:
1. DatabaseLoader keeps the SQLite connection to a database open while loading.
2. DatabaseLoader assumes CSV fields do not contain embedded commas inside quotes (since csv module is not used).
3. DatabaseLoader uses the following parsing rules:
    3.1. Subject.csv and metabolome_annotation.csv are comma-separated (',') files
    3.2. OmicsAbundance.tsv is a tab-separated ('\t') file
'''

class DatabaseLoader:
    def __init__(self, database_filename):
        self.con = sqlite3.connect(database_filename) # creates a connection to the database
        self.con.execute('PRAGMA foreign_keys = ON') # enables FK constraints
        self.cur = self.con.cursor()

    def commit(self):
        self.con.commit()

    def close(self):
        self.con.commit()
        self.con.close()

    '''
    define cleanup()

    - objective: replace empty strings, 'NA', or 'Unknown' values with None
    '''
    def cleanup(self, line):
        for col in range(len(line)):
            if line[col] == '' or re.match(r'^(NA|Unknown)$', line[col], re.IGNORECASE):
                line[col] = None
        return line

    '''
    define load_subject()

    - objective: load_subject() loads information about the study subjects into the Subject table.
    - note: the information includes SubjectID, Race, Sex, Age, BMI, SSPG, and IR_IS_classification
            (insulin-resistant (IR) or insulin-sensitive (IS) status).
    - assumptions:
        1. the columns in the Subject.csv file are in the following order:
           SubjectID, Race, Sex, Age, BMI, SSPG, IR_IS_classification.
    '''
    def load_subject(self, subject_filename):
        try: 
            with open(subject_filename) as file:
                header = file.readline().rstrip().split(',')  # split the header row into columns

                for line in file:
                    line = line.rstrip().split(',')

                    # ensure the correct number of columns for each line in Subject.csv
                    if len(line) != len(header):
                        continue

                    # iterate through each column in a line
                    # convert values indicated as 'NA' or 'Unknown' to None
                    line = self.cleanup(line)

                    # store the values for each column in the line under separate variable names
                    subject_ID, race, sex, age, bmi, sspg, ir_is = line

                    self.cur.execute(
                            "INSERT OR REPLACE INTO Subject "
                            "(SubjectID, Race, Sex, Age, BMI, SSPG, IR_IS_classification) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (subject_ID, race, sex, age, bmi, sspg, ir_is)
                    )
        # if the Subjects information file is not found, the user will be notified with a FileNotFoundError
        except FileNotFoundError: 
            raise FileNotFoundError(f'{subject_filename} not found')
    
    '''
    define load_omics()

    - objective: load_omics() loads
        1. Omics_IDs onto the Omics table
        2. SampleID, SubjectID, and VisitID onto the Sample table
        3. OmicsMeasurement onto the Measured relationship
    - note/constraints:
        - the word "Omics" can be transcriptomics, proteomics, or metabolomics, depending on the omics_abundance.tsv file.
        - some samples were not measured for all three omics types, so INSERT OR IGNORE is used instead of INSERT OR REPLACE
          when storing SampleID values (purpose 2).
    - assumptions:
        1. the columns in the omics_abundance.tsv file are in the following order:
           SampleID, Omics_ID1, Omics_ID2, ...
           where the header stores Omics IDs and the remaining rows store the corresponding measurement values.
    '''
    def load_omics(self, omics_filename):
        # determine whether the omics_abundance.tsv file contains transcriptomics, proteomics or metabolomics data
        # measurement_table, entity_table and entity_ID act as placeholders for the OmicsMeasurement table, Omics table and Omics_ID
        if re.search(r'proteom', omics_filename, re.IGNORECASE):
            measurement_table = 'ProteomeMeasurement'
            entity_table = 'Protein'
            entity_ID = 'ProteinID'
            '''
            'prot' is used as a keyword to ensure that the program recognises the omics type
            regardless of whether the filename is 'proteome' or 'proteomics'.
            '''
        elif re.search(r'transcriptom', omics_filename, re.IGNORECASE):
            measurement_table = 'TranscriptomeMeasurement'
            entity_table = 'Transcript'
            entity_ID = 'TranscriptID'
            '''
            'transcript' is used as a keyword to ensure that the program recognises the omics type
            regardless of whether the filename is 'transcriptome' or 'transcriptomics'.
            '''
        elif re.search(r'metabolom', omics_filename, re.IGNORECASE):
            measurement_table = 'MetabolomeMeasurement'
            entity_table = 'Peak'
            entity_ID = 'PeakID'
            '''
            'metabo' is used as a keyword to ensure that the program recognises the omics type
            regardless of whether the filename is 'metabolome' or 'metabolomics'.
            '''
        else:
            return

        # split the columns of omics_abundance.tsv into SampleID, Omics_IDs and measurements
        try: 
            with open(omics_filename) as file:
                header = file.readline().rstrip().split('\t')  # split the header row into columns
                omics_IDs = header[1:]  # Omics_ID starts from 2nd column onwards (1st column is SampleID)

                # part 1 of load_omics(): load Omics_ID onto the Omics table
                for ID in omics_IDs:
                    self.cur.execute(
                        f"INSERT OR REPLACE INTO {entity_table} ({entity_ID}) VALUES (?)",
                        (ID,)
                    )

                # loop through each line in omics_abundance.tsv
                for line in file:
                    line = line.rstrip().split('\t')

                    # ensure the same number of columns for each line in omics_abundance.tsv
                    if len(line) != len(header):
                        continue  # skip rows that do not have the same number of columns as the header

                    # parse SampleID column into its constituent SubjectID and VisitID components following the '<SubjectID>-<VisitID>' format
                    sampleID = line[0]
                    subjectID, visitID = sampleID.split("-", 1)

                    # part 2 of load_omics(): load parsed IDs (SampleID, SubjectID and VisitID) onto the Sample table
                    # INSERT OR IGNORE is used since some samples were not measured for all three omics types
                    self.cur.execute(
                        "INSERT OR IGNORE INTO Sample (SampleID, SubjectID, VisitID) "
                        "VALUES (?, ?, ?)",
                        (sampleID, subjectID, visitID)
                    )

                    # part 3 of load_omics(): load Omics IDs and Measurement onto the OmicsMeasurement table
                    omics_measurements = line[1:]

                    current_omics_ID = 0  # keep track of the Omics_ID
                    for measurement in omics_measurements:
                        omics_ID = omics_IDs[current_omics_ID]

                        self.cur.execute(
                            f"INSERT OR REPLACE INTO {measurement_table} (SampleID, {entity_ID}, Measurement) "
                            "VALUES (?, ?, ?)",
                            (sampleID, omics_ID, measurement)
                        )
                        current_omics_ID += 1  # move on to the next Omics_ID
        except FileNotFoundError:
            raise FileNotFoundError(f'{omics_filename} not found')
    '''
    define load_annotation()

    - objective: load_annotation() loads
        1. annotation information of each metabolite into the Metabolite table
        2. AnnotatedWith relationship between Peak and Metabolite onto the AnnotatedWith table
        3. PathwayName onto the Pathway table
        4. BelongedTo relationship between Metabolite and Pathway onto the BelongedTo table
    - note/constraints:
        - the information includes PeakID, MetaboliteName, KEGG, HMDB and ChemicalClass
        - some metabolites may be missing information for certain fields
        - some metabolites belong to >1 pathway and each pathway contains >1 metabolite
        - a peak may be linked to >1 compound and a compound may be linked to >1 peak
    - assumptions:
        1. the columns in the metabolome_annotation.csv file are in the following order:
           PeakID, MetaboliteName, KEGG, HMDB, ChemicalClass, Pathway
        2. purposes 3 and 4 are only performed if a metabolite belongs to at least one pathway
        3. metabolite_name is assumed to be non-null in the annotation file - there can never be a row with empty metabolite name
    '''
    def load_annotation(self, annotation_filename):
        try:
            with open(annotation_filename) as file:
                header = file.readline().rstrip().split(',')  # split the header row into columns

                for line in file:
                    line = line.rstrip().split(',')

                    # iterate through each column in a line and convert empty cell to None
                    line = self.cleanup(line)

                    # store the values from each column in the line under separate variable names
                    peak_ID, metabolite_name, kegg, hmdb, chemical_class, pathway = line

                    # data cleaning before loading values onto the table

                    # metabolite_name
                    individual_metabolites = metabolite_name.split('|') # case 1: a peak is linked to >1 metabolite

                    metabolites = []
                    for i in range(len(individual_metabolites)):
                        metabolite = individual_metabolites[i].strip()
                        metabolite = re.sub(r'\([1-5]\)$', '', metabolite) # case 2: a metabolite is linked to >1 peak
                        metabolites.append(metabolite)

                    # kegg and hmdb
                    keggs = None
                    hmdbs = None
                    if kegg is not None and re.search(r'\|', kegg):  # case 1: a peak is linked to >1 metabolite
                        keggs = [i.strip() for i in kegg.split('|')]
                    if hmdb is not None and re.search(r'\|', hmdb):  # case 1: a peak is linked to >1 metabolite
                        hmdbs = [i.strip() for i in hmdb.split('|')]

                    # pathway
                    pathways = None
                    if pathway is not None:
                        if re.search(r';', pathway):
                            pathways = pathway.split(';') # case 1: a metabolite can belong to >1 pathways
                        else:
                            pathways = [pathway] # case 2: a metabolite can belong to only 1 pathway
                    
                    # iterate through the lists (metabolites, keggs, hmdbs) created previously
                    i = 0
                    while i < len(metabolites):
                        met = metabolites[i].strip()

                        # align kegg/hmdb by index if lists exist, else reuse single value
                        if keggs is not None and i < len(keggs):
                            kegg_i = keggs[i]
                        else:
                            kegg_i = kegg

                        if hmdbs is not None and i < len(hmdbs):
                            hmdb_i = hmdbs[i]
                        else:
                            hmdb_i = hmdb

                        # part 1 of load_annotation(): load annotation information of each metabolite into the Metabolite table
                        self.cur.execute(
                            "INSERT OR REPLACE INTO Metabolite "
                            "(MetaboliteName, KEGG, HMDB, ChemicalClass) "
                            "VALUES (?, ?, ?, ?)",
                            (met, kegg_i, hmdb_i, chemical_class)
                        )

                        # part 2 of load_annotation(): load AnnotatedWith relationship between Peak and Metabolite onto AnnotatedWith table
                        self.cur.execute(
                            "INSERT OR REPLACE INTO AnnotatedWith (PeakID, MetaboliteName) VALUES (?, ?)",
                            (peak_ID, met)
                        )

                        # part 3 and part 4
                        # part 3 of load_annotation(): load PathwayName onto Pathway table
                        if pathways is not None:
                            j = 0
                            while j < len(pathways): # iterate through the list (pathways) created previously
                                pw = pathways[j].strip()

                                self.cur.execute(
                                    "INSERT OR REPLACE INTO Pathway (PathwayName) VALUES (?)",
                                    (pw,)
                                )

                                # part 4 of load_annotation(): load BelongedTo relationship between Metabolite and Pathway onto BelongedTo table
                                self.cur.execute(
                                    "INSERT OR REPLACE INTO BelongedTo (MetaboliteName, PathwayName) VALUES (?, ?)",
                                    (met, pw)
                                )

                                j += 1
                        i += 1
        except FileNotFoundError:
            raise FileNotFoundError(f'{annotation_filename} not found')
        
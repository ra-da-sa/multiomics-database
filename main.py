import argparse
import sqlite3
import sys
from database_creator import DatabaseCreator # import DatabaseCreator class from 3174555_database_creator.py
from database_loader import DatabaseLoader # import DatabaseLoader class from 3174555_database_loader.py

# ---------------------------------------------------
# set up an ArgumentParser (instantiating the object)
# ---------------------------------------------------
parser = argparse.ArgumentParser(
    description='This program creates and loads longitudinal multi-omics data into a database and executes predefined queries.\n'
                'Database creation, data loading, and query execution are controlled via command-line options.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

# add an argument parser
parser.add_argument('--createdb', action='store_true', help='Create the database structure') 
parser.add_argument('--loaddb', action='store_true', help='Parse the data files and insert the relevant data into the database')
parser.add_argument('--querydb', type=int, choices=range(1, 10), help='Run one of the predefined queries (1–9)')
# restricts the user to running predefined queries numbered 1 to 9
parser.add_argument('database', help='Specify the SQLite database file')

# call the parse_args() function and assign the output to a variable called args
args = parser.parse_args()

# python3 main.py --createdb 3174555S.db
# python3 main.py --loaddb 3174555S.db
# python3 main.py --querydb=1 3174555S.db

# -----------------
# create a database
# -----------------
# if --createddb is provided, DatabaseCreator reads a SQLite schema file and creates a database with the name specified by args.database
if args.createdb:
    try:
        creator = DatabaseCreator('3174555.sql', args.database)
        creator.create()
    # if the SQL schema file is not found, the user will be notified with an error message
    except FileNotFoundError: 
        print('Error: SQL schema file not found')
        sys.exit(1)

# -----------------
# load the database
# -----------------
# if --loaddb is provided, DatabaseLoader loads data onto the database
if args.loaddb:
    try: 
        loader = DatabaseLoader(args.database) # instantiating DatabaseLoader

        loader.load_subject('Subject.csv') # load subject information into the database
        loader.load_omics('HMP_proteome_abundance.tsv') # load proteomics abundance data into the database
        loader.load_omics('HMP_metabolome_abundance.tsv') # load metabolomics abundance data into the database
        loader.load_omics('HMP_transcriptome_abundance.tsv') # load transcriptomics abundance data into the database
        loader.load_annotation('HMP_metabolome_annotation.csv') # load metabolite annotation data and peak–metabolite relationships into the database

        loader.close() 
    except FileNotFoundError:
        print('Error: required input file(s) not found. Please check your working directory.')
        sys.exit(1)
        
# ------------------
# query the database
# ------------------
# --querydb=1
# Retrieve SubjectID and Age of subjects whose age is greater than 70.
if args.querydb == 1:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT SubjectID, Age
        FROM Subject
        WHERE Age > 70
        '''
    )

    results = cur.fetchall()

    # iterate over results to print the SubjectID and Age for each subject
    for subject_id, age in results: 
        print(f'{subject_id}\t{age}')

    con.close()

# --querydb=2
# Retrieve all female SubjectID who have a healthy BMI (18.5 to 24.9). Sort the results in descending order.
if args.querydb == 2:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT SubjectID
        FROM Subject
        WHERE Sex = 'F'
          AND BMI BETWEEN 18.5 AND 24.9
        ORDER BY BMI DESC 
        '''
    )

    results = cur.fetchall()

    # iterate over results to print the SubjectID of females with a healthy BMI
    # output is sorted by BMI in descending order
    for (subject_id,) in results:
        print(subject_id)

    con.close()

# --querydb=3
# Retrieve the Visit IDs of Subject 'ZNQOVZV'. 
if args.querydb == 3:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT VisitID
        FROM Sample
        WHERE SubjectID = 'ZNQOVZV'
        '''
    )

    results = cur.fetchall()

     # iterate over results to print the VisitID of Subject 'ZNQOVZV'.
    for (visit_id,) in results:
        print(visit_id)

    con.close()

# --querydb=4
# Retrieve distinct SubjectIDs who have metabolomics samples and are insulin-resistant.
if args.querydb == 4:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT DISTINCT s.SubjectID
        FROM Subject s
        JOIN Sample sa ON s.SubjectID = sa.SubjectID
        JOIN MetabolomeMeasurement mm ON sa.SampleID = mm.SampleID
        WHERE s.IR_IS_classification = 'IR'
        '''
    )

    results = cur.fetchall()

    # iterate through results to print the SubjectIDs of subjects who have metabolomics samples and are insulin-resistant
    for (subject_id,) in results:
        print(subject_id)

    con.close()

# --querydb=5
# Retrieve the unique KEGG IDs that have been annotated for the following peaks: 
#   a. 'nHILIC_121.0505_3.5'
#   b. 'nHILIC_130.0872_6.3'
#   c. 'nHILIC_133.0506_2.3'
#   d. 'nHILIC_133.0506_4.4'

if args.querydb == 5:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT DISTINCT m.KEGG
        FROM AnnotatedWith aw
        JOIN Metabolite m ON aw.MetaboliteName = m.MetaboliteName
        WHERE aw.PeakID IN (
            'nHILIC_121.0505_3.5',
            'nHILIC_130.0872_6.3',
            'nHILIC_133.0506_2.3',
            'nHILIC_133.0506_4.4'
        )
        AND m.KEGG IS NOT NULL
        '''
    )

    # iterate over results to print the unique KEGG IDs that have been annotated
    results = cur.fetchall()

    for (kegg_id,) in results:
        print(kegg_id)

    con.close()

# --querydb=6
# Retrieve the minimum, maximum and average age of Subjects.
if args.querydb == 6:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT MIN(Age), MAX(Age), AVG(Age)
        FROM Subject
        '''
    )

    result = cur.fetchone()

    min_age, max_age, avg_age = result

    # print the minimum, maximum and average age of Subjects
    print(f'{min_age}\t{max_age}\t{avg_age}')

    con.close()

# --querydb=7
# 7. Retrieve the list of pathways from the annotation data, and the count of how many times each pathway has been annotated. 
#    Display only pathways that have a count of at least 10. 
#    Order the results by the number of annotations in descending order.
if args.querydb == 7:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT PathwayName, COUNT(*) AS annotation_count
        FROM BelongedTo
        GROUP BY PathwayName
        HAVING COUNT(*) >= 10
        ORDER BY annotation_count DESC
        '''
    )

    results = cur.fetchall()

    # iterate over results to print each pathway name and the count of how many times it has been annotated
    # output is sorted in descending order by count
    for pathway, count in results:
        print(f'{pathway}\t{count}')

    con.close()

# --querydb=8
# Retrieve the maximum abundance of the transcript 'A1BG' for subject 'ZOZOW1T' across all samples.
if args.querydb == 8:
    con = sqlite3.connect(args.database)
    cur = con.cursor()

    cur.execute(
        '''
        SELECT MAX(tm.Measurement)
        FROM TranscriptomeMeasurement tm
        JOIN Sample s ON tm.SampleID = s.SampleID
        WHERE tm.TranscriptID = 'A1BG'
          AND s.SubjectID = 'ZOZOW1T'
        '''
    )

    result = cur.fetchone() # returns exactly one row of result

    max_abundance = result[0] # retrieve value of maximum abundance

    print(max_abundance) # print the maximum abundance

    con.close()

# --querydb=9
# 9. Retrieve the subjects’ age and BMI. If there are NULL values in the Age or BMI columns, that subject should be omitted from the results. 
#    At the same time, generate a scatter plot of age vs BMI using the query results from above. 
#    Store the resulting scatter plot as a PNG image file called ‘age_bmi_scatterplot.png’ in the same directory as your program.
if args.querydb == 9:
    import matplotlib.pyplot as plt

    con = sqlite3.connect(args.database)
    cur = con.cursor()

    # part 1: omit subjects where Age or BMI is NULL
    cur.execute(
        '''
        SELECT Age, BMI
        FROM Subject
        WHERE Age IS NOT NULL
          AND BMI IS NOT NULL
        '''
    )

    results = cur.fetchall() 
    con.close()

    # create lists that store age and BMI to iterate over in part 2 and 3
    ages = []
    bmis = [] 

    i = 0
    while i < len(results):
        age, bmi = results[i]
        ages.append(age)
        bmis.append(bmi)
        i += 1

    # part 2: iterate over results to print the subjects’ Age and BMI
    i = 0
    while i < len(results):
        age, bmi = results[i]
        print(f'{age}\t{bmi}')
        i += 1

    # part 3: generate a scatter plot of age vs BMI
    plt.figure()
    plt.scatter(ages, bmis, s=7) # size of each datapoint is set to 7pt to avoid 2 datapoints merging into 1 in the 
    plt.xlabel('Age')
    plt.ylabel('BMI')
    plt.title('Age vs BMI')

    # part 4: store 'age_bmi_scatterplot.png’ in the same directory as your program
    plt.savefig('age_bmi_scatterplot.png', dpi=300) # dpi is set to 300 to improve the image quality
    plt.close()

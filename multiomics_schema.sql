-- CREATE ALL THE TABLES AND CONSTRAINTS

-- Creates a table that corresponds to the SUBJECT entity
CREATE TABLE Subject (
	SubjectID CHAR(7) NOT NULL, 
	Race CHAR(1),
	Sex CHAR(1),
	Age DECIMAL (4,2), 
	BMI DECIMAL (4,2),
	SSPG DECIMAL, 
	IR_IS_classification CHAR(2),
	PRIMARY KEY (SubjectID)
);

-- Create a table that corresponds to the SAMPLE entity
-- The ASSAYED ON relationship is implicitly represented here through the FK to SUBJECT
CREATE TABLE Sample (
	SampleID VARCHAR(15) NOT NULL,
	SubjectID CHAR(7) NOT NULL,
    VisitID INT NOT NULL,
	PRIMARY KEY (SampleID),
	FOREIGN KEY (SubjectID) REFERENCES Subject(SubjectID)
);

-- Create a table that corresponds to the PROTEIN entity
CREATE TABLE Protein (
  ProteinID VARCHAR(15) NOT NULL,
  PRIMARY KEY (ProteinID)
);

-- Create a table that corresponds to the MEASURED relationship between proteins and samples
CREATE TABLE ProteomeMeasurement (
    SampleID VARCHAR(15) NOT NULL,
    ProteinID VARCHAR(15) NOT NULL,
    Measurement DECIMAL,
    PRIMARY KEY (SampleID, ProteinID),
    FOREIGN KEY (SampleID) REFERENCES Sample(SampleID),
    FOREIGN KEY (ProteinID) REFERENCES Protein(ProteinID)
);

-- Create a table that corresponds to the TRANSCRIPT entity
CREATE TABLE Transcript (
  TranscriptID VARCHAR(15) NOT NULL,
  PRIMARY KEY (TranscriptID)
);

-- Create a table that corresponds to the MEASURED relationship between transcripts and samples
CREATE TABLE TranscriptomeMeasurement (
    SampleID VARCHAR(15) NOT NULL,
    TranscriptID VARCHAR(15) NOT NULL,
    Measurement DECIMAL,
    PRIMARY KEY (SampleID, TranscriptID),
    FOREIGN KEY (SampleID) REFERENCES Sample(SampleID),
    FOREIGN KEY (TranscriptID) REFERENCES Transcript(TranscriptID)
);

-- Create a table that corresponds to the PEAK entity
CREATE TABLE Peak (
  PeakID VARCHAR(25) NOT NULL,
  PRIMARY KEY (PeakID)
);

-- Create a table that corresponds to the MEASURED relationship between peaks and samples
CREATE TABLE MetabolomeMeasurement (
	SampleID VARCHAR(15) NOT NULL,
	PeakID VARCHAR(25) NOT NULL,
	Measurement DECIMAL,
	PRIMARY KEY (SampleID, PeakID),
    FOREIGN KEY (PeakID) REFERENCES Peak(PeakID),
	FOREIGN KEY (SampleID) REFERENCES Sample(SampleID)
);

-- Create a table that corresponds to the METABOLITE entity
CREATE TABLE Metabolite (
	MetaboliteName VARCHAR(55) NOT NULL,
	KEGG CHAR(6),
	HMDB CHAR(9),
	ChemicalClass VARCHAR(25),
	PRIMARY KEY (MetaboliteName)
);

-- Create a table that corresponds to the ANNOTATED WITH relationship between PEAK and METABOLITE 
CREATE TABLE AnnotatedWith (
	PeakID VARCHAR(25) NOT NULL,
	MetaboliteName VARCHAR(55) NOT NULL,
	PRIMARY KEY (PeakID, MetaboliteName),
	FOREIGN KEY (PeakID) REFERENCES Peak(PeakID),
	FOREIGN KEY (MetaboliteName) REFERENCES Metabolite(MetaboliteName)
);

-- Create a table that corresponds to the PATHWAY entity
CREATE TABLE Pathway (
    PathwayName VARCHAR(55) NOT NULL,
    PRIMARY KEY (PathwayName)
);

-- Create a table that corresponds to the BELONGED TO relationship between METABOLITE and PATHWAY
CREATE TABLE BelongedTo (
	MetaboliteName VARCHAR(55) NOT NULL,
    PathwayName VARCHAR(55) NOT NULL,
	PRIMARY KEY (MetaboliteName, PathwayName),
	FOREIGN KEY (MetaboliteName) REFERENCES Metabolite(MetaboliteName),
    FOREIGN KEY (PathwayName) REFERENCES Pathway(PathwayName)
);

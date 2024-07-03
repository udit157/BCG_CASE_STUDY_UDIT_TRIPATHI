import os

# Function to create directories if they do not exist
def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def create_df(spark,charges_file,damages_file,endorsements_file,primary_person_file,restrict_file,units_file):
    # Load CSV files into Spark DataFrames
    charges = spark.read.csv(charges_file, header=True, inferSchema=True)
    damages = spark.read.csv(damages_file, header=True, inferSchema=True)
    endorsements = spark.read.csv(endorsements_file, header=True, inferSchema=True)
    primary_person = spark.read.csv(primary_person_file, header=True, inferSchema=True)
    restrict = spark.read.csv(restrict_file, header=True, inferSchema=True)
    units = spark.read.csv(units_file, header=True, inferSchema=True)
    return charges, damages, endorsements, primary_person, restrict, units
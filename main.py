import yaml
from Code.CrashAnalysis import VehicleCrashAnalyzer
from Code.utils import create_df
from Code.sparksession import create_spark_session

if __name__ == "__main__":
    spark = create_spark_session()
    # Load Configuration from Config.yaml
    with open(r"Config\Config.yaml", 'r') as stream:
        try:
            Config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            exit(1)

    # Extract input csv paths
    charges_file = Config['input_csv']['charges']
    damages_file = Config['input_csv']['damages']
    endorsements_file = Config['input_csv']['endorsements']
    primary_person_file = Config['input_csv']['primary_person']
    restrict_file = Config['input_csv']['restrict']
    units_file = Config['input_csv']['units']

    charges, damages, endorsements, primary_person, restrict, units = create_df(spark, charges_file, damages_file,
                                                                               endorsements_file, primary_person_file,
                                                                               restrict_file, units_file)

    # Output directory
    output_directory = Config['output_directory']

    # Create an instance of the VehicleCrashAnalyzer class
    analyzer = VehicleCrashAnalyzer(spark, charges, damages, endorsements, primary_person, restrict, units,
                                    output_directory)


    analyzer.analyze_crashes(analysis_number=1)
    analyzer.analyze_crashes(analysis_number=2)
    analyzer.analyze_crashes(analysis_number=3)
    analyzer.analyze_crashes(analysis_number=4)
    analyzer.analyze_crashes(analysis_number=5)
    analyzer.analyze_crashes(analysis_number=6)
    analyzer.analyze_crashes(analysis_number=7)
    analyzer.analyze_crashes(analysis_number=8)
    analyzer.analyze_crashes(analysis_number=9)
    analyzer.analyze_crashes(analysis_number=10)

    # Stop sparkSession
    spark.stop()
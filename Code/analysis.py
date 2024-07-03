from pyspark.sql.functions import col, count, sum, desc, dense_rank
from pyspark.sql.window import Window
import os
from Code.utils import create_directory_if_not_exists

def vehicle_crash_analysis(spark, charges, damages, endorsements,
                            primary_person, restrict, units, output_directory):
    
    # Create output directory
    create_directory_if_not_exists(output_directory)

    # Analysis 1: Number of crashes where number of males killed > 2
    num_crashes_with_male_fatalities = primary_person.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") > 2)) \
    .select("CRASH_ID") \
    .distinct() \
    .count()

    # Write Analysis 1 result to a file
    analysis_1_output_path = os.path.join(output_directory, "analysis_1_result.txt")
    with open(analysis_1_output_path, "w") as file:
        file.write(f"Analysis 1: Number of crashes where number of males killed > 2: {num_crashes_with_male_fatalities}\n")


    # Analysis 2: Count of two-wheelers booked for crashes
    num_two_wheelers_crash = units.filter(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%")) \
    .select("CRASH_ID") \
    .distinct() \
    .count()

    # Write Analysis 2 result to a file
    analysis_2_output_path = os.path.join(output_directory, "analysis_2_result.txt")
    with open(analysis_2_output_path, "w") as file:
        file.write(f"Analysis 2: Count of two-wheelers booked for crashes: {num_two_wheelers_crash}\n")


    # Analysis 3: Top 5 Vehicle Makes of cars where driver died and Airbags did not deploy
    window_spec = Window.partitionBy("VEH_MAKE_ID") \
       .orderBy(col("crash_count").desc())

    # Filter and count crashes where the driver died and airbags did not deploy
    top_5_makes = units.filter(col("VEH_MAKE_ID") != "NA") \
    .join(primary_person.filter((col("PRSN_TYPE_ID") == "DRIVER") & (col("DEATH_CNT") > 0) & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")), "CRASH_ID") \
    .groupBy("VEH_MAKE_ID") \
    .agg(count("CRASH_ID").alias("crash_count")) \
    .orderBy(desc("crash_count")) \
    .limit(5) \
    .select("VEH_MAKE_ID")

    # Collect results as list
    top_5_makes_list = top_5_makes.collect()

    # Write Analysis 3 result to a file
    analysis_3_output_path = os.path.join(output_directory, "analysis_3_result.txt")
    with open(analysis_3_output_path, "w") as file:
        file.write("Analysis 3: Top 5 Vehicle Makes of cars where driver died and Airbags did not deploy:\n")
        for row in top_5_makers_list:
            file.write(f"Vehicle Make ID: {row['VEH_MAKE_ID']}\n")


    # Analysis 4: Number of vehicles with valid licensed drivers involved in hit and run
    num_licensed_hit_and_run_vehicles = primary_person.filter((col("PRSN_TYPE_ID") == "DRIVER") & (col("DRVR_LIC_TYPE_ID").isNotNull())) \
    .join(units.filter(col("VEH_HNR_FL") == "Y"), "CRASH_ID") \
    .select("CRASH_ID") \
    .distinct() \
    .count()

    # Write Analysis 4 result to a file
    analysis_4_output_path = os.path.join(output_directory, "analysis_4_result.txt")
    with open(analysis_4_output_path, "w") as file:
        file.write(f"Analysis 4: Number of vehicles with valid licensed drivers involved in hit and run: {num_licensed_hit_and_run_vehicles}\n")


   # Analysis 5: State with highest number of accidents where females are not involved
    highest_num_accident_female = primary_person.filter((col("PRSN_GNDR_ID") != "FEMALE")) \
    .join(units, "CRASH_ID") \
    .groupBy("DRVR_LIC_STATE_ID") \
    .agg(count("CRASH_ID").alias("accident_count")) \
    .orderBy(desc("accident_count")) \
    .select("DRVR_LIC_STATE_ID") \
    .first()["DRVR_LIC_STATE_ID"]

    # Write Analysis 5 result to a file
    analysis_5_output_path = os.path.join(output_directory, "analysis_5_result.txt")
    with open(analysis_5_output_path, "w") as file:
        file.write(f"Analysis 5: State with highest number of accidents where females are not involved: {highest_num_accident_female}\n")


    # Analysis 6: Top 3rd to 5th VEH_MAKE_IDs contributing to largest number of injuries including death
    primary_person_alias = primary_person.alias("pp")
    units_alias = units.alias("u")

    # Analysis 6: Top 3rd to 5th VEH_MAKE_IDs contributing to the largest number of injuries including death
    window_spec_6 = Window.orderBy(desc("injury_count"))

    top_3_to_5_veh_make = primary_person_alias.filter(col("pp.TOT_INJRY_CNT") > 0) \
    .join(units_alias, col("pp.CRASH_ID") == col("u.CRASH_ID")) \
    .groupBy("u.VEH_MAKE_ID") \
    .agg(sum("pp.TOT_INJRY_CNT").alias("injury_count")) \
    .withColumn("rank", dense_rank().over(window_spec_6)) \
    .filter((col("rank") >= 3) & (col("rank") <= 5)) \
    .select("u.VEH_MAKE_ID", "injury_count")

    # Write Analysis 6 result to a file
    analysis_6_output_path = os.path.join(output_directory, "analysis_6_result.txt")
    top_3_to_5_veh_make_rows = top_3_to_5_veh_make.collect()
    with open(analysis_6_output_path, "w") as file:
        file.write("Analysis 6: Top 3rd to 5th VEH_MAKE_IDs contributing to largest number of injuries including death:\n")
        for row in top_3_to_5_veh_make_rows:
            file.write(f"Vehicle Make ID: {row['VEH_MAKE_ID']}, Injury Count: {row['injury_count']}\n")


    # Analysis 7: Top ethnic user group for each unique body style involved in crashes
    window_spec_7 = Window.partitionBy("VEH_BODY_STYL_ID") \
    .orderBy(desc("ethnic_count"))

    top_ethnic_group = primary_person.join(units, "CRASH_ID") \
    .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
    .agg(count("CRASH_ID").alias("ethnic_count")) \
    .withColumn("rank", dense_rank().over(window_spec_7)) \
    .filter(col("rank") == 1) \
    .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

    # Remove rows where VEH_BODY_STYL_ID is 'NA'
    top_ethnic_group = top_ethnic_group.filter(col("VEH_BODY_STYL_ID") != "NA")

    # Write Analysis 7 result to a file
    analysis_7_output_path = os.path.join(output_directory, "analysis_7_result.txt")
    top_ethnic_group_rows = top_ethnic_group.collect()
    with open(analysis_7_output_path, "w") as file:
       file.write("Analysis 7: Top ethnic user group for each unique body style involved in crashes:\n")
       for row in top_ethnic_group_rows:
          file.write(f"Body Style ID: {row['VEH_BODY_STYL_ID']}, Ethnicity ID: {row['PRSN_ETHNICITY_ID']}\n")

    # Analysis 8: Top 5 Zip Codes with highest number of crashes with alcohol as contributing factor
    top_5_zip_alcohol_crash = primary_person.join(units, "CRASH_ID") \
    .filter((col("CONTRIB_FACTR_1_ID").like("%ALCOHOL%")) | (col("CONTRIB_FACTR_2_ID").like("%ALCOHOL%"))) \
    .filter(col("DRVR_ZIP").isNotNull()) \
    .groupBy("DRVR_ZIP") \
    .agg(count("CRASH_ID").alias("alcohol_crash_count")) \
    .orderBy(desc("alcohol_crash_count")) \
    .select("DRVR_ZIP") \
    .limit(5)

    # Write Analysis 8 result to a file
    analysis_8_output_path = os.path.join(output_directory, "analysis_8_result.txt")
    top_5_zip_alcohol_crash_rows = top_5_zip_alcohol_crash.collect()

    with open(analysis_8_output_path, "w") as file:
        file.write("Analysis 8: Top 5 Zip Codes with highest number of crashes with alcohol as contributing factor:\n")
        for row in top_5_zip_alcohol_crash_rows:
            file.write(f"Zip Code: {row['DRVR_ZIP']}\n")



    # Analysis 9: Count of distinct Crash IDs where No Damaged Property observed, Damage Level > 4, and car avails Insurance
    num_crash_insurance = units.filter((col("VEH_DMAG_SCL_1_ID") > 4) & (col("FIN_RESP_PROOF_ID") == "Y")) \
    .join(damages.filter(col("DAMAGED_PROPERTY").isNull()), "CRASH_ID") \
    .select("CRASH_ID") \
    .distinct() \
    .count()



    # Write Analysis 9 result to a file
    analysis_9_output_path = os.path.join(output_directory, "analysis_9_result.txt")
    with open(analysis_9_output_path, "w") as file:
       file.write(f"Analysis 9: Count of distinct Crash IDs where No Damaged Property observed, Damage Level > 4, and car avails Insurance: {num_crash_insurance}\n")

   # Analysis 10: Top 5 Vehicle Makes where drivers are charged with speeding related offences, have licensed Drivers,
   # used top 10 used vehicle colours, and have vehicles licensed with the Top 25 states with highest number of offences

   # Get top 10 vehicle colors based on crash count
    top_10_colors = units.groupBy("VEH_COLOR_ID") \
    .agg(count("CRASH_ID").alias("color_count")) \
    .orderBy(desc("color_count")).limit(10) \
    .select("VEH_COLOR_ID")

   # Get top 25 states based on crash count
    top_25_states = primary_person.groupBy("DRVR_LIC_STATE_ID") \
    .agg(count("CRASH_ID").alias("state_count")) \
    .orderBy(desc("state_count")).limit(25) \
    .select("DRVR_LIC_STATE_ID")

    top_5_vehicles = charges.filter(col("CHARGE").like("%SPEEDING%")) \
    .join(primary_person.filter(col("DRVR_LIC_TYPE_ID").isNotNull()), "CRASH_ID") \
    .join(units, "CRASH_ID") \
    .join(top_10_colors, "VEH_COLOR_ID") \
    .join(top_25_states, "DRVR_LIC_STATE_ID") \
    .groupBy("VEH_MAKE_ID") \
    .agg(count("CRASH_ID").alias("make_count")) \
    .orderBy(desc("make_count")).limit(5) \
    .select("VEH_MAKE_ID", "make_count")

    # Write Analysis 10 result to a file
    analysis_10_output_path = os.path.join(output_directory, "analysis_10_result.txt")
    top_5_vehicles_rows = top_5_vehicles.collect()
    with open(analysis_10_output_path, "w") as file:
        file.write("Analysis 10: Top 5 Vehicle Makes where drivers are charged with speeding related offences, have licensed Drivers, use top 10 vehicle colors, and are licensed in the Top 25 states with highest number of offences:\n")
        for row in top_5_vehicles_rows:
            file.write(f"Vehicle Make ID: {row['VEH_MAKE_ID']}, Count: {row['make_count']}\n")


    # Stop sparkSession
    spark.stop()
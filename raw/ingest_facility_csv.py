from pathlib import Path
import pandas as pd
import psycopg2

# Import CSV

data_folder = Path("../source_csv/")
file_to_open = data_folder / "Assisted_Living_Facility_2024730_145340.csv"

data = pd.read_csv(file_to_open)
df = pd.DataFrame(data)

# Connect to SQL Server
conn = psycopg2.connect(
    database="caroldemo",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)

cursor = conn.cursor()

# Create Table
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS landing.assisted_living_facility_list
    (
        facility_id serial PRIMARY KEY,
        Facility VARCHAR(200) NOT NULL,
        City VARCHAR(50),
        County VARCHAR(50),
        Bed_Size INT,
        Number_of_Substantiated_Complaints INT,
        Sanctions_Final_Orders INT,
        Fine_Amount VARCHAR(50),
        Total_Deficiencies INT,
        Class_1 INT,
        Class_2 INT,
        Class_3 INT,
        Class_4 INT,
        Unclassified INT,
        Number_of_Activities VARCHAR(5),
        Activities VARCHAR(200),
        Number_of_Nurse_Availability VARCHAR(5),
        Nurse_Availability VARCHAR(300),
        Number_of_Special_Programs_and_Services VARCHAR(5),
        Special_Programs_and_Services VARCHAR(300),
        created_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
)

# Insert DataFrame to Table
# r=0
for row in df.itertuples():
    cursor.execute(
        """
            INSERT INTO landing.assisted_living_facility_list (
                facility,
                city,
                county,
                bed_size,
                number_of_substantiated_complaints,
                sanctions_final_orders,
                fine_amount,
                total_deficiencies,
                class_1,
                class_2,
                class_3,
                class_4,
                unclassified,
                number_of_activities,
                activities,
                number_of_nurse_availability,
                nurse_availability,
                number_of_special_programs_and_services,
                special_programs_and_services
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            row.Facility,
            row.City,
            row.County,
            row._4,
            row._5,
            row._6,
            row._7,
            row._8,
            row._9,
            row._10,
            row._11,
            row._12,
            row.Unclassified,
            row._14,
            row.Activities,
            row._16,
            row._17,
            row._18,
            row._19
        )
    )
conn.commit()
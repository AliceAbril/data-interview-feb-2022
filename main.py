import pandas as ps
import os
import zipfile
import findspark
findspark.init()

from pyspark.sql import SparkSession

# keep in memory map, simulating DB, to keep what has been saved already
in_memory_employees = dict()


def save_employee(employee):
    in_memory_employees[employee["employee_number"]] = [
        employee["employee_number"],
        employee["status"],
        employee["first_name"],
        employee["last_name"],
        employee["gender"],
        employee["email"],
        employee["phone_number"],
        employee["salary"],
        employee["termination_date"]
    ]


def init_session():
    return SparkSession.builder\
            .appName("interview")\
            .config("spark.master", "local")\
            .getOrCreate()


def process_file(spark):
    incoming_df = spark.read\
                    .option("header", "true")\
                    .csv("incoming_data/employee_data")

    # append to existing snapshot
    incoming_df.repartition(1).write.option("header", "true").csv("employees/employees_snapshot/", mode = 'append')

    # collect the results and save them to dict
    employees = incoming_df.collect()
    for e in employees: save_employee(e)


def save_employees_to_csv():
    # persist in_memory_employees to csv
    to_save = ps.DataFrame(in_memory_employees.items(),
                           columns=['employee_number','status','first_name','last_name',
                                    'gender','email','phone_number','salary','termination_date'])
    to_save.to_csv("employees/employees.csv")


def unzip_data():
    with zipfile.ZipFile('incoming_data/employee_data.zip') as z:
        for filename in z.namelist():
            if not os.path.isdir(filename):
                z.extract(filename, "incoming_data/")


if __name__ == "__main__":
    spark = init_session()
    unzip_data()
    process_file(spark)
    save_employees_to_csv()
import os
import random
import pandas as pd

from airflow.decorators import task, dag
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta


# Define file paths
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SOURCE_FILE = os.path.join(DATA_DIR, "students.csv")
PROCESSED_FILE = f"{DATA_DIR}/processed_students.csv"
STATE_FILE = f"{DATA_DIR}/state.json"

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="student_data_processing",
    default_args=default_args,
    description="Generate and process student data",
    start_date=datetime(2025, 7, 14),
    schedule=timedelta(seconds=5),
    catchup=False,
)
def student_data_processing_dag():

    @task
    def generate_fake_students(num_students: int = 1):
        """Generate fake student data and append to CSV file"""
        FIRST_NAMES = [
            "James",
            "Mary",
            "John",
            "Patricia",
            "Robert",
            "Jennifer",
            "Michael",
            "Linda",
            "William",
            "Elizabeth",
        ]
        LAST_NAMES = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
        ]

        # Create new student records
        new_students = []
        for _ in range(num_students):
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            student = {
                "student_id": f"S{random.randint(10000, 99999)}",
                "first_name": first_name,
                "last_name": last_name,
                "major": random.choice(["Computer Science", "Data Science", "AI", "Business", "Engineering"]),
                "gpa": round(random.uniform(2.0, 4.0), 2),
                "email": f"{first_name.lower()}.{last_name.lower()}{random.randint(80, 99)}@university.edu",
                "processed": False,
            }
            new_students.append(student)

        # Load existing data or create new dataframe
        if os.path.exists(SOURCE_FILE):
            df = pd.read_csv(SOURCE_FILE)
            new_df = pd.DataFrame(new_students)
            df = pd.concat([df, new_df], ignore_index=True)
        else:
            df = pd.DataFrame(new_students)

        # Save to CSV
        df.to_csv(SOURCE_FILE, index=False)
        print(f"Generated {num_students} new student records")

        total_students = len(df)
        # Push the count of new records
        return total_students

    @task.short_circuit
    def check_if_3_new_students(total_students):
        return total_students % 3 == 0

    @task
    def process_student_data():
        if not os.path.exists(SOURCE_FILE):
            print("No student data to process.")
            return

        df = pd.read_csv(SOURCE_FILE)

        unprocessed = df[df["processed"] == False]
        num_unprocessed = len(unprocessed)

        if num_unprocessed < 3:
            print(f"Only {num_unprocessed} unprocessed records. Need at least 3 to process.")
            return

        # Process records in batches of 3
        batches_to_process = num_unprocessed // 3
        processed_indices = []
        for batch in range(batches_to_process):
            start_idx = batch * 3
            end_idx = start_idx + 3
            batch_df = unprocessed.iloc[start_idx:end_idx]

            origin_indices = batch_df.index.tolist()
            processed_indices.extend(origin_indices)

            for idx in origin_indices:
                student = df.iloc[idx]
                df.at[idx, "summary"] = (
                    f"{student['first_name']} {student['last_name']} - {student['major']} - GPA: {student['gpa']} - Email: {student['email']}"
                )
                df.at[idx, "processed"] = True

        # Save updated data
        df.drop(columns=["summary"]).to_csv(SOURCE_FILE, index=False)

        # Save processed records to a separate file (append mode)
        processed_df = df.loc[processed_indices]
        processed_df = processed_df[["student_id", "summary"]]

        if os.path.exists(PROCESSED_FILE):
            existing_processed = pd.read_csv(PROCESSED_FILE)
            all_processed = pd.concat([existing_processed, processed_df], ignore_index=True)
            all_processed.to_csv(PROCESSED_FILE, index=False)
        else:
            processed_df.to_csv(PROCESSED_FILE, index=False)

        print(f"Processed {batches_to_process * 3} student records in {batches_to_process} batches")

    chain(
        check_if_3_new_students(generate_fake_students(num_students=1)),
        process_student_data(),
    )


student_data_processing_dag()

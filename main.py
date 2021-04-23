import mysql.connector
from mysql.connector import Error
import pandas as pd
from passcode import pw

# connecting to MYSQL server
def create_connection(hostname, username, passcode):
    connection = None # closes any exisiting connections so that the server doesn't become confused
    try:
        connection = mysql.connector.connect(
            host = hostname,
            user = username,
            passwd = passcode
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

db = "school"

connection = create_connection("localhost", "root", pw)

# create a new database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("DATABASE CREATED SUCCESSFULLY!")
    except Error as err:
        print(f"Error: '{err}'")

create_database_query = "CREATE DATABASE school"
create_database(connection, create_database_query)

# modify create_connection function to connect directly to this database
def create_database_connection(hostname, username, passcode, db_name):
    connection = None # closes any exisiting connections so that the server doesn't become confused
    try:
        connection = mysql.connector.connect(
            host = hostname,
            user = username,
            passwd = passcode,
            database = db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

# function that will execute query
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("QUERY SUCCESSFUL")
    except Error as err:
        print(f"Error: '{err}'")


# create tables
create_teacher_table = """
CREATE TABLE teacher (
    teacher_id INT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    course_one VARCHAR(3) NOT NULL,
    course_two VARCHAR(3),
    dob DATE,
    tax_id INT UNIQUE,
    phone_number VARCHAR(20)
);
"""

connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, create_teacher_table)

create_client_table = """
CREATE TABLE client(
    client_id INT PRIMARY KEY,
    client_name VARCHAR(40) NOT NULL,
    address VARCHAR(60) NOT NULL,
    industry VARCHAR(20)
);
"""

create_participant_table = """
CREATE TABLE participant(
    participant_id INT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    phone_number VARCHAR(20),
    client INT
);
"""

create_course_table = """
CREATE TABLE course (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(40) NOT NULL,
    language VARCHAR(3) NOT NULL,
    level VARCHAR(2),
    course_length_weeks INT,
    start_date DATE,
    in_school BOOLEAN,
    teacher INT,
    client INT
);
"""

connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, create_client_table)
execute_query(connection, create_participant_table)
execute_query(connection, create_course_table)

alter_participant = """
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL
"""
alter_course = """
ALTER TABLE course
ADD FOREIGN KEY(teacher)
REFERENCES client(teacher_id)
ON DELETE SET NULL
"""
alter_course_again = """
ALTER TABLE course
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL
"""

create_takescourse_table = """
CREATE TABLE takes_course (
    participant_id INT,
    course_id INT,
    PRIMARY KEY(participant_id, course_id),
    FOREIGN KEY(participant_id) REFERENCES participant(participant_id) ON DELETE CASCADE,
    FOREIGN KEY (course_id) REFERENCES course(course_id) ON DELETE CASCADE
);
"""

connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, alter_participant)
execute_query(connection, alter_course)
execute_query(connection, alter_course_again)
execute_query(connection, alter_course_again)
execute_query(connection, create_takescourse_table)

# populate the tables
populate_teacher = """
INSERT INTO teacher VALUES
(1, 'Sam', 'Wilson', 'GER', NULL, '1980-10-20', 12345, '+9095555555'),
(2, 'Bucky', 'Barnes', 'RUS', NULL, '1940-05-04', 67890, '+9095555234'),
(3, 'Steve', 'Rogers', 'SPA', NULL, '1950-02-12', 34567, '+9093945555')
"""

connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, populate_teacher)

populate_client = """
INSERT INTO client VALUES
(101, 'Apple Inc', 'One Apple Park Way CA', 'Tech'),
(102, 'Microsoft', 'Mountain View CA', 'Tech'),
(103, 'IBM', 'Armonk NY', 'Tech')
"""

populate_participant = """
INSERT INTO participant VALUES
(101, 'Nick', 'Fury', '4915555555', 101),
(102, 'Maria', 'Hill', '4915554567', 102),
(103, 'Thor', 'Odinson', '4914325555', 101)
"""

populate_course = """
INSERT INTO course VALUES
(12, 'Deutsch 1', 'GER', 'A1', 10, '2020-01-15', TRUE, 3, 103),
(13, 'Russian', 'RUS', 'B1', 12, '2020-04-23', FALSE, 5, 101),
(14, 'Spanish', 'SPA', 'C1', 8, '2020-05-20', TRUE, 2, 101)
"""

populate_takescourse = """
INSERT INTO takes_course VALUES
(101, 15),
(101, 17),
(102, 17)
"""
connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, populate_client)
execute_query(connection, populate_participant)
execute_query(connection, populate_course)
execute_query(connection, populate_takescourse)

# reading data: no changes to be made.

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


# try it out with a simple query
# query1 = """
# SELECT * FROM teacher;
# """

# connection = create_database_connection("localhost", "root", pw, db)
# results = read_query(connection, query1)

# for result in results:
#     print(result)


# try it out with a JOIN

query5 = """
SELECT course.course_id, course.course_name, course.language, client.client_name, client.address
FROM course
JOIN client
ON course.client = client.client_id
WHERE course.in_school = FALSE;
"""

connection = create_database_connection("localhost", "root", pw, db)
results = read_query(connection, query5)

for result in results:
    print(result)

# formatting output into a list

# from_db = []

# for result in results:
#     result = result
#     from_db.append(result)

# print(from_db)

# formatting output into a pandas dataframe

# from_db = []

# for result in results:
#     result = list(result)
#     from_db.append(result)

# columns = ["course_id", "course_name", "language", "client_name", "address"]
# df = pd.DataFrame(from_db, columns=columns)

# print(df)

# updating records

update = """
UPDATE client
SET address = '107 Briston Private, Ottawa'
WHERE client_id = 101;
"""
connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, update)

# deleting records

delete_course = """
DELETE FROM course
WHERE course_id = 14;
"""

connection = create_database_connection("localhost", "root", pw, db)
execute_query(connection, delete_course)
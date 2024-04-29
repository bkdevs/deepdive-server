from deepdive.gpt import GptClient
from deepdive.gpt.models.helper import parse_sql
from deepdive.gpt.open_ai import complete_prompt
from deepdive.gpt.prompts import writer
from deepdive.schema import DatabaseSchema

EXAMPLE_TABLES = """
Table advisor, columns = [*,s_ID,i_ID]
Table classroom, columns = [*,building,room_number,capacity]
Table course, columns = [*,course_id,title,dept_name,credits]
Table department, columns = [*,dept_name,building,budget]
Table instructor, columns = [*,ID,name,dept_name,salary]
Table prereq, columns = [*,course_id,prereq_id]
Table section, columns = [*,course_id,sec_id,semester,year,building,room_number,time_slot_id]
Table student, columns = [*,ID,name,dept_name,tot_cred]
Table takes, columns = [*,ID,course_id,sec_id,semester,year,grade]
Table teaches, columns = [*,ID,course_id,sec_id,semester,year]
Table time_slot, columns = [*,time_slot_id,day,start_hr,start_min,end_hr,end_min]
Foreign_keys = [course.dept_name = department.dept_name,instructor.dept_name = department.dept_name,section.building = classroom.building,section.room_number = classroom.room_number,section.course_id = course.course_id,teaches.ID = instructor.ID,teaches.course_id = section.course_id,teaches.sec_id = section.sec_id,teaches.semester = section.semester,teaches.year = section.year,student.dept_name = department.dept_name,takes.ID = student.ID,takes.course_id = section.course_id,takes.sec_id = section.sec_id,takes.semester = section.semester,takes.year = section.year,advisor.s_ID = student.ID,advisor.i_ID = instructor.ID,prereq.prereq_id = course.course_id,prereq.course_id = course.course_id]
"""

EXAMPLE_QUERIES = """
Q: "Find the buildings which have rooms with capacity more than 50."
SQL: SELECT DISTINCT building FROM classroom WHERE capacity > 50

Q: "Find the room number of the rooms which can sit 50 to 100 students and their buildings."
SQL: SELECT building , room_number FROM classroom WHERE capacity BETWEEN 50 AND 100

Q: "Give the name of the student in the History department with the most credits."
SQL: SELECT name FROM student WHERE dept_name = ’History’ ORDER BY tot_cred DESC LIMIT 1

Q: "Find the total budgets of the Marketing or Finance department."
SQL: SELECT sum(budget) FROM department WHERE dept_name = ’Marketing’ OR dept_name = ’Finance’

Q: "Find the department name of the instructor whose name contains ’Soisalon’."
SQL: SELECT dept_name FROM instructor WHERE name LIKE ’%Soisalon%’

Q: "What is the name of the department with the most credits?"
SQL: SELECT dept_name FROM course GROUP BY dept_name ORDER BY sum(credits) DESC LIMIT 1

Q: "How many instructors teach a course in the Spring of 2010?"
SQL: SELECT COUNT (DISTINCT ID) FROM teaches WHERE semester = ’Spring’ AND YEAR = 2010

Q: "Find the name of the students and their department names sorted by their total credits in ascending order."
SQL: SELECT name , dept_name FROM student ORDER BY tot_cred

Q: "Find the year which offers the largest number of courses."
SQL: SELECT YEAR FROM SECTION GROUP BY YEAR ORDER BY count(*) DESC LIMIT 1

Q: "What are the names and average salaries for departments with average salary higher than 42000?"
SQL: SELECT dept_name , AVG (salary) FROM instructor GROUP BY dept_name HAVING AVG (salary) > 42000

Q: "How many rooms in each building have a capacity of over 50?"
SQL: SELECT count(*) , building FROM classroom WHERE capacity > 50 GROUP BY building

Q: "Find the names of the top 3 departments that provide the largest amount of courses?"
SQL: SELECT dept_name FROM course GROUP BY dept_name ORDER BY count(*) DESC LIMIT 3

Q: "Find the maximum and average capacity among rooms in each building."
SQL: SELECT max(capacity) , avg(capacity) , building FROM classroom GROUP BY building

Q: "Find the title of the course that is offered by more than one department."
SQL: SELECT title FROM course GROUP BY title HAVING count(*) > 1

Q: "Find the total budgets of the Marketing or Finance department."
SQL: SELECT sum(budget) FROM department WHERE dept_name = ’Marketing’ OR dept_name = ’Finance’

Q: "Find the name and building of the department with the highest budget."
SQL: SELECT dept_name , building FROM department ORDER BY budget DESC LIMIT 1

Q: "What is the name and building of the departments whose budget is more than the average budget?"
SQL: SELECT dept_name , building FROM department WHERE budget > (SELECT avg(budget) FROM department)

Q: "Find the total number of students and total number of instructors for each department."
SQL: SELECT count(DISTINCT T2.id) , count(DISTINCT T3.id) , T3.dept_name FROM department AS T1 JOIN student AS T2 ON T1.dept_name = T2.dept_name JOIN instructor AS T3 ON T1.dept_name = T3.dept_name GROUP BY T3.dept_name

Q: "Find the title of courses that have two prerequisites?"
SQL: SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id = T2.course_id GROUP BY T2.course_id HAVING count(*) = 2

Q: "Find the name of students who took any class in the years of 2009 and 2010."
SQL: SELECT DISTINCT T1.name FROM student AS T1 JOIN takes AS T2 ON T1.id = T2.id WHERE T2.YEAR = 2009 OR T2.YEAR = 2010

Q: "list in alphabetic order all course names and their instructors’ names in year 2008."
SQL: SELECT T1.title , T3.name FROM course AS T1 JOIN teaches AS T2 ON T1.course_id = T2.course_id JOIN instructor AS T3 ON T2.id = T3.id WHERE T2.YEAR = 2008 ORDER BY T1.title

Q: "Find the title of courses that have two prerequisites?"
SQL: SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id = T2.course_id GROUP BY T2.course_id HAVING count(*) = 2

Q: "Find the name and building of the department with the highest budget."
SQL: SELECT dept_name , building FROM department ORDER BY budget DESC LIMIT 1

Q: "Find the title, credit, and department name of courses that have more than one prerequisites?"
SQL: SELECT T1.title , T1.credits , T1.dept_name FROM course AS T1 JOIN prereq AS T2 ON T1.course_id = T2.course_id GROUP BY T2.course_id HAVING count(*) > 1

Q: "Give the name and building of the departments with greater than average budget."
SQL: SELECT dept_name , building FROM department WHERE budget > (SELECT avg(budget) FROM department)

Q: "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010."
SQL: SELECT id FROM teaches WHERE semester = ’Fall’ AND YEAR = 2009 EXCEPT SELECT id FROM teaches WHERE semester = ’Spring’ AND YEAR = 2010

Q: "Find the name of the courses that do not have any prerequisite?"
SQL: SELECT title FROM course WHERE course_id NOT IN (SELECT course_id FROM prereq)

Q: "Find the salaries of all distinct instructors that are less than the largest salary."
SQL: SELECT DISTINCT salary FROM instructor WHERE salary < (SELECT max(salary) FROM instructor)

Q: "Find the names of students who have taken any course in the fall semester of year 2003."
SQL: SELECT name FROM student WHERE id IN (SELECT id FROM takes WHERE semester = ’Fall’ AND YEAR = 2003)

Q: "Find the minimum salary for the departments whose average salary is above the average payment of all instructors."
SQL: SELECT min(salary) , dept_name FROM instructor GROUP BY dept_name HAVING avg(salary) > (SELECT avg(salary) FROM instructor)

Q: "What is the course title of the prerequisite of course Mobile Computing?"
SQL: SELECT title FROM course WHERE course_id IN (SELECT T1.prereq_id FROM prereq AS T1 JOIN course AS T2 ON T1.course_id = T2.course_id WHERE T2.title = ’Mobile Computing’)

Q: "Give the title and credits for the course that is taught in the classroom with the greatest capacity."
SQL: SELECT T3.title , T3.credits FROM classroom AS T1 JOIN SECTION AS T2 ON T1.building = T2.building AND T1.room_number = T2.room_number JOIN course AS T3 ON T2.course_id = T3.course_id WHERE T1.capacity = (SELECT max(capacity) FROM classroom)
"""


def construct_prompt_header(db_schema: DatabaseSchema) -> str:
    prompt = "# Create SQL queries for the given questions.\n"
    prompt += EXAMPLE_TABLES + "\n"
    prompt += writer.write_db(db_schema)
    prompt += EXAMPLE_QUERIES
    prompt += "\n"

    return prompt


def construct_prompt(prompt_header: str, message: str) -> str:
    prompt = prompt_header
    prompt += f'Q: "{message}"\n'
    prompt += "SQL:"
    return prompt


class FewShotGPT(GptClient):
    """
    Few-shot GPT text to SQL model, prompts based on: https://arxiv.org/pdf/2304.11015.pdf#page=10&zoom=100,401,165
    """

    def __init__(self, model, db_schema: DatabaseSchema):
        self.model = model
        self.prompt_header = construct_prompt_header(db_schema)

    def construct_query(self, question: str, example_queries: str = "") -> str:
        prompt = construct_prompt(self.prompt_header, question)
        if self.model == "gpt-3.5-turbo":
            response = complete_prompt(
                prompt, model=self.model, temperature=0, max_tokens=600, stop=["Q:"]
            )
            return parse_sql(response)
        else:
            raise Exception("Model not supported: " + self.model)

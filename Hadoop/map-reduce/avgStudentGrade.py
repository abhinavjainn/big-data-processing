'''
Identify the 100 all-time, best-performing graduates. 
Students need to be ranked using the average grade they got across all of the modules they have taken. 
Students who are yet to graduate will not be considered.
File columns: acaedemic year; student id; university id; module; grade; attemptNum; hasGraduated
'''

import statistics

def mapper1(self,_,_line):
	fields = line.split(";")
	studentID = fields[1]
	moduleID = fields[3]
	grade = fields[4]
	hasGraduated = fields[6]

	if hasGraduated == True:
		yield(studentID,grade)

def reducer1(self,studentID,grade):
	avgGrade = statistics.mean(grade)
	yield(studentID,avgGrade)


def mapper2(self,studentID, avgGrade):
	yield(None,(studentID,avgGrade))


def reducer2(self,_,values):
    sorted_values = sorted(values, reverse=True, key=lambda x: x[0])[:100]
    key = 0
    for i in sorted_values:
        key+=1
        yield(key,('{}-{}'.format(float(i[0]),str(i[1]).strip())))

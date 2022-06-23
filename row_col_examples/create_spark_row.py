from pyspark.sql import Row


row=Row("James",40)
print(row[0] +","+str(row[1]))


row=Row(name="Alice", age=11)
print(row.name, row.age)


Person = Row("name", "age")
p1=Person("James", 40)
p2=Person("Alice", 35)
print(p1.name +","+p2.name)



# For Automation purposes / running sql queries in SmallSQL
import jaydebeapi
from sql_metadata import Parser
count_dict = {}
from collections import Counter

conn = jaydebeapi.connect("smallsql.database.SSDriver",
                          "jdbc:smallsql:MyDb?create=true",
                          ["", ""],
                          "/Users/niksrid/Downloads/smallsql0.21_lib/smallsql.jar",)

curs = conn.cursor()
curs.execute('USE MyDb')
# curs.execute('DROP VIEW FirstView')

# Init Part 
# with open('init_TableThree.txt') as f:
#     line = f.readline()
#     while line:
#         line = f.readline()
#         curs = conn.cursor()
#         curs.execute(line)
#         curs.close()

# Select part
with open('selects.txt') as f:
    line = f.readline()
    while line:
        line = f.readline()
        join = '|'.join(Parser(line).tables)
        if(count_dict.get(join)):
            count_dict[join] += 1
        else:
            count_dict[join] =  1
        curs.close()


sorted_dict = ({k: v for k, v in sorted(count_dict.items(), key=lambda item: item[1])})
queries = []
for item in sorted_dict.items():
    views = ', '.join(item[0].split('|'))
    if(views == ''):
        continue
    query = '''CREATE VIEW {view_name} AS SELECT * FROM {table_split}'''.format(view_name = "MaterializedView", table_split = views)
    queries.append(query)


# print("The has seen tables {table_split} { times each and generated the following".format(table_split = table_split, n = n))
print("The program recommends the following views: ")
print("-------------------------------------------")
for query in queries:
    print(query)

# Store the view if required
# curs = conn.cursor()
# curs.execute(gen_query)

# Fetch data from view

# curs.execute('SELECT * FROM FirstView')
# print(curs.fetchall())


curs.close()
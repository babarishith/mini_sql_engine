import numpy as np
import sqlparse
import sql_database
import sys
import re

database = sql_database.Database(name="db", metadata="metadata.txt")

def main():
	database.load_data()
	query = str(sys.argv[1])
	process(query)

def process(query):
	parsed = sqlparse.parse(query)[0].tokens
	flag_where = False
	flag_op = "none"

	columns = parsed[2]
	if "," not in str(columns):
		if str(columns) == "*":
			col_list = ['*']
		else:
			col_list = [str(columns)]
	else:
		col_list = [str(i) for i in columns.get_identifiers()]

	tables = parsed[6]
	if "," not in str(tables):
		if str(tables) == "*":
			tab_list = ['*']
		else:
			tab_list = [str(tables)]
	else:
		tab_list = [str(i) for i in tables.get_identifiers()]

	if "where" in str(parsed):
		flag_where = True
		conditions = parsed[8]
		if all(map(lambda x: x not in str(conditions).lower(), ['and','or'])):
			cond_list = [[str(i) for i in conditions[2].tokens if str(i) != " "]]
		else:
			if "and" in str(conditions).lower():
				flag_op = "and"
			else:
				flag_op = "or"
			cond_tokens = conditions.tokens
			conds = [cond_tokens[2], cond_tokens[6]]
			cond_list = []
			for j in conds:
				cond_list.append([str(i) for i in j.tokens if str(i) != " "])

	# print col_list, tab_list
	# if flag_where:
	# 	print cond_list
	t_names = database.t_names
	for i in tab_list:
		if i not in t_names:
			print "Error: %s does not exist" %i
			sys.exit()

	table1 = getattr(database, t_names[0])
	table2 = getattr(database, t_names[1])

	t1 = table1.data
	t2 = table2.data

	merged = t1.merge(t2, how='inner')
	# print merged

	if len(tab_list) == 1:
		table = getattr(database, tab_list[0]).data
		if flag_where == True:
			if flag_op == "none":
				table = table.query(" ".join(cond_list[0]))
			elif flag_op == "and":
				table = table.query((" ".join(cond_list[0])) + " & " + (" ".join(cond_list[1])))
			else:
				table = table.query((" ".join(cond_list[0])) + " | " + (" ".join(cond_list[1])))
		if len(col_list) == 1 and col_list[0] == "*":
			print table
		elif "(" not in col_list[0]:
			print table[col_list]
		else:
			cols = []
			funcs = []
			for i in col_list:
				if "(" not in i:
					print "Error: All columns should have aggregator functions"
					sys.exit()
				val = re.sub(ur"[\(\)]",' ',i).split(" ")
				cols.append(val[1])
				if val[0] == "distinct":
					funcs.append("unique")
				elif val[0] == "average":
					funcs.append("mean")
				else:
					funcs.append(val[0].lower())
			print "\t".join(col_list)
			if "unique" in funcs:
				if len(funcs) == 2:
					a = getattr(table[cols[0]], funcs[0])()
					b = getattr(table[cols[1]], funcs[1])()
					try:
						len(a)
					except:
						a = [a]
					try:
						len(b)
					except:
						b = [b]
					for i in b:
						for j in a:
							print j,i
				else:
					a = getattr(table[cols[0]], funcs[0])()
					for i in a:
						print i
			else:
				for i in range(len(cols)):
					print getattr(table[cols[i]], funcs[i])(),"\t",
				print
	else:
		if flag_where == True:
			a = cond_list[0][0].split(".")[1]
			b = cond_list[0][2].split(".")[1]
			if (a == b) and a in table1.attributes and a in table2.attributes:
				if len(col_list) == 1 and col_list[0] == "*":
					print merged
				else: 
					print merged[col_list]
			else:
				print "Error: join can occur only on columns with same name in both tables"
		else:
			if col_list[0] in table1.attributes and col_list[1] in table2.attributes:
				a = t1[col_list[0]]
				b = t2[col_list[1]]
				print "table1",col_list[0],",","table2", col_list[1]
				for i in b:
					for j in a:
						print i,j
			else:
				a = t2[col_list[0]]
				b = t1[col_list[1]]
				print "table2",col_list[0],",","table1", col_list[1]
				for i in b:
					for j in a:
						print i,j



if __name__ == "__main__":
	main()
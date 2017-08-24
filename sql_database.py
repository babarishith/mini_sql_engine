import numpy as np
import pandas as pd
import sqlparse
import sys

class Table:
	def __init__(self, name, attributes):
		self.name = name
		self.attributes = attributes
		self.data = pd.read_csv("%s.csv" %self.name, names=attributes)

class  Database(object):
	def __init__(self, name, metadata):
		self.name = name
		self.metadata = metadata
		

	def get_table_scheme(self,file):
		final = []
		# file = "metadata.txt"
		schema = open(file, 'r')
		ischema = iter(schema)
		state = 0
		for line in ischema:
			line = line.strip()
			if line == "<begin_table>":
				name = next(ischema).strip()
				attributes = []
				new = next(ischema).strip()
				while new != "<end_table>":
					attributes.append(new)
					new = next(ischema).strip()
				final.append([name, attributes])
		return final

	def load_data(self):
		schemas = self.get_table_scheme(self.metadata)
		self.table1 = Table(schemas[0][0], schemas[0][1])
		self.table2 = Table(schemas[1][0], schemas[1][1])
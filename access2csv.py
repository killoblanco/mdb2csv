"""
Python3 Class

$ ⚡️ python access2csv.py "path/to/database" "TABLE_NAME" "db/output/name"
"""
import sys, time
import pandas as pd
import pandas_access as mdb


"""
@class access2csv
@param {String} db         - Database file path #./databaseName.mdb
@param {String} table      - Table name # "U_ID"
@param {String} outputName - Output file name # "databaseName"

@constructor {db, table, outputName}: @params

@return {Function} execution_time - Return execution timestamp # HH:MM:SS
"""
class access2csv:
	def __init__(self, db, table, outputName):
		self.chunk_size = 10000
		self.db = db
		self.data = []
		self.output = '%s.csv' % outputName
		self.table = table
		self.start_timestamp = time.time()

	def execution_time(self):
		hours, rem = divmod(time.time() - self.start_timestamp, 3600)
		minu, sec = divmod(rem, 60)
		print("{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minu), sec))

	def main(self):
		# Read database by groups of 10000
		for chunk in mdb.read_table(self.db, self.table, chunksize=self.chunk_size):
			self.data.append(chunk)

		pd.concat(self.data).to_csv(self.output, index=False)
		# Print execution time
		self.execution_time()


if __name__ == '__main__':
	a2c = access2csv(*sys.argv[1:])
	a2c.main()

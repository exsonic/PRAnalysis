"""
Bobi Pu, bobi.pu@usc.edu
Copyrights belongs to USC Marshall Business School.
"""

import csv
from DBController import DBController


def loadCompeletedCodingFile(filePath):
	db = DBController()
	with open(filePath, 'rU') as f:
		reader = csv.reader(f)
		keyList = ['_id', 'OC_ID', 'OUTCOME_ID', 'CAUSE_ID', 'PR_ID', 'NAME', 'OUTCOME', 'FAVORABILITY', 'CAUSE', 'LOCUS_CAUSALITY', 'CONTROLLABILITY']
		for i, line in enumerate(reader):
			if i == 0:
				continue
			sentenceDict = dict(zip(keyList ,line))
			try:
				db.saveCompletedSentence(sentenceDict)
			except:
				pass


if __name__ == '__main__':
	loadCompeletedCodingFile('Corpus/completed-coding.csv')
"""
Bobi Pu, bobi.pu@usc.edu
"""

from pymongo import MongoClient


class DBController(object):
	def __init__(self):
		try:
			self._db = MongoClient().PRCorpus
		except Exception as e:
			print e
			exit()

	def saveCompletedSentence(self, sentenceDict):
		self._db.completedSentence.save(sentenceDict)

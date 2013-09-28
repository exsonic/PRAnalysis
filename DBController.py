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

	def getAllCompletedSentence(self):
		return self._db.completedSentence.find(timeout = False)

	def getAllCompletedSentenceByKeyAndScore(self, key, score):
		return self._db.completedSentence.find({key : score}, timeout = False)

	def savePRArticle(self, articleDict):
		self._db.PRArticle.save(articleDict)

	def getAllPRArticle(self):
		return self._db.PRArticle.find(timeout=False)
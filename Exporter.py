"""
Bobi Pu, bobi.pu@usc.edu
"""
from Queue import Queue
import csv
import os
from threading import Thread
from Settings import *
from DBController import DBController
from Utils import sentenceToWordList, getWordDict, getWordRegexPattern, getNGramTupleList


class CSVWriterThread(Thread):
	def __init__(self, resultQueue, outputfilePath, attributeLineList, mode='w'):
		super(CSVWriterThread, self).__init__()
		self._resultQueue = resultQueue
		self._outputfilePath = outputfilePath
		self._attributeLineList = attributeLineList
		self._writeMode = mode
		if not os.path.exists('export/'):
			os.makedirs('export/')

	def run(self):
		i = 0
		with open(self._outputfilePath, self._writeMode) as f:
			writer = csv.writer(f)
			writer.writerow(self._attributeLineList)
			while True:
				lineList = self._resultQueue.get()
				print(i)
				i += 1
				if lineList == END_OF_QUEUE:
					self._resultQueue.task_done()
					break
				else:
					try:
						writer.writerow(lineList)
					except Exception as e:
						print(e)
					finally:
						self._resultQueue.task_done()


class ProcessThread(Thread):
	def __init__(self, taskQueue, resultQueue, *args):
		super(ProcessThread, self).__init__()
		self._taskQueue = taskQueue
		self._resultQueue = resultQueue
		self._args = args
		self._executeFunction = None
		self._db = DBController()

	def extarctKeywordFromCompletedSentence(self):
		key, isBigram = self._args[0], self._args[1]
		wordDict = {}
		filterWordDict = getWordDict(WORD_FILTER)
		while True:
			sentenceList = self._taskQueue.get()
			if sentenceList == END_OF_QUEUE:
				self._taskQueue.task_done()
				break
			else:
				for sentenceDict in sentenceList:
					if key == KEY_FAV:
						wordList = sentenceToWordList(sentenceDict['OUTCOME'], filterWordDict)
					else:
						wordList = sentenceToWordList(sentenceDict['CAUSE'], filterWordDict)
					if isBigram:
						biGramTupleList = getNGramTupleList(wordList, 2)
						for bigramTuple in biGramTupleList:
							bigramString = ' '.join(bigramTuple)
							if bigramString not in wordDict:
								wordDict[bigramString] = [bigramString, 1]
							else:
								wordDict[bigramString][1] += 1
					else:
						for word in wordList:
							if word not in wordDict:
								wordDict[word] = [word, 1]
							else:
								wordDict[word][1] += 1

				wordList = wordDict.values()
				sortedWordList = sorted(wordList, key= lambda item : item[1], reverse=True)

				for wordTuple in sortedWordList:
					#if wordTuple[1] < CUT_OFF_TIMES:
					#	break
					self._resultQueue.put(wordTuple)
				self._taskQueue.task_done()

	def matchKeywordWithArticle(self):
		isMcDDict = self._args[0]
		if isMcDDict:
			patternList = [getWordRegexPattern(MCD_LITIGIOUS), getWordRegexPattern(MCD_MODAL_STRONG), getWordRegexPattern(MCD_MODAL_WEAK), getWordRegexPattern(MCD_POS), getWordRegexPattern(MCD_NEG), getWordRegexPattern(MCD_UNCERTAIN)]
		else:
			patternList = [getWordRegexPattern(WORD_FAV_POS), getWordRegexPattern(WORD_FAV_NEG), getWordRegexPattern(WORD_CAUSE_IN), getWordRegexPattern(WORD_CAUSE_EX), getWordRegexPattern(WORD_CONTROL_LOW), getWordRegexPattern(WORD_CONTROL_HIGH)]
		while True:
			articleList = self._taskQueue.get()
			if articleList == END_OF_QUEUE:
				self._taskQueue.task_done()
				break
			else:
				for articleDict in articleList:
					lineList = [articleDict['_id']] + [''] * (2 * NUM_OF_WORD_TYPE)
					for i, pattern in enumerate(patternList):
						matchedWordList = pattern.findall(articleDict['text'])
						lineList[i + 1] = len(matchedWordList)
						lineList[i + NUM_OF_WORD_TYPE + 1] = ', '.join(matchedWordList)
					self._resultQueue.put(lineList)
				self._taskQueue.task_done()


	def run(self):
		self._executeFunction()


class ExportMaster():
	def __init__(self):
		self._resultQueue = Queue()
		self._taskQueue = Queue()
		self._db = DBController()
		self._threadNumber = 1
		self._threadList = []

	def exportCompletedSentenceKeyword(self, fileName, key, score, isBigram=False):
		attributeList = ['word', 'frequency']
		writer = CSVWriterThread(self._resultQueue, 'word/' + fileName, attributeList)
		writer.start()

		self._taskQueue.put(self._db.getAllCompletedSentenceByKeyAndScore(key, score))
		self._taskQueue.put(END_OF_QUEUE)

		thread = ProcessThread(self._taskQueue, self._resultQueue, key, isBigram)
		thread._executeFunction = thread.extarctKeywordFromCompletedSentence
		thread.start()

		self._taskQueue.join()
		thread.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writer.join()

	def exportArticleKeywordMatch(self, fileName, isMcDDict):
		if isMcDDict:
			attributeList = ['PR_ID', 'litigous', 'Mod_strong', 'Mod_weak', 'Pos', 'Neg', 'Uncer', 'litigous_words', 'Mod_strong_words', 'Mod_weak_word', 'Pos_words', 'Neg_words', 'Uncer_words']
		else:
			attributeList = ['PR_ID', 'fav_pos', 'fav_neg', 'cau_int', 'cau_ext', 'cont_h',	'cont_l', 'fav_pos_words', 'fav_neg_word', 'cau_int_word', 'cau_ext_words', 'cont_h_words', 'cont_l_words']
		writer = CSVWriterThread(self._resultQueue, 'export/' + fileName, attributeList)
		writer.start()

		self._taskQueue.put(self._db.getAllPRArticle())
		self._taskQueue.put(END_OF_QUEUE)


		thread = ProcessThread(self._taskQueue, self._resultQueue, isMcDDict)
		thread._executeFunction = thread.matchKeywordWithArticle
		thread.start()

		self._taskQueue.join()
		thread.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writer.join()

if __name__ == '__main__':
	exporter = ExportMaster()
	exporter.exportCompletedSentenceKeyword('favorability_neg.csv', KEY_FAV, 1, True)
	exporter.exportCompletedSentenceKeyword('favorability_pos.csv', KEY_FAV, 7, True)
	exporter.exportCompletedSentenceKeyword('causality_ext.csv', KEY_CAUSE, 1, True)
	exporter.exportCompletedSentenceKeyword('causality_int.csv', KEY_CAUSE, 7, True)
	exporter.exportCompletedSentenceKeyword('controlability_low.csv', KEY_CONTROL, 1, True)
	exporter.exportCompletedSentenceKeyword('controlability_high.csv', KEY_CONTROL, 7,True)
	#exporter.exportArticleKeywordMatch('output_Mcd_dic.csv', True)

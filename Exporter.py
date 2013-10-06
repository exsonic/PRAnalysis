"""
Bobi Pu, bobi.pu@usc.edu
"""
from Queue import Queue
import csv
import os
from threading import Thread
from Settings import *
from DBController import DBController
from Utils import sentenceToWordList, getWordDict, getNGramTupleList, getMatchWordListFromPattern, getWordRegexPattern


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
			if self._attributeLineList:
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
					if wordTuple[1] < CUT_OFF_TIMES:
						break
					self._resultQueue.put(wordTuple[:1])
				self._taskQueue.task_done()

	def matchKeywordWithArticle(self):
		isMcDDict = self._args[0]
		filterWordDict = getWordDict(WORD_FILTER)
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
					totalWordCount = len(articleDict['text'].split())
					lineList = [articleDict['_id'], totalWordCount] + [''] * (2 * NUM_OF_WORD_TYPE)
					for i, pattern in enumerate(patternList):
						matchedWordList = getMatchWordListFromPattern(articleDict['text'], pattern, filterWordDict)
						lineList[i + 2] = len(matchedWordList)
						lineList[i + NUM_OF_WORD_TYPE + 2] = ', '.join(matchedWordList)
					self._resultQueue.put(lineList)
				self._taskQueue.task_done()

	def validate(self):
		patternList = [getWordRegexPattern(WORD_FAV_NEG), getWordRegexPattern(WORD_FAV_POS), getWordRegexPattern(WORD_CAUSE_EX), getWordRegexPattern(WORD_CAUSE_IN), getWordRegexPattern(WORD_CONTROL_LOW), getWordRegexPattern(WORD_CONTROL_HIGH)]
		filterWordDict = getWordDict(WORD_FILTER)
		while True:
			sentenceList = self._taskQueue.get()
			if sentenceList == END_OF_QUEUE:
				self._taskQueue.task_done()
				break
			else:
				for sentenceDict in sentenceList:
					lineList = [sentenceDict['_id'], sentenceDict['OUTCOME'], sentenceDict['FAVORABILITY'], 0, 0, sentenceDict['CAUSE'], sentenceDict['LOCUS_CAUSALITY'], 0, 0, sentenceDict['CONTROLLABILITY'], 0, 0]
					#loop key and linelist index
					if sentenceDict['FAVORABILITY'] <= 2:
						#lineList[3] = len(getMatchWordListFromPattern(sentenceDict['OUTCOME'], patternList[0], filterWordDict))
						lineList[3] = ' '.join(getMatchWordListFromPattern(sentenceDict['OUTCOME'], patternList[0], filterWordDict))
					elif sentenceDict['FAVORABILITY'] >= 6:
						#lineList[4] = len(getMatchWordListFromPattern(sentenceDict['OUTCOME'], patternList[1], filterWordDict))
						lineList[4] = ' '.join(getMatchWordListFromPattern(sentenceDict['OUTCOME'], patternList[1], filterWordDict))
					if sentenceDict['LOCUS_CAUSALITY'] <= 2:
						#lineList[7] = len(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[2], filterWordDict))
						lineList[7] = ' '.join(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[2], filterWordDict))
					elif sentenceDict['LOCUS_CAUSALITY'] >= 6:
						#lineList[8] = len(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[3], filterWordDict))
						lineList[8] = ' '.join(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[3], filterWordDict))
					if sentenceDict['CONTROLLABILITY'] <= 2:
						#lineList[10] = len(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[4], filterWordDict))
						lineList[10] = ' '.join(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[4], filterWordDict))
					elif sentenceDict['CONTROLLABILITY'] >= 6:
						#lineList[11] = len(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[5], filterWordDict))
						lineList[11] = ' '.join(getMatchWordListFromPattern(sentenceDict['CAUSE'], patternList[5], filterWordDict))

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

	def exportKeywordDictFromCompletedSentence(self, fileName, key, score, isBigram=False):
		#attributeList = ['word', 'frequency']
		attributeList = []
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
			attributeList = ['PR_ID', 'total_word_count', 'litigous', 'Mod_strong', 'Mod_weak', 'Pos', 'Neg', 'Uncer', 'litigous_words', 'Mod_strong_words', 'Mod_weak_word', 'Pos_words', 'Neg_words', 'Uncer_words']
		else:
			attributeList = ['PR_ID', 'total_word_count', 'fav_pos', 'fav_neg', 'cau_int', 'cau_ext', 'cont_l',	'cont_h', 'fav_pos_words', 'fav_neg_word', 'cau_int_word', 'cau_ext_words', 'cont_l_words', 'cont_h_words']
		writer = CSVWriterThread(self._resultQueue, 'export/' + fileName, attributeList)
		writer.start()

		articleList = list(self._db.getAllPRArticle())
		chunkSize = len(articleList)/self._threadNumber
		for i in range(0, len(articleList), chunkSize):
			self._taskQueue.put(articleList[i: i+chunkSize])

		for i in range(0, self._threadNumber):
			self._taskQueue.put(END_OF_QUEUE)

		threadList = []
		for i in range(self._threadNumber):
			thread = ProcessThread(self._taskQueue, self._resultQueue, isMcDDict)
			thread._executeFunction = thread.matchKeywordWithArticle
			thread.start()
			threadList.append(thread)

		for thread in threadList:
			thread.join()
		self._taskQueue.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writer.join()

	def exportDictValidation(self, fileName):
		attributeList = ['id', 'outcome', 'favorability', 'favorability_neg', 'favoribility_pos', 'cause', 'locus_causality', 'causality_ext', 'causality_int', 'controllability', 'controlability_low', 'controlability_high']
		writer = CSVWriterThread(self._resultQueue, 'export/' + fileName, attributeList)
		writer.start()
		self._taskQueue.put(self._db.getAllCompletedSentence())
		self._taskQueue.put(END_OF_QUEUE)


		thread = ProcessThread(self._taskQueue, self._resultQueue)
		thread._executeFunction = thread.validate
		thread.start()

		self._taskQueue.join()
		thread.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writer.join()

if __name__ == '__main__':
	exporter = ExportMaster()
	#exporter.exportKeywordDictFromCompletedSentence('favorability_neg.csv', KEY_FAV, 1, True)
	#exporter.exportKeywordDictFromCompletedSentence('favorability_pos.csv', KEY_FAV, 7, True)
	#exporter.exportKeywordDictFromCompletedSentence('causality_ext.csv', KEY_CAUSE, 1, True)
	#exporter.exportKeywordDictFromCompletedSentence('causality_int.csv', KEY_CAUSE, 7, True)
	#exporter.exportKeywordDictFromCompletedSentence('controlability_low.csv', KEY_CONTROL, 1, True)
	#exporter.exportKeywordDictFromCompletedSentence('controlability_high.csv', KEY_CONTROL, 7,True)
	exporter.exportArticleKeywordMatch('output_att_dic.csv', False)
	#exporter.exportDictValidation('dict_validation.csv')
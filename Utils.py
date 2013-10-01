"""
Bobi Pu, bobi.pu@usc.edu
Copyrights belongs to USC Marshall Business School.
"""

import csv
from itertools import tee, islice
import os
import re
from Settings import *
from DBController import DBController
from nltk.stem.wordnet import WordNetLemmatizer

lemmatizer = WordNetLemmatizer()

def getWordListFilePath(wordType):
	if wordType == WORD_FILTER:
		return 'word/filterWord.csv'
	elif wordType == WORD_FAV_POS:
		return 'word/favorability_pos.csv'
	elif wordType == WORD_FAV_NEG:
		return 'word/favorability_neg.csv'
	elif wordType == WORD_CAUSE_EX:
		return 'word/causality_ext.csv'
	elif wordType == WORD_CAUSE_IN:
		return 'word/causality_int.csv'
	elif wordType == WORD_CONTROL_LOW:
		return 'word/controlability_low.csv'
	elif wordType == WORD_CONTROL_HIGH:
		return 'word/controlability_high.csv'
	elif wordType == MCD_LITIGIOUS:
		return 'word/LoughranMcDonald_Litigious.csv'
	elif wordType == MCD_MODAL_STRONG:
		return 'word/LoughranMcDonald_ModalStrong.csv'
	elif wordType == MCD_MODAL_WEAK:
		return 'word/LoughranMcDonald_ModalWeak.csv'
	elif wordType == MCD_POS:
		return 'word/LoughranMcDonald_Positive.csv'
	elif wordType == MCD_NEG:
		return 'word/LoughranMcDonald_Negative.csv'
	elif wordType == MCD_UNCERTAIN:
		return 'word/LoughranMcDonald_Uncertainty.csv'

def getWordList(wordType):
	with open(getWordListFilePath(wordType)) as f:
		return [word.strip().lower() for word in f.readlines()]

def getWordDict(wordType):
	wordList = getWordList(wordType)
	return dict(zip(wordList, [0] * len(wordList)))

def getWordRegexPattern(wordType):
	wordList = getWordList(wordType)
	patternString = r'|'.join([r'\b' + word + r'\b' for word in wordList])
	return re.compile(patternString, re.IGNORECASE)

def getBigramWordRegexPatternList(wordType):
	wordList = getWordList(wordType)
	patternList = []
	#FIND THE PATTERN that may contain a word within it, them check the word is in filter list or not, not is number
	for bigram in wordList:
		patternString = r'\b' + (bigram.split()[0] + r'( [\w\d]+)* ') + bigram.split()[1] + r'\b'
		patternList.append(re.compile(patternString, re.IGNORECASE))
	return patternList

def getValidBigramMatchCount(text, patternList, filterWordDict):
	count = 0
	for pattern in patternList:
		matchedObject = pattern.search(text)
		if matchedObject:
			isValid = True
			wordString = matchedObject.group()
			for middleWord in wordString.split()[1:-1]:
				if middleWord not in filterWordDict and not unicode.isdigit(middleWord):
					isValid = False
					break
			count = count + 1 if isValid else count
	return count

def lemmatize(word):
	lemmatizedWord = lemmatizer.lemmatize(word, NOUN)
	if lemmatizedWord != word:
		return lemmatizedWord
	lemmatizedWord = lemmatizer.lemmatize(word, VERB)
	if lemmatizedWord != word:
		return lemmatizedWord
	lemmatizedWord = lemmatizer.lemmatize(word, ADJ)
	if lemmatizedWord != word:
		return lemmatizedWord
	return lemmatizer.lemmatize(word, ADV)

def sentenceToWordList(sentence, filterWordDict=None):
	if filterWordDict is not None:
		wordList = [lemmatize(word.lower().strip()) for word in sentence.split() if unicode.isalpha(word)]
		return [word for word in wordList if word not in filterWordDict]
	else:
		return [lemmatize(word.lower().strip()) for word in sentence.split() if unicode.isalpha(word)]

def getNGramTupleList(wordList, n):
	tupleList = []
	while True:
		a, b = tee(wordList)
		l = tuple(islice(a, n))
		if len(l) == n:
			tupleList.append(l)
			next(b)
			wordList = b
		else:
			break
	return tupleList

def loadCompeletedCodingFile(filePath):
	db = DBController()
	with open(filePath, 'rU') as f:
		reader = csv.reader(f)
		keyList = ['_id', 'OC_ID', 'OUTCOME_ID', 'CAUSE_ID', 'PR_ID', 'NAME', 'OUTCOME', 'FAVORABILITY', 'CAUSE', 'LOCUS_CAUSALITY', 'CONTROLLABILITY']
		for i, line in enumerate(reader):
			if i == 0:
				continue
			try:
				line[7] = int(line[7])
				line[9] = int(line[9])
				line[10] = int(line[10])
				sentenceDict = dict(zip(keyList ,line))
				db.saveCompletedSentence(sentenceDict)
			except:
				pass

def loadPRFiles(folderPath):
	db = DBController()
	for dirPath, dirNames, fileNames in os.walk(folderPath):
		for fileName in fileNames:
			if not fileName.endswith('TXT.txt'):
				continue
			filePath = os.path.join(dirPath, fileName)
			fileNameParts = fileName.split('.')[0].split('_')
			articleDict = {'_id':fileName.split('.')[0], 'code' : fileNameParts[0], 'year' : int(fileNameParts[1]), 'quarter' : fileNameParts[2]}
			with open(filePath, 'rU') as f:
				articleDict['text'] = ('\n '.join(f.readlines())).decode('utf-8', 'ignore')
			try:
				db.savePRArticle(articleDict)
			except:
				pass


#if __name__ == '__main__':
	#loadCompeletedCodingFile('Corpus/completed-coding.csv')
	#loadPRFiles('/Users/exsonic/Dropbox/Marshall_RA/ENRON/SP500_PR_1999_2004')
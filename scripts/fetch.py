'''
Fetch an RSS feed and update Blender text object

Author: Julien Deswaef

This program comes with ABSOLUTELY NO WARRANTY;
This program is free software; you can redistribute it and/or modify it.

The Class "Convert to Roman Numerals" it by Mark Pilgrim
Copyright (c) 2001 Mark Pilgrim
and released under the terms of the Python 2.1.1 license, available at
http://www.python.org/2.1.1/license.html

The rest of the code is by Julien Deswaef
Copyright (c) 2015 Julien Deswaef
and released under the terms of the Gnu GPL v3 license, available at
https://www.gnu.org/licenses/gpl.html

'''

import bpy
import requests
from xml.etree import ElementTree as ElTree
from html.parser import HTMLParser
import re

#-----------------------------------------------------------------------
# SCRIPT PARAMETERS
#-----------------------------------------------------------------------

NYT_WORLD = 'http://www.nytimes.com/services/xml/rss/nyt/World.xml'
NYT_US = 'http://www.nytimes.com/services/xml/rss/nyt/US.xml'
NYT_AFRIKA = 'http://www.nytimes.com/services/xml/rss/nyt/Africa.xml' 
NYT_MIDDLE_EAST = 'http://www.nytimes.com/services/xml/rss/nyt/MiddleEast.xml'

DATA_FOLDER = bpy.path.abspath('//data/')

WORDS = [r'war(\s|s)', r'suicide bomb', r'terrorist', r'peace', r'diploma[^\ss]+']

yellow = bpy.data.materials['yellow']
font_family = bpy.data.fonts['NewsCycle']
intro_scene = bpy.data.scenes['Logo']

#-----------------------------------------------------------------------
# A CLASS to Convert to Roman numearals
#-----------------------------------------------------------------------

#Define exceptions
class RomanError(Exception): pass
class OutOfRangeError(RomanError): pass
class NotIntegerError(RomanError): pass
class InvalidRomanNumeralError(RomanError): pass

#Define digit mapping
romanNumeralMap = (('M',  1000),
                   ('CM', 900),
                   ('D',  500),
                   ('CD', 400),
                   ('C',  100),
                   ('XC', 90),
                   ('L',  50),
                   ('XL', 40),
                   ('X',  10),
                   ('IX', 9),
                   ('V',  5),
                   ('IV', 4),
                   ('I',  1))

def toRoman(n):
    """convert integer to Roman numeral"""
    if not isinstance(n, int):
        raise NotIntegerError("decimals can not be converted")
    if not (0 < n < 5000):
        raise OutOfRangeError("number out of range (must be 1..4999)")

    result = ""
    for numeral, integer in romanNumeralMap:
        while n >= integer:
            result += numeral
            n -= integer
    return result

#-----------------------------------------------------------------------
# HTMLPARSER class to clean out the RSS feid descriptions
#-----------------------------------------------------------------------

class Parser(HTMLParser):
	# Just an HTML parser to strip out all HTMLtag 
	def extract(self, html):
		self.text = ""
		self.feed(self.unescape(html))
		return self.text
	def handle_data(self, data):
		self.text = data


def getRSSFeed( url ):
	raw = requests.get( url )
	rss = ElTree.fromstring(raw.text)

	items = []

	channel = rss.getchildren()[0]

	for item in channel.getchildren():
		if item.tag == 'item':
			item_dic = {}
			for child in item.getchildren():
				if child.tag == 'title':
					item_dic['title'] = child.text
				elif child.tag == 'description':
					item_dic['description'] = Parser().extract(child.text)
				elif child.tag == 'guid':
					item_dic['link'] = child.text
			items.append(item_dic)
	
	return items
	

#-----------------------------------------------------------------------
# FUNCTIONS
#-----------------------------------------------------------------------
	
def compileRegex(word_list):
	compiled = []
	for word in word_list:
		c = re.compile(word, re.UNICODE | re.IGNORECASE )
		compiled.append( c )
	return compiled

def filterArticles( feed, regex, past_guid ):
	extracted = []
	for item in feed:
		extract = False
		for word in regex:
			if not extract:
				if word.search( item['title'].lower() ) or word.search( item['description'].lower() ) :
					extract = True
					for guid in past_guid:
						if extract and item['link'] == guid:
							extract = False
					if extract:
						extracted.append(item)
	return extracted
	
def deleteIntroText():
	# Delete intro text if it exists
	if bpy.data.objects.find('intro_text') is not -1:
		intro_scene.objects.unlink(intro_scene.objects['intro_text'])
		bpy.data.objects.remove(bpy.data.objects['intro_text'])

def createIntroText( articles ):
    bpy.ops.object.text_add(location=(-5,0,0))
    text_object = bpy.context.active_object
    text_object.name = 'intro_text'
    text_object.data.align = 'JUSTIFY'
    bpy.context.object.data.space_line = 1.7
    text_object.data.font = font_family             # set font family
    text_object.data.text_boxes[0].width = 10       # box width
    text_object.data.size = 1.5                     # font size
    text_object.data.space_line = 1                 # line spacing
    
    
    bpy.ops.object.mode_set( mode = 'EDIT' )
    bpy.ops.font.delete(type='ALL')
    for article in articles:
        bpy.ops.font.text_insert(text= article['title'])
        bpy.ops.font.line_break()
        bpy.ops.font.text_insert(text= article['description'])
        bpy.ops.font.line_break()
        bpy.ops.font.line_break()
    bpy.ops.object.mode_set( mode = 'OBJECT' )
       
    text_object.data.materials.append(yellow)
    
def getText(file):
	# reads a text files and returns an array of text lines
	f = open(file, 'r')
	lines = f.readlines()
	for i, l in enumerate(lines):
		if l[-1:] == '\n':
			lines[i] = l[:-1]
	f.close()
	return lines


def writeText(file, text):
	#opens a text file (or creates one if does not exists) and writes new text
	f = open(file, 'w')
	f.write(text)
	f.close()

def addMoreArticlesFrom( feed, regex, past_guid ):
	# adds more articles to a global dic
	more_news = getRSSFeed( feed )
	more_news = filterArticles( more_news, regex, past_guid )
	for new_article in more_news:
		exists_already = False
		for article in news:
			if new_article['title'] == article['title']:
				exists_already = True
		if not exists_already:
			news.append(new_article)

def updateEpisodeTitle():
	counter = int( getText( DATA_FOLDER + 'counter.txt' )[0] ) + 1
	episode_title = "Episode " + toRoman(counter)
	episode_text = bpy.data.scenes['Logo'].objects['episode_text']
	episode_text.data.body = episode_title
	writeText( DATA_FOLDER + 'counter.txt', str(counter) )
	return episode_title

def writeOutDescription(news, episode_title):
	# WRITING OUT TEXT DATA
	description_text = "WORD WARS − " + episode_title + "\n"
	description_text += '----------------------------------------------------------------------\n\n'

	for article in news:
		description_text += article['title'] + '\n'
		description_text += article['description'] + '\n'
		description_text += article['link'] + '\n\n' 
		
	description_text += '----------------------------------------------------------------------\n'
	description_text += 'WORD WARS − News from the Empire − by http://xuv.be'
	
	print( description_text )
	
	writeText( DATA_FOLDER + 'description.txt', description_text )

def updateCameraDollyKeyframe():
	# Setup the keyframes for the camera dolly relative to the text
	intro_text = intro_scene.objects['intro_text']
	intro_length = intro_text.dimensions.y

	dolly = intro_scene.objects['dolly']
	intro_scene.frame_set(1920)
	dolly.location.y = -(intro_length + 50)
	dolly.keyframe_insert(data_path='location', frame=1920)
	intro_scene.frame_set(1)
	
def updatePastGuidList( news, past_guid, maximum, outputFile ):
	for article in news:
		past_guid.append( article['link'] )
	
	diff = len( past_guid ) - maximum
	if diff > 0:
		past_guid = past_guid[(diff-1):]
	
	text = ""
	for line in past_guid:
		text += line + '\n'
	writeText(outputFile, text)

#-----------------------------------------------------------------------
# MAIN
#-----------------------------------------------------------------------

regex = compileRegex( WORDS )
past_guid = getText( DATA_FOLDER + 'past_guid.txt' )

news = getRSSFeed( NYT_WORLD )
	
news = filterArticles( news, regex, past_guid )

# Maximum of news to display
maximum = 3

if len( news ) < maximum :
	addMoreArticlesFrom( NYT_US, regex, past_guid )
	        
if len( news ) < maximum :
	addMoreArticlesFrom( NYT_MIDDLE_EAST, regex, past_guid )

if len( news ) < maximum :
	addMoreArticlesFrom( NYT_AFRIKA, regex, past_guid )

# Always limit the number of use to be maximum 3	
if len( news ) > maximum :
    news = news[0:maximum]

if len( news ) > 0 :
	deleteIntroText()
	createIntroText( news )
	updateCameraDollyKeyframe()
	episode_title = updateEpisodeTitle()
	writeOutDescription( news, episode_title )
	updatePastGuidList( news, past_guid, maximum*4, DATA_FOLDER + 'past_guid.txt')
	# save blende file
	bpy.ops.wm.save_mainfile()

	

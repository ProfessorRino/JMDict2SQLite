import urllib.request
import os
import gzip
import sqlite3
import xml.etree.ElementTree as ET #for normal parsing 
from lxml import etree as et #just for parsing annotation without expanding references

import re

url = "ftp://ftp.monash.edu.au/pub/nihongo/JMdict.gz"
path = os.getcwd()
zipPath = path + "/JMdict.gz"
targetPath = path + "/JMdict.xml"

print("Downloading JMdict.gz")
urllib.request.urlretrieve(url, zipPath)

block_size=65536

print("Unzipping JMdict.gz")
with gzip.open(zipPath, 'rb') as s_file, \
          open(targetPath, 'wb') as d_file:
    while True:
        block = s_file.read(block_size)
        if not block:
            break
        else:
            d_file.write(block)
    d_file.write(block)

print("Building wootdictionaryJEX.db")

#prepare .db
name = "/wootdictionaryJEX.db"
conn = sqlite3.connect(path + name)
cur = conn.cursor()

cur.execute("Drop table if exists JEX;")
cur.execute("Create table JEX (primaryKey int PRIMARY KEY, key int, resultword text, searchword text, lan text, pri int)")
cur.execute("Drop table if exists ATTRIBUTES;")
cur.execute("Create table ATTRIBUTES (primaryKey int PRIMARY KEY, key int, tagstring text);")

# database scheme:
# table JEX (X is whatever third language, delete other languages later)
# primaryKey -> Room requires an explicit one
# key identifies dictionary items as corresponding to each other
# resultword text with all remarks, usually in ()
# searchword text without remarks, brackets and everything in between removed
# LAN:
# R reading
# K kanji
# eng sense in English
# ger German
# rus Russian
# dut Dutch
# hun Hungarian
# pri
# 0 no priority
# 1 frequently used
# table ATTRIBUTES
# primaryKey 
# key
# tagstring contains the abbreviated annotations to a dictionary item

def removeBrackets(word):
    return re.sub(r" ?\([^)]+\)", "", word)

def removeInfinitiveTo(word):
    if (word.startswith("to ")):
        return word[3:]
    return word

def getPriority(priorities):
    for priority in priorities:
            if priority.text in priorityList:
                return 1
    return 0                

priorityList = ["news1", "ichi1", "spec1", "spec2", "gai1"]
insertWord = """Insert into JEX values (?, ?, ?, ?, ?, ?)"""

tree = ET.parse(targetPath)
root = tree.getroot()

batchCount = 0
keyCount = 0
for entry in root.findall("entry"):
    key = entry.find("ent_seq")
    for reading in entry.findall("r_ele"):
        word = reading.find("reb").text
        pri = getPriority(reading.findall("re_pri"))
        cur.execute(insertWord, (None, int(key.text), word, removeBrackets(word), "R", pri))
    for kanji in entry.findall("k_ele"):
        word  = kanji.find("keb").text
        pri = getPriority(kanji.findall("ke_pri"))
        cur.execute(insertWord, (None, int(key.text), word, removeBrackets(word), "K", pri))
    for sense in entry.findall("sense"):
        for gloss in sense.findall("gloss"):
            word = gloss.text
            lan = gloss.attrib['{http://www.w3.org/XML/1998/namespace}lang']
            if (lan == "eng" or lan == "ger") : #or lan == "hun" or lan == "dut" or lan == "rus"):
                filteredSearchWord = removeBrackets(word)
                if (lan == "eng"):
                    filteredSearchWord = removeInfinitiveTo(filteredSearchWord)
                cur.execute(insertWord, (None, int(key.text), word, filteredSearchWord, lan, 0))
    batchCount += 1
    if (batchCount > 50000):
        conn.commit()
        batchCount = 0
conn.commit()
cur.execute("""CREATE INDEX 'KEY_INDEX' ON 'JEX' ('key','searchword','resultword','lan', 'pri');""")
conn.commit()
cur.execute("""CREATE INDEX 'SEARCH_INDEX' ON 'JEX' ('searchword','key','resultword','lan', 'pri');""")
conn.commit()

print("building annotations")
#parsing one more time with lxml just for annotations, because they are not to be expanded 
parser = et.XMLParser(resolve_entities=False)
root = et.parse(targetPath, parser).getroot()
insertAnnot = """Insert into ATTRIBUTES values (?,?,?)"""
batchCount = 0
for entry in root.findall("entry"):
    key = entry.find("ent_seq")
    for pos in entry.iter("pos"):
        pos = str(et.tostring(pos)).replace("b\'<pos>","").replace("</pos>","").replace("&","").replace("\\n","").replace("\'","").replace(";","")
        cur.execute(insertAnnot, (None, int(key.text), pos))
    batchCount += 1
    if (batchCount > 50000):
        conn.commit()
        batchCount = 0
conn.commit()   
cur.execute("""CREATE INDEX 'ANNOT_INDEX' ON 'ATTRIBUTES' ('key','tagstring');""")
conn.commit() 
cur.execute("VACUUM;")
conn.commit()
cur.close()
conn.close()
print("Deleting .zip and .xml")
os.remove(zipPath)
os.remove(targetPath)
print("Finished, .db is ready")

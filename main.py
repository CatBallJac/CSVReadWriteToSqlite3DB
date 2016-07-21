import csv
import glob
import chardet


def getEncodingType(fileDir):
    csvbyte = open(fileDir,"rb").read()
    result = chardet.detect(csvbyte)
    return result['encoding']

def readCSVFile(fileDir):
    csvfile = open(fileDir, newline = '', encoding = getEncodingType(fileDir))
    reader = csv.DictReader(csvfile)
    return reader

def ifHasUsername(Reader):
    fieldnames = []
    for row in Reader:
        fieldnames = row
        break
    for item in fieldnames:
        if item == 'username':
            return True
    return False

def writeCSVFile(outputfile,name_list):
    with open(outputfile, 'w') as csvfile:
        fieldnames = ['username']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for x in name_list:
            writer.writerow({'username': x})



name_list = []

for filename in glob.glob('instagram_data/*.csv'):
    csvReader = readCSVFile(filename)
    if ifHasUsername(csvReader):
        for row in csvReader:
            name_list.append(row['username'])

name_list = set(sorted(name_list))
print(len(name_list))

writeCSVFile('instagram_data/username_sheet.csv',name_list)










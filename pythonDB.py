import sqlite3
import csv
import glob
import chardet

class Sqlite3Adapter:

    conn = sqlite3.connect("mySqlite3.db")

    def getEncodingType(self, fileDir):
        csvbyte = open(fileDir, "rb").read()
        result = chardet.detect(csvbyte)
        return result['encoding']

    def readCSVFile(self, fileDir):
        csvfile = open(fileDir, newline='', encoding=self.getEncodingType(fileDir))
        reader = csv.DictReader(csvfile)
        return reader

    def getCSVfieldnames(self, reader):
        fieldlist_origin = reader.fieldnames
        fieldlist = []
        translation_table = dict.fromkeys(map(ord,' '), None)
        for item in fieldlist_origin:
            fieldlist.append(str(item).translate(translation_table))
        return fieldlist

    def getCSVToRows(self, fieldlist, reader):
        rows = []
        for row in reader:
            row_list = []
            for item in fieldlist:
                row_list.append(row[item])
            rows.append(row_list)
        return rows

    def saveToTable(self, fileDir, tableName = None):
        csvReader = self.readCSVFile(fileDir)
        rows = self.getCSVToRows(csvReader.fieldnames, csvReader)
        fieldnames = self.getCSVfieldnames(csvReader)
        if tableName == None:
            self.createTable(fileDir, fieldnames)
            self.insertToTable(fileDir, rows)

        else:
            if self.checkTableExists(tableName):
                self.insertToTable(tableName, rows)
            else:
                self.createTable(tableName, fieldnames)
                self.insertToTable(tableName, rows)

    def checkTableExists(self, tableName):
        dbcur = self.conn.cursor()
        stmt = "SELECT * FROM sqlite_master WHERE name = '" + tableName + "' and type = 'table'"
        dbcur.execute(stmt)
        result = dbcur.fetchone()
        # if can fetch one: already exists
        # if can find no one: not exists
        dbcur.close()
        if result:
            return True

        return False

    def printTableContent(self, tableName):
        dbcur = self.conn.cursor()
        extractedTable = dbcur.execute("SELECT * FROM '" + tableName + "'")
        for row in extractedTable:
            print(row)
        dbcur.close()

    def createTable(self, tableName, fields_list):
        dbcur = self.conn.cursor()
        stmt = "CREATE TABLE " + tableName + " ("
        stmt += fields_list[0]
        fields_list.remove(fields_list[0])
        for item in fields_list:
            stmt += ", " + item
        stmt += ")"
        if not self.checkTableExists(tableName):
            print(stmt)
            dbcur.execute(stmt)
        dbcur.close()

    def insertToTable(self, tableName, rows):
        dbcur = self.conn.cursor()
        if not self.checkTableExists(tableName):
            print("Table " + tableName + "is missing!")
        else:
            stmt = "insert into " + tableName + " values ("

            num_fields = 0
            for r in rows:
                num_fields = int(len(r))
                break
            i = 0
            while i < num_fields - 1:
                stmt += "?,"
                i += 1
            stmt += "?)"

            dbcur.executemany(stmt, rows)
            #if the connection is not commited: the content inserted will not be saved
            self.conn.commit()

    def getTableFieldNames(self, tableName):
        dbcur = self.conn.cursor()
        names = []
        if not self.checkTableExists(tableName):
            print("Table " + tableName + " is missing!")
        else:
            extractedTable = dbcur.execute("SELECT * FROM '" + tableName + "'")
            # get the column names from the selected table
            names = [description[0] for description in extractedTable.description]
        return names

    def extractTable(self, tableName):
        dictTable = [{}]
        dbcur = self.conn.cursor()
        if not self.checkTableExists(tableName):
            print("Table "+tableName+" is missing!")
        else:
            extractedTable = dbcur.execute("SELECT * FROM '" + tableName + "'")
            fieldnames = self.getTableFieldNames(tableName)
            for row in extractedTable:
                dictRow = {}
                i = 0
                for item in fieldnames:
                    dictRow[item] = row[i]
                    i = i+1
                dictTable.append(dictRow)
            dbcur.close()
        return dictTable

    def saveToCSV(self, fileDir, tableName):
        dict = self.extractTable(tableName)
        with open(fileDir, 'w') as csvfile:
            fieldnames = self.getTableFieldNames(tableName)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dict)


myDB = Sqlite3Adapter()
fileDir = "instagram_data/Instagram_segway_data.csv"
dictReader = myDB.readCSVFile(fileDir)
#print(myDB.getCSVToRows(dictReader.fieldnames,dictReader))


#myDB.saveToTable(fileDir, "sheet")

if myDB.checkTableExists("sheet"):
    print(myDB.extractTable("sheet"))
    myDB.saveToCSV("instagram_data/2.csv", "sheet")

#myDB.extractTable("sheet")

'''
#create a table
fields_list = ['catagory', 'tag']
myDB.createTable('Dict3', fields_list)

#Insert a row of data
rows = [('sport', 'bike'), ('sport', 'skateboard'),]
myDB.insertToTable('Dict3', rows)
myDB.printTableContent('Dict3')
'''
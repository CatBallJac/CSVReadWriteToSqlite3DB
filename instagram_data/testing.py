import csv
import pprint
try:
        csvfile = open('1.csv', newline = '', encoding = 'gbk')
except:
        print('Error!')
else:
        reader = csv.DictReader(csvfile)
        print('Read in successfully.')
        for row in reader:
                pprint.pprint(row['username'])
        






                        
                                
                                
                        
                                

                        
	

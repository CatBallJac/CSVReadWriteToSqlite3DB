import csv

great_list=[]
file_list = ['Instagram_hovorboard_data.csv', 'Instagram_segway_data.csv', 'Instagram_skate2work_data.csv', 'Instagram_skate14_data.csv', 'Instagram_staryboard_data.csv']  

def read_csvfile(filename,encoding_type):
    with open(filename, newline = '', encoding = encoding_type) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['tags'] != '':
                row_list = row['tags'].split("#")
                row_list.remove('')
                for item in row_list:
                    word_list = item.split(":")
                    great_list.append(word_list[0])


   
def write_csvfile(filename):
    with open(filename, 'w') as csvfile:
        fieldnames = ['catagories', 'tags']
        writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
        writer.writeheader()
        for x in great_list:
            writer.writerow({'tags':x})



read_csvfile('Instagram_hovorboard_data.csv','gbk')
read_csvfile('Instagram_skate2work_data.csv','gbk')
read_csvfile('Instagram_skate14_data.csv','gbk')
great_list = sorted(set(great_list))
write_csvfile('sheet.csv')


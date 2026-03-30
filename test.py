import pandas as pd
import csv

file1 = "tavor.csv"
file2 = "import.csv"
item_t = ""
item_n = ""
item_s = []
description = []
id_item = []
about = []

with open(file1, "r") as csvfile1, open(file2, "r") as csvfile2:
    csvreader1 = list(csv.reader(csvfile1))
    csvreader2 = list(csv.reader(csvfile2))
    for line2 in csvreader2[1:]:
        added = False
        description = line2[1].split(' ')
        item_s = description[2][1:].split('x')
        item = line2[2].split(' ')
        try:
            item_c = round(float(description[-2]), 1)
        except ValueError:
            id_item.append("")
            about.append("")
            continue
        item_m = description[-1]
        if (len(item) > 1):
            item_t = item[0].lower()
            item_n = item[1]
        else:
            item_t, item_n = "", ""
        for line1 in csvreader1[1:]:
            cover = "ZNW" if line1[-4] == 'Zn' else "BLK"
            try:
                cls = round(float(line1[-3]), 1)
            except ValueError:
                continue
            if line1[3].lower() == item_t and line1[4] == item_n and line1[7] == item_s[0] and line1[9] == item_s[1] and cls == item_c and cover == item_m:
                id_item.append(line1[0].zfill(6))
                about.append(line1[1])
                added = True
                break
        if not added:
            id_item.append("")
            about.append("")


csv_file_path = 'import.csv'
df = pd.read_csv(csv_file_path)

df.insert(1, column='tavor_id', value=id_item)
df.insert(2, column='about', value=about)
# df.to_csv(csv_file_path, index=False)

df.to_csv(csv_file_path, index=False)


import glob
from functools import reduce

import pandas as pd
path =r'/home/dka3830/Documents/proj/output/'  #indicate your dir
allFiles = glob.glob(path + "/*.csv")
frame = pd.DataFrame()
list_ = []
for file_ in allFiles:
    df = pd.read_csv(file_,index_col=None) # you can add head=0
    df['dauid'] = df['dauid'].apply(str)  #To make sure all the file have same type on the key merg
    df = df.iloc[:,1:3] #Selecting only the wanted column to be saved
    list_.append(df)

frame = reduce(lambda left, right: pd.merge(left,right,how='left',on='dauid'), list_)

frame.to_csv("/home/dka3830/Documents/proj/output/Merged.csv", sep=',')
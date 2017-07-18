# First try 

import os
import bz2

filedir = r'C:\Users\ciao\Desktop\Baruch\SS2017\Big Data\Project C\Sample Twitter Data'
for _filepath, _filename, _files in os.walk(filedir):
    for _filename in _files:
        if _filename.endswith('.json.bz2'):
            path = os.path.join(_filepath, _filename)
            #print(path)
            #newpath = os.path.join(_filepath, _filename[:-4])  # cut off the '.bz2' part
            #filenames = os.path.basename(path)  
            #which is equivalent to, 
            #filenames = os.path.split(path)[1]
            #print(filenames)             
            try: 
                with bz2.BZ2File(path.encode('utf-8'),'rb') as file:
                  data = file.read()
                  print(data)
            except:
                  EOFError
                  

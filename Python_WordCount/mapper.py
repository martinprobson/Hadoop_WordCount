'''
Created on 12 Jun 2017

@author: martinr
'''
import glob

def Mapper(inputDir):
    for file in fileList(inputDir):
        with open(file,'r') as f:
            for line in f:
                # Remove non-alpha characters.
                line = ''.join([i for i in line if i.isalpha() or i.isspace()])  
                words = line.split()
                for word in words:
                    print('{}\t{}'.format(word,1))
        
def fileList(startDir):
    for file in glob.iglob(startDir,recursive=True):
        yield(file)

if __name__ == '__main__':
    Mapper('../data/**/*.txt')

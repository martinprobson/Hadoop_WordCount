'''
Created on 12 Jun 2017

@author: martinr
'''
import fileinput

def reduce():
    lastWord = ''
    total = 0
    firstTime = True
    for line in fileinput.input():
        word, _ = line.split('\t')
        if firstTime == True:
            firstTime = False
            lastWord = word
            total = total + 1
        else:
            if word == lastWord:
                total = total + 1
            else:
                print('{}\t{}'.format(lastWord,total))
                total = 1
                lastWord = word
    print('{}\t{}'.format(lastWord,total)) 

if __name__ == '__main__':
    reduce()

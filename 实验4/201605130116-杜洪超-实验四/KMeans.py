import random
import numpy as np
import sys
import matplotlib.pyplot as plt
import random
import re
import os

os.system('rm result*')
os.system('rm fig*')
fp=open('data.txt','w')

K=10
num=1000
if (len(sys.argv)>1):
    K=int(sys.argv[1])
center=[]
for i in range(K):
    center.append([random.randint(-100,100),random.randint(-100,100)]) 
print(center)
i=0
while i<num:
    c=random.randint(0,K-1)
    x=int(np.random.normal(center[c][0],25,1)[0])
    y=int(np.random.normal(center[c][1],25,1)[0])
    if (x<-100 or y<-100 or x>100 or y>100):
        continue
    i+=1
    fp.write(str(x)+' '+str(y)+'\n')
fp.close()
    
if (len(sys.argv)>2):
    K=int(sys.argv[2])
data=open('data.txt','r').readlines()
x=[]
y=[]
for i in data:
    x.append(int(re.findall('[0-9.e-]+',i)[0]))
    y.append(int(re.findall('[0-9.e-]+',i)[1]))

data_size=len(x)
center=[]
number=[]
for i in range(K):
    center.append([x[i],y[i]])
    number.append(data_size/K)
max_n=100
done=False
n=0
while not done and n<max_n:
    res='result_'+str(n)+'.txt'
    n+=1
    result=open(res,'w')
    new_center=[]
    new_number=[]
    for i in range(K):
        new_center.append([0.0,0.0])
        new_number.append(0)
    for i in range(data_size):
        min=80000
        c=0
        for j in range(K):
            dd=(center[j][0]-x[i])**2+(center[j][1]-y[i])**2
            if dd<min:
                min=dd
                c=j
        new_center[c][0]+=x[i]
        new_center[c][1]+=y[i]
        new_number[c]+=1
        result.write(str(x[i])+' '+str(y[i])+' '+str(c)+'\n')
    done=True
    for i in range(K):
        center[i][0]=round(new_center[i][0]/new_number[i],2)
        center[i][1]=round(new_center[i][1]/new_number[i],2)
        if new_number[i]!=number[i]:
            done=False
    number=new_number
    result.close()
print(n)
print(center)
print(number)

num=n
for n in range(num):
    f='result_'+str(n)+'.txt'
    data=open(f,'r').readlines()
    x=[]
    y=[]
    K=10
    for i in range(K):
        x.append([])
        y.append([])
        
    for i in data:
        x[int(re.findall('[0-9.e-]+',i)[2])].append(int(re.findall('[0-9.e-]+',i)[0]))
        y[int(re.findall('[0-9.e-]+',i)[2])].append(int(re.findall('[0-9.e-]+',i)[1]))
   # plt.figure(figsize=(10,6))
    for i in range(K):
        plt.scatter(x[i], y[i])
    plt.savefig('fig_'+str(n)+'.png')



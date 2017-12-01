#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pipeline
import time

# Definição das funções

def init1():
    print('Teste:')

def func1():
    for i in range(10):
        time.sleep(1)
        yield i

def func2(i):
    time.sleep(1)
    return 2*i, i+1

def func3(x, y):
    time.sleep(1)
    return {'a': 2*x, 'b': 3*y, 'c': x+y}

def func4(a=None, b=None, c=None):
    time.sleep(1)
    print(a, b, c)

# Definição do pipeline
stage1 = pipeline.Stage(func1, init1)
stage2 = pipeline.Stage(func2)
stage3 = pipeline.Stage(func3)
stage4 = pipeline.Stage(func4)
P = pipeline.Pipeline(stage4(stage3(stage2(stage1()))))

#print('--------------------')
#print('Execução sequencial:')
#t0 = time.time()
#for x in func1():
#    func4(**func3(*func2(x)))
#t = time.time()
#dt = t-t0
#print('Duração: {:.2f}s'.format(dt))

print('----------------------')
print('Execução com pipeline:')
t0 = time.time()
P.start()
P.join()
t = time.time()
dt = t-t0
print('Duração: {:.2f}s'.format(dt))

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pipeline

@pipeline.stage
def a():
    for i in range(10000):
        yield i

@pipeline.stage
def b(i):
    x = 2*i
    y = -i
    return x, y

@pipeline.stage
def c(x, y):
    x = x + 2
    y = 3*y
    z = 100-y
    return {'x': x, 'y': y, 'z': z}

@pipeline.stage
def d(z=None, y=None, x=None):
    #x + y + z
    print('x: {}, y: {}, z: {}'.format(x, y, z))

P = pipeline.Pipeline(d(c(b(a()))))
P.start()
P.join()

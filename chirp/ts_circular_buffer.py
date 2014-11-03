#!/usr/bin/env python
#coding: utf-8

import random, math


class TSCircularBuffer():


    def __init__(self, size):
        self._data = dict()
        self._cum_prob = list()
        
        self._size = size
        self._cursor = 0
        self._rolled_over_once = False
        self._zero = 0
    
    
    def insert(self, item, log_prob):
        if self._rolled_over_once:
            self._zero = self._cum_prob[self._cursor]
        
        self._data[self._cursor] = item
        
        if self._rolled_over_once:
            self._cum_prob[self._cursor] = self._cum_prob[self._cursor-1] + math.exp(log_prob)
        elif self._cursor != 0:
            self._cum_prob.append(self._cum_prob[self._cursor-1] + math.exp(log_prob))
        else:
            self._cum_prob.append(math.exp(log_prob))
        
        self._cursor += 1
        if self._cursor == self._size:
            self._rolled_over_once = True
            self._cursor = 0
    
    
    def rand(self):
        if not self._rolled_over_once and self._cursor == 0:
            return None
        
        threshold = random.random() * (self._cum_prob[self._cursor - 1] - self._zero) + self._zero
        
        if self._rolled_over_once:
            return self._thresholdItem(threshold, self._cursor - self._size, self._cursor - 1 )
        else:
            return self._thresholdItem(threshold, 0, self._cursor - 1)
    
    
    def _thresholdItem(self, threshold, begin, end):
        if begin == end:
            return self._data[begin] if begin >= 0 else self._data[begin + self._size]
        else:
            mid = (begin + end) / 2
            if self._cum_prob[mid] < threshold:
                return self._thresholdItem(threshold, mid + 1, end)
            return self._thresholdItem(threshold, begin, mid)



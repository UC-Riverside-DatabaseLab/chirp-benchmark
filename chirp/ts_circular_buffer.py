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
        self._prob_zero = 0
        self._exp_zero = 0
    
    
    def insert(self, item, log_prob):
        if self._rolled_over_once:
            self._prob_zero = self._cum_prob[self._cursor]
        
        self._data[self._cursor] = item
        
        if self._rolled_over_once:
            self._cum_prob[self._cursor] = self._cum_prob[self._cursor-1] + math.exp(log_prob - self._exp_zero)
        elif self._cursor != 0:
            self._cum_prob.append(self._cum_prob[self._cursor-1] + math.exp(log_prob))
        else:
            self._cum_prob.append(math.exp(log_prob))
        
        self._cursor += 1
        if self._cursor == self._size:
            self._rolled_over_once = True
            self._cursor = 0
            self._adjustSums()
    
    
    def _adjustSums(self):
        ratio = self._cum_prob[-1] - self._cum_prob[-2]
        self._exp_zero = math.log(ratio)
        
        for index in xrange(self._size):
            self._cum_prob[index] /= ratio
        self._prob_zero /= ratio
    
    
    def rand(self):
        if not self._rolled_over_once and self._cursor == 0:
            return None
        
        threshold = random.random() * (self._cum_prob[self._cursor - 1] - self._prob_zero) + self._prob_zero
        
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



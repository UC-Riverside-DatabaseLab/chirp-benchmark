#!/usr/bin/env python
#coding: utf-8

import random, math


class TSCircularBuffer():
    '''A circular buffer that stores items and associated relative log-likelihood of reads on them.
    Allows picking random items based on these likelihoods. TSCircularBuffer gives O(1) amortized performance on inserts,
    O(1) on indexed reads and O(logN) performance for random reads where N is the size of the buffer.'''

    def __init__(self, size):
        '''Initialize storage structures and state variables.'''
    
        self._data = dict()
        self._cum_prob = list()
        
        self._size = size
        self._cursor = 0
        self._rolled_over_once = False
        
        # These variables help guarantee performance without causing numerical overflow in probability values.
        self._prob_zero = 0
        self._exp_zero = 0
    
    
    def insert(self, item, log_prob):
        '''Insert an item item into the buffer.'''
        
        # Record the zero error in the cumulative probabilities.
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
            
            # Capture rollover, reset cursor and adjust cumulative probability values
            self._rolled_over_once = True
            self._cursor = 0
            self._adjustSums()
    
    
    def _adjustSums(self):
        '''Adjust cumulative probability values based on the last-written item's log-likelihood. This periodic step
        ensures that probabilities will not blow up during long runs.'''
        
        # Calculate the probability of the last-written item and set a new 'zero' value for future log-likelihoods.
        ratio = self._cum_prob[-1] - self._cum_prob[-2]
        self._exp_zero = math.log(ratio)
        
        # Divide existing cumulative probability values and their zero error by the probability of the last item.
        for index in xrange(self._size):
            self._cum_prob[index] /= ratio
        self._prob_zero /= ratio
    
    
    def rand(self):
        '''Returns a random item from buffer based on probabilities derived from the log-likelihoods associated with items.'''
    
        if not self._rolled_over_once and self._cursor == 0:
            return None
        
        # Calculate a cumulative probability threshold to search for taking into account the zero error
        threshold = random.random() * (self._cum_prob[self._cursor - 1] - self._prob_zero) + self._prob_zero
        
        return self._thresholdItem(threshold)
    
    
    def _thresholdItem(self, threshold):
        '''Performs a binary search to find the item such that threshold lies in the cumulative probability range specified by the item.'''
    
        begin = self._cursor - self._size if self._rolled_over_once else 0
        end = self._cursor - 1
        
        while begin != end:
            mid = (begin + end) / 2
            
            if self._cum_prob[mid] < threshold:
                begin = mid + 1
            else:
                end = mid
        
        return self[begin]
    
    
    def __getitem__(self, index):
        '''Item-getter for indexed reads.'''
    
        return self._data[index % self._size]



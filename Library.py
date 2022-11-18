#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  4 16:39:24 2022

@author: jiecanwang
"""
# From mmc639_Final_Exam, I import its required function as my raw data to help my library develop
import datetime
import time
from polygon import RESTClient
from sqlalchemy import create_engine 
from sqlalchemy import text
import pandas as pd
from math import sqrt
from math import isnan
import matplotlib.pyplot as plt
from numpy import mean
from numpy import std
from math import floor
import numpy as np


# In[10]:


# The following 10 blocks of code define the classes for storing the the return data, for each
# currency pair
        
# Define the AUDUSD_return class - each instance will store one row from the dataframe
class AUDUSD_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if AUDUSD_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - AUDUSD_return.last_price) / AUDUSD_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            AUDUSD_return.run_sum = 0
        else:
            # Increment the counter
            if AUDUSD_return.num < 5:
                AUDUSD_return.num += 1
            AUDUSD_return.run_sum += hist_return
        AUDUSD_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            AUDUSD_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            AUDUSD_return.run_sum -= pop_value
            avg = AUDUSD_return.run_sum/(AUDUSD_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(AUDUSD_return.run_squared_sum/(AUDUSD_return.num))
            self.std_return = std
            AUDUSD_return.run_sum_of_std += std
            AUDUSD_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            AUDUSD_return.run_sum_of_std -= pop_value
            avg_std = AUDUSD_return.run_sum_of_std/(AUDUSD_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[11]:


# Define the GBPEUR_return class - each instance will store one row from the dataframe
class GBPEUR_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if GBPEUR_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - GBPEUR_return.last_price) / GBPEUR_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            GBPEUR_return.run_sum = 0
        else:
            # Increment the counter
            if GBPEUR_return.num < 5:
                GBPEUR_return.num += 1
            GBPEUR_return.run_sum += hist_return
        GBPEUR_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            GBPEUR_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            GBPEUR_return.run_sum -= pop_value
            avg = GBPEUR_return.run_sum/(GBPEUR_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(GBPEUR_return.run_squared_sum/(GBPEUR_return.num))
            self.std_return = std
            GBPEUR_return.run_sum_of_std += std
            GBPEUR_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            GBPEUR_return.run_sum_of_std -= pop_value
            avg_std = GBPEUR_return.run_sum_of_std/(GBPEUR_return.num) 
            self.avg_of_std_return = avg_std 
            return avg_std


# In[12]:


# Define the USDCAD_return class - each instance will store one row from the dataframe
class USDCAD_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):

        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDCAD_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDCAD_return.last_price) / USDCAD_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDCAD_return.run_sum = 0
        else:
            # Increment the counter
            if USDCAD_return.num < 5:
                USDCAD_return.num += 1
            USDCAD_return.run_sum += hist_return
        USDCAD_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDCAD_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCAD_return.run_sum -= pop_value
            avg = USDCAD_return.run_sum/(USDCAD_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDCAD_return.run_squared_sum/(USDCAD_return.num))
            self.std_return = std
            USDCAD_return.run_sum_of_std += std
            USDCAD_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCAD_return.run_sum_of_std -= pop_value
            avg_std = USDCAD_return.run_sum_of_std/(USDCAD_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[13]:


# Define the USDJPY_return class - each instance will store one row from the dataframe
class USDJPY_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDJPY_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDJPY_return.last_price) / USDJPY_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDJPY_return.run_sum = 0
        else:
            # Increment the counter
            if USDJPY_return.num < 5:
                USDJPY_return.num += 1
            USDJPY_return.run_sum += hist_return
        USDJPY_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDJPY_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDJPY_return.run_sum -= pop_value
            avg = USDJPY_return.run_sum/(USDJPY_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDJPY_return.run_squared_sum/(USDJPY_return.num))
            self.std_return = std
            USDJPY_return.run_sum_of_std += std
            USDJPY_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDJPY_return.run_sum_of_std -= pop_value
            avg_std = USDJPY_return.run_sum_of_std/(USDJPY_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[14]:


# Define the USDMXN_return class - each instance will store one row from the dataframe
class USDMXN_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDMXN_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDMXN_return.last_price) / USDMXN_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDMXN_return.run_sum = 0
        else:
            # Increment the counter
            if USDMXN_return.num < 5:
                USDMXN_return.num += 1
            USDMXN_return.run_sum += hist_return
        USDMXN_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDMXN_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDMXN_return.run_sum -= pop_value
            avg = USDMXN_return.run_sum/(USDMXN_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDMXN_return.run_squared_sum/(USDMXN_return.num))
            self.std_return = std
            USDMXN_return.run_sum_of_std += std
            USDMXN_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDMXN_return.run_sum_of_std -= pop_value
            avg_std = USDMXN_return.run_sum_of_std/(USDMXN_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[15]:


# Define the EURUSD_return class - each instance will store one row from the dataframe
class EURUSD_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if EURUSD_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - EURUSD_return.last_price) / EURUSD_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            EURUSD_return.run_sum = 0
        else:
            # Increment the counter
            if EURUSD_return.num < 5:
                EURUSD_return.num += 1
            EURUSD_return.run_sum += hist_return
        EURUSD_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            EURUSD_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            EURUSD_return.run_sum -= pop_value
            avg = EURUSD_return.run_sum/(EURUSD_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(EURUSD_return.run_squared_sum/(EURUSD_return.num))
            self.std_return = std
            EURUSD_return.run_sum_of_std += std
            EURUSD_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            EURUSD_return.run_sum_of_std -= pop_value
            avg_std = EURUSD_return.run_sum_of_std/(EURUSD_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[16]:


# Define the USDCNY_return class - each instance will store one row from the dataframe
class USDCNY_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDCNY_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDCNY_return.last_price) / USDCNY_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDCNY_return.run_sum = 0
        else:
            # Increment the counter
            if USDCNY_return.num < 5:
                USDCNY_return.num += 1
            USDCNY_return.run_sum += hist_return
        USDCNY_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDCNY_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCNY_return.run_sum -= pop_value
            avg = USDCNY_return.run_sum/(USDCNY_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDCNY_return.run_squared_sum/(USDCNY_return.num))
            self.std_return = std
            USDCNY_return.run_sum_of_std += std
            USDCNY_return.run_squared_sum = 0
   
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCNY_return.run_sum_of_std -= pop_value
            avg_std = USDCNY_return.run_sum_of_std/(USDCNY_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std
    


# In[17]:
# Define the USDCZK_return class - each instance will store one row from the dataframe
class USDCZK_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDCZK_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDCZK_return.last_price) / USDCZK_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDCZK_return.run_sum = 0
        else:
            # Increment the counter
            if USDCZK_return.num < 5:
                USDCZK_return.num += 1            
            USDCZK_return.run_sum += hist_return
        USDCZK_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDCZK_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCZK_return.run_sum -= pop_value
            avg = USDCZK_return.run_sum/(USDCZK_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDCZK_return.run_squared_sum/(USDCZK_return.num))
            self.std_return = std
            USDCZK_return.run_sum_of_std += std
            USDCZK_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDCZK_return.run_sum_of_std -= pop_value
            avg_std = USDCZK_return.run_sum_of_std/(USDCZK_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[18]:


# Define the USDPLN_return class - each instance will store one row from the dataframe
class USDPLN_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDPLN_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDPLN_return.last_price) / USDPLN_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDPLN_return.run_sum = 0
        else:
            # Increment the counter
            if USDPLN_return.num < 5:
                USDPLN_return.num += 1
            USDPLN_return.run_sum += hist_return
        USDPLN_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDPLN_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDPLN_return.run_sum -= pop_value
            avg = USDPLN_return.run_sum/(USDPLN_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDPLN_return.run_squared_sum/(USDPLN_return.num))
            self.std_return = std
            USDPLN_return.run_sum_of_std += std
            USDPLN_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDPLN_return.run_sum_of_std -= pop_value
            avg_std = USDPLN_return.run_sum_of_std/(USDPLN_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


# In[19]:


# Define the USDINR_return class - each instance will store one row from the dataframe
class USDINR_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDINR_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDINR_return.last_price) / USDINR_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDINR_return.run_sum = 0
        else:
            # Increment the counter
            if USDINR_return.num < 5:
                USDINR_return.num += 1
            USDINR_return.run_sum += hist_return
        USDINR_return.last_price = avg_price
    
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDINR_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDINR_return.run_sum -= pop_value
            avg = USDINR_return.run_sum/(USDINR_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDINR_return.run_squared_sum/(USDINR_return.num))
            self.std_return = std
            USDINR_return.run_sum_of_std += std
            USDINR_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDINR_return.run_sum_of_std -= pop_value
            avg_std = USDINR_return.run_sum_of_std/(USDINR_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std

# Historical data used the russian ruble, but the live data no longer uses it. So we define a class 
# for it here. 
# Define the USDRUB_return class - each instance will store one row from the dataframe
class USDRUB_return(object):
    # Variable to store the total number of instantiated objects in this class
    num = 0
    # Variable to store the running sum of the return
    run_sum = 0 
    run_squared_sum = 0
    run_sum_of_std = 0
    last_price = -1
    
    # Init all the necessary variables when instantiating the class
    def __init__(self, tick_time, avg_price):
        
        # Store each column value into a variable in the class instance
        self.tick_time = tick_time
        #self.price = avg_price
        
        if USDRUB_return.last_price == -1:
            hist_return = float('NaN')
        else:
            hist_return = (avg_price - USDRUB_return.last_price) / USDRUB_return.last_price
        
        self.hist_return = hist_return
        if isnan(hist_return):
            USDRUB_return.run_sum = 0
        else:
            # Increment the counter
            if USDRUB_return.num < 5:
                USDRUB_return.num += 1
            USDRUB_return.run_sum += hist_return
        USDRUB_return.last_price = avg_price
        
    def add_to_running_squared_sum(self,avg):
        if isnan(self.hist_return) == False:
            USDRUB_return.run_squared_sum += (self.hist_return - avg)**2
    
    def get_avg(self,pop_value):
        if isnan(self.hist_return) == False:
            USDRUB_return.run_sum -= pop_value
            avg = USDRUB_return.run_sum/(USDRUB_return.num)
            self.avg_return = avg
            return avg
    
    def get_std(self):
        if isnan(self.hist_return) == False:
            std = sqrt(USDRUB_return.run_squared_sum/(USDRUB_return.num))
            self.std_return = std
            USDRUB_return.run_sum_of_std += std
            USDRUB_return.run_squared_sum = 0
            return std
    
    def get_avg_std(self,pop_value):
        if isnan(self.hist_return) == False:
            USDRUB_return.run_sum_of_std -= pop_value
            avg_std = USDRUB_return.run_sum_of_std/(USDRUB_return.num)
            self.avg_of_std_return = avg_std 
            return avg_std


class library_main:
    """
    This class will request currency conversion data via Polygon API, convert and store them
    """

    def __init__(self):
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"

    def ts_to_datetime(self, ts) -> str:
        """convert the data type of timestamp

        Parameters
        ----------
        ts:timestamp
        """
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    def reset_raw_data_tables(self, engine, currency_pairs):
        with engine.begin() as conn:
            for curr in currency_pairs:
                conn.execute(text("DROP TABLE "+curr[0]+curr[1]+"_raw;"))
                conn.execute(text(
                    "CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text);"))

    def initialize_raw_data_tables(self, engine, currency_pairs):
        with engine.begin() as conn:
            for curr in currency_pairs:
                conn.execute(text(
                    "CREATE TABLE "+curr[0]+curr[1]+"_raw(ticktime text, fxrate  numeric, inserttime text);"))

    def initialize_aggregated_tables(self, engine, currency_pairs):
        with engine.begin() as conn:
            for curr in currency_pairs:
                conn.execute(text(
                    "CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate  numeric, stdfxrate numeric);"))

    def initialize_kelter_tables(self, engine, currency_pairs):
        with engine.begin() as conn:
            for curr in currency_pairs:
                conn.execute(text(
                    "CREATE TABLE "+curr[0]+curr[1]+"_kelter(Period numeric, Max numeric, Min numeric, Mean numeric, VOL numeric, FD numeric);"))

    def aggregate_kelter_tables(self, engine, currency_pairs, last_dict, period):
        with engine.begin() as conn:
            for curr in currency_pairs:
                key = curr[0] + "_" + curr[1]
                max = last_dict[key][0]
                min = last_dict[key][1]
                mean = round(last_dict[key][2], 10)
                vol = round(max - min, 6)
                if vol == 0:
                    conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_kelter(Period, Max, Min, Mean, VOL) VALUES (:Period, :Max, :Min, :Mean, :VOL)"), [
                        {"Period": period, "Max": max, "Min": min, "Mean": mean, "VOL": vol}])
                else:
                    FD = round(last_dict[key][3] / vol, 10)
                    conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_kelter(Period, Max, Min, Mean, VOL, FD) VALUES (:Period, :Max, :Min, :Mean, :VOL, :FD)"), [
                        {"Period": period, "Max": max, "Min": min, "Mean": mean, "VOL": vol, "FD": FD}])

    def aggregate_raw_data_tables(self, engine, currency_pairs):
        with engine.begin() as conn:
            for curr in currency_pairs:
                result = conn.execute(text(
                    "SELECT AVG(fxrate) as avg_price, COUNT(fxrate) as tot_count FROM "+curr[0]+curr[1]+"_raw;"))
                for row in result:
                    avg_price = row.avg_price
                    tot_count = row.tot_count
                std_res = conn.execute(text("SELECT SUM((fxrate - "+str(avg_price)+")*(fxrate - "+str(
                    avg_price)+"))/("+str(tot_count)+"-1) as std_price FROM "+curr[0]+curr[1]+"_raw;"))
                for row in std_res:
                    std_price = sqrt(row.std_price)
                date_res = conn.execute(
                    text("SELECT MAX(ticktime) as last_date FROM "+curr[0]+curr[1]+"_raw;"))
                for row in date_res:
                    last_date = row.last_date
                conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg (inserttime, avgfxrate, stdfxrate) VALUES (:inserttime, :avgfxrate, :stdfxrate);"), [
                             {"inserttime": last_date, "avgfxrate": avg_price, "stdfxrate": std_price}])

                # This calculates and stores the return values
                exec("curr[2].append("+curr[0]+curr[1] +
                     "_return(last_date,avg_price))")
                #exec("print(\"The return for "+curr[0]+curr[1]+" is:"+str(curr[2][-1].hist_return)+" \")")

                if len(curr[2]) > 5:
                    try:
                        avg_pop_value = curr[2][-6].hist_return
                    except:
                        avg_pop_value = 0
                    if isnan(avg_pop_value) == True:
                        avg_pop_value = 0
                else:
                    avg_pop_value = 0
                # Calculate the average return value and print it/store it
                curr_avg = curr[2][-1].get_avg(avg_pop_value)
                #exec("print(\"The average return for "+curr[0]+curr[1]+" is:"+str(curr_avg)+" \")")

                # Now that we have the average return, loop through the last 5 rows in the list to start compiling the
                # data needed to calculate the standard deviation
                for row in curr[2][-5:]:
                    row.add_to_running_squared_sum(curr_avg)

                # Calculate the standard dev using the avg
                curr_std = curr[2][-1].get_std()
                #exec("print(\"The standard deviation of the return for "+curr[0]+curr[1]+" is:"+str(curr_std)+" \")")

                # Calculate the average standard dev
                if len(curr[2]) > 5:
                    try:
                        pop_value = curr[2][-6].std_return
                    except:
                        pop_value = 0
                else:
                    pop_value = 0
                curr_avg_std = curr[2][-1].get_avg_std(pop_value)
                #exec("print(\"The average standard deviation of the return for "+curr[0]+curr[1]+" is:"+str(curr_avg_std)+" \")")

                # -------------------Investment Strategy-----------------------------------------------
                try:
                    return_value = curr[2][-1].hist_return
                except:
                    return_value = 0
                if isnan(return_value) == True:
                    return_value = 0

                try:
                    return_value_1 = curr[2][-2].hist_return
                except:
                    return_value_1 = 0
                if isnan(return_value_1) == True:
                    return_value_1 = 0

                try:
                    return_value_2 = curr[2][-3].hist_return
                except:
                    return_value_2 = 0
                if isnan(return_value_2) == True:
                    return_value_2 = 0

                try:
                    upp_band = curr[2][-1].avg_return + \
                        (1.5 * curr[2][-1].std_return)
                    # (return_value > 0) and (return_value_1 > 0) and
                    if return_value >= upp_band and curr[3].Prev_Action_was_Buy == True and return_value != 0:
                        curr[3].sell_curr(avg_price)
                except:
                    pass

                try:
                    loww_band = curr[2][-1].avg_return - \
                        (1.5 * curr[2][-1].std_return)
                    # and  (return_value < 0) and (return_value_1 < 0)
                    if return_value <= loww_band and curr[3].Prev_Action_was_Buy == False and return_value != 0:
                        curr[3].buy_curr(avg_price)
                except:
                    pass

    # This function is called every 6 minutes to aggregate the data, make the necessary calculations,
    # and make a decision about buying
    def offline_aggregate_raw_data_tables(engine, currency_pairs):
        with engine.begin() as conn:
            for curr in currency_pairs:
                result = conn.execute(
                    text("SELECT inserttime, avgfxrate FROM "+curr[0]+curr[1]+"_agg;"))
                for row in result:
                    avg_price = row.avgfxrate
                    last_date = row.inserttime

                    # This calculates and stores the return values
                    exec("curr[2].append("+curr[0]+curr[1] +
                         "_return(last_date,avg_price))")
                    #exec("print(\"The return for "+curr[0]+curr[1]+" is:"+str(curr[2][-1].hist_return)+" \")")

                    if len(curr[2]) > 5:
                        try:
                            avg_pop_value = curr[2][-6].hist_return
                        except:
                            avg_pop_value = 0
                        if isnan(avg_pop_value) == True:
                            avg_pop_value = 0
                    else:
                        avg_pop_value = 0
                    # Calculate the average return value and print it/store it
                    curr_avg = curr[2][-1].get_avg(avg_pop_value)
                    #exec("print(\"The average return for "+curr[0]+curr[1]+" is:"+str(curr_avg)+" \")")

                    # Now that we have the average return, loop through the last 5 rows in the list to start compiling the
                    # data needed to calculate the standard deviation
                    for row in curr[2][-5:]:
                        row.add_to_running_squared_sum(curr_avg)

                    # Calculate the standard dev using the avg
                    curr_std = curr[2][-1].get_std()
                    #exec("print(\"The standard deviation of the return for "+curr[0]+curr[1]+" is:"+str(curr_std)+" \")")

                    # Calculate the average standard dev
                    if len(curr[2]) > 5:
                        try:
                            pop_value = curr[2][-6].std_return
                        except:
                            pop_value = 0
                    else:
                        pop_value = 0
                    curr_avg_std = curr[2][-1].get_avg_std(pop_value)
                    #exec("print(\"The average standard deviation of the return for "+curr[0]+curr[1]+" is:"+str(curr_avg_std)+" \")")

                    # -------------------Investment Strategy-----------------------------------------------
                    try:
                        return_value = curr[2][-1].hist_return
                    except:
                        return_value = 0
                    if isnan(return_value) == True:
                        return_value = 0

                    try:
                        return_value_1 = curr[2][-2].hist_return
                    except:
                        return_value_1 = 0
                    if isnan(return_value_1) == True:
                        return_value_1 = 0

                    try:
                        return_value_2 = curr[2][-3].hist_return
                    except:
                        return_value_2 = 0
                    if isnan(return_value_2) == True:
                        return_value_2 = 0

                    try:
                        upp_band = curr[2][-1].avg_return + \
                            (1.5 * curr[2][-1].std_return)
                        # (return_value > 0) and (return_value_1 > 0) and
                        if return_value >= upp_band and curr[3].Prev_Action_was_Buy == True and return_value != 0:
                            curr[3].sell_curr(avg_price)
                    except:
                        pass

                    try:
                        loww_band = curr[2][-1].avg_return - \
                            (1.5 * curr[2][-1].std_return)
                        # and  (return_value < 0) and (return_value_1 < 0)
                        if return_value <= loww_band and curr[3].Prev_Action_was_Buy == False and return_value != 0:
                            curr[3].buy_curr(avg_price)
                    except:
                        pass

    def collect_data(self, currency_pairs):
        # Number of list iterations - each one should last about 1 second
        count = 0
        agg_count = 0

        # Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
        engine = create_engine(
            "sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)

        # Create the needed tables in the database
        self.initialize_raw_data_tables(engine, currency_pairs)
        self.initialize_aggregated_tables(engine, currency_pairs)

        # Open a RESTClient for making the api calls
        with RESTClient(self.key) as client:
            # Loop that runs until the total duration of the program hits 24 hours.
            while count < 361:
                # Actually we need collect data of 24 hours, but we are not authorized
                # to access to the database. So let's try just one time
                # Make a check to see if 6 minutes has been reached or not
                if agg_count == 360:
                    # Aggregate the data and clear the raw data tables
                    self.aggregate_raw_data_tables(engine, currency_pairs)
                    self.reset_raw_data_tables(engine, currency_pairs)
                    agg_count = 0

                # Only call the api every 1 second, so wait here for 0.85 seconds, because the
                # code takes about .15 seconds to run
                time.sleep(0.85)

                # Increment the counters
                count += 1
                agg_count += 1

                # Loop through each currency pair
                for currency in currency_pairs:
                    # Set the input variables to the API
                    from_ = currency[0]
                    to = currency[1]

                    # Call the API with the required parameters
                    try:
                        resp = client.forex_currencies_real_time_currency_conversion(
                            from_, to, amount=100, precision=2)
                    except:
                        continue

                    # This gets the Last Trade object defined in the API Resource
                    last_trade = resp.last

                    # Format the timestamp from the result
                    dt = self.ts_to_datetime(last_trade["timestamp"])

                    # Get the current time and format it
                    insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # Calculate the price by taking the average of the bid and ask prices
                    avg_price = (last_trade['bid'] + last_trade['ask'])/2
                    print(from_, to, avg_price)

                    # Write the data to the SQLite database, raw data tables
                    with engine.begin() as conn:
                        conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"), [
                                     {"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])

    def collect_data_N(self, currency_pairs):
        # Number of list iterations - each one should last about 1 second
        last_dict = {
            "AUD_USD": [0, 999, 0, 0],
            "GBP_EUR": [0, 999, 0, 0],
            "USD_CAD": [0, 999, 0, 0],
            "USD_JPY": [0, 999, 0, 0],
            "USD_MXN": [0, 999, 0, 0],
            "EUR_USD": [0, 999, 0, 0],
            "USD_CNY": [0, 999, 0, 0],
            "USD_CZK": [0, 999, 0, 0],
            "USD_PLN": [0, 999, 0, 0],
            "USD_INR": [0, 999, 0, 0]
        }
        last_price_dict = {
            "AUD_USD": 0,
            "GBP_EUR": 2,
            "USD_CAD": 2,
            "USD_JPY": 2,
            "USD_MXN": 2,
            "EUR_USD": 2,
            "USD_CNY": 2,
            "USD_CZK": 2,
            "USD_PLN": 2,
            "USD_INR": 2
        }
        kelter_dict = {
            "AUD_USD": [],
            "GBP_EUR": [],
            "USD_CAD": [],
            "USD_JPY": [],
            "USD_MXN": [],
            "EUR_USD": [],
            "USD_CNY": [],
            "USD_CZK": [],
            "USD_PLN": [],
            "USD_INR": []
        }
        count = 0
        agg_count = 0
        period = 1

        # Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
        engine = create_engine(
            "sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)

        # Create the needed tables in the database
        self.initialize_raw_data_tables(engine, currency_pairs)
        self.initialize_kelter_tables(engine, currency_pairs)

        # Open a RESTClient for making the api calls
        client = RESTClient(self.key)
        # Loop that runs until the total duration of the program hits 10 hours.
        while count < 36001:
            # Actually we need collect data of  hours, but we are not authorized
            # to access to the database. So let's try just one time
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 360:
                # Aggregate the data and clear the raw data tables
                self.aggregate_kelter_tables(
                    engine, currency_pairs, last_dict, period)
                for curr in currency_pairs:
                    key = curr[0] + "_" + curr[1]
                    kelter_dict[key] = []
                    mean = last_dict[key][2]
                    vol = last_dict[key][0] - last_dict[key][1]
                    for i in [x for x in range(-100, 101) if x != 0]:
                        band = mean + 0.025 * i * vol
                        kelter_dict[key].append(band)

                last_dict = {
                    "AUD_USD": [0, 999, 0, 0],
                    "GBP_EUR": [0, 999, 0, 0],
                    "USD_CAD": [0, 999, 0, 0],
                    "USD_JPY": [0, 999, 0, 0],
                    "USD_MXN": [0, 999, 0, 0],
                    "EUR_USD": [0, 999, 0, 0],
                    "USD_CNY": [0, 999, 0, 0],
                    "USD_CZK": [0, 999, 0, 0],
                    "USD_PLN": [0, 999, 0, 0],
                    "USD_INR": [0, 999, 0, 0]
                }
                agg_count = 0
                period += 1

            # Only call the api every 1 second, so wait here for 0.85 seconds, because the
            # code takes about .25 seconds to run
            time.sleep(0.45)

            # Increment the counters
            count += 1
            agg_count += 1

            # Loop through each currency pair
            for currency in currency_pairs:
                # Set the input variables to the API
                from_ = currency[0]
                to = currency[1]
                key = from_+"_"+to

                # Call the API with the required parameters
                try:
                    resp = client.get_real_time_currency_conversion(
                        from_, to, amount=100, precision=2)
                except:
                    continue

                # This gets the Last Trade object defined in the API Resource
                last_trade = resp.last

                # Format the timestamp from the result
                dt = self.ts_to_datetime(last_trade.timestamp)

                # Get the current time and format it
                insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Calculate the price by taking the average of the bid and ask prices
                avg_price = round((last_trade.bid + last_trade.ask)/2, 6)
                last_price = last_price_dict[key]

                if((avg_price - 1) * (last_price - 1) < 0):
                    avg_price = round(1/avg_price, 6)

                if(avg_price > last_dict[key][0]):
                    last_dict[key][0] = avg_price

                if(avg_price < last_dict[key][1]):
                    last_dict[key][1] = avg_price

                last_dict[key][2] = (
                    (last_dict[key][2] * (agg_count - 1)) + avg_price) / agg_count

                if avg_price >= last_price:
                    count_cross = np.histogram(kelter_dict[key], bins=[
                                               last_price, avg_price])
                else:
                    count_cross = np.histogram(kelter_dict[key], bins=[
                                               avg_price, last_price])

                last_dict[key][3] += count_cross[0][0]

                # Write the data to the SQLite database, raw data tables
                with engine.begin() as conn:
                    conn.execute(text("INSERT INTO "+from_+to+"_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"), [
                                 {"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])

                # print(last_price, avg_price,
                #       last_dict[key][0], last_dict[key][1], last_dict[key][2])
                last_price_dict[key] = avg_price

    def test_group_daily(self, date):
        with RESTClient(self.key) as client:
            test = client.forex_currencies_grouped_daily(date)
        return test

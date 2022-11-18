#!/usr/bin/env python
# coding: utf-8

# # Final Exam: Load and analyze data from Polygon in real time

# 1. I noticed that buying/selling anytime that there were 2 or 3 consecutive returns in the same direction lead to rather ocsilatory profits, because it is a very common occurance to have 2/3 consecutive returns in the same direction. To pick out a stronger signal of returns, I decided to try to couple that strategy with a strategy of buying and selling when the returns cross the Bollinger bands constructed out of the moving averages of returns. In order to test these hypotheses, I calculated the profits/losses for just using the first strategy, using both strategies, and using only the latter strategy. It turns out that the profits/losses turned out better when only using the second strategy (Bollinger bands for the returns). I also tried 3 different levels of bolliinger bands for each experienmt (1.25, 1.5, and 1.75). Bollinger bands using 1.5 times the standard deviation ended up being the ideal way to get the best profits. 
# 2. My investment strategy was to buy/sell any time that the return value was outside the Bollinger bands that were created using the returns. 
# 3. The code that was used for analysis/strategy development based on histocal data is included towards the end of this file. Please note, this was dataset used to develop a strategy was not a complete 24 hours worth of data because of poor logistical planning on my part. 
# 4. Please note: I use the original formula for returns, so the first 6 minute checkpoint doesn't yeild any interesting results, because there is no previous price to compare with yet. 

# In[9]:


# Import required libraries
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

from Library import library_main
# I import the main function from the library that I created
# In[20]:


# We can buy, sell, or do nothing each time we make a decision.
# This class defies a nobject for keeping track of our current investments/profits for each currency pair
class portfolio(object):
    def __init__(self,from_,to):
        # Initialize the 'From' currency amont to 1
        self.amount = 1
        self.curr2 = 0
        self.from_ = from_
        self.to = to
        # We want to keep track of state, to see what our next trade should be
        self.Prev_Action_was_Buy = False
    
    # This defines a function to buy the 'To' currency. It will always buy the max amount, in whole number
    # increments
    def buy_curr(self, price):
        if self.amount >= 1:
            num_to_buy = floor(self.amount)
            self.amount -= num_to_buy
            self.Prev_Action_was_Buy = True
            self.curr2 += num_to_buy*price
            print("Bought %d worth of the target currency (%s). Our current profits and losses in the original currency (%s) are: %f." % (num_to_buy,self.to,self.from_,(self.amount-1)))
        else:
            print("There was not enough of the original currency (%s) to make another buy." % self.from_)
    # This defines a function to sell the 'To' currency. It will always sell the max amount, in a whole number
    # increments
    def sell_curr(self, price):
        if self.curr2 >= 1:
            num_to_sell = floor(self.curr2)
            self.amount += num_to_sell * (1/price)
            self.Prev_Action_was_Buy = False
            self.curr2 -= num_to_sell
            print("Sold %d worth of the target currency (%s). Our current profits and losses in the original currency (%s) are: %f." % (num_to_sell,self.to,self.from_,(self.amount-1)))
        else:
            print("There was not enough of the target currency (%s) to make another sell." % self.to)   


# In[21]:


# In[22]:


# A dictionary defining the set of currency pairs we will be pulling data for
currency_pairs = [["AUD","USD",[],portfolio("AUD","USD")],
                  ["GBP","EUR",[],portfolio("GBP","EUR")],
                  ["USD","CAD",[],portfolio("USD","CAD")],
                  ["USD","JPY",[],portfolio("USD","JPY")],
                  ["USD","MXN",[],portfolio("USD","MXN")],
                  ["EUR","USD",[],portfolio("EUR","USD")],
                  ["USD","CNY",[],portfolio("USD","CNY")],
                  ["USD","CZK",[],portfolio("USD","CZK")],
                  ["USD","PLN",[],portfolio("USD","PLN")],
                  ["USD","INR",[],portfolio("USD","INR")]]

# Run the main data collection loop
Dp = library_main()
Dp.collect_data_N(currency_pairs)
#Here is the main function from library

# # The following code blocks were used on historical data to fomulate a strategy

# In[ ]:


# In[ ]:


# This function is called every 6 minutes to aggregate the data, make the necessary calculations, 
# and make a decision about buying
def offline_aggregate_raw_data_tables(engine,currency_pairs):
    with engine.begin() as conn:
        for curr in currency_pairs:
            result = conn.execute(text("SELECT inserttime, avgfxrate FROM "+curr[0]+curr[1]+"_agg;"))
            for row in result:
                avg_price = row.avgfxrate
                last_date = row.inserttime
                
                # This calculates and stores the return values
                exec("curr[2].append("+curr[0]+curr[1]+"_return(last_date,avg_price))")
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
                    upp_band = curr[2][-1].avg_return + (1.5 * curr[2][-1].std_return)
                    if return_value >= upp_band and curr[3].Prev_Action_was_Buy == True and return_value != 0: #  (return_value > 0) and (return_value_1 > 0) and   
                        curr[3].sell_curr(avg_price)
                except:
                    pass
                
                try:
                    loww_band = curr[2][-1].avg_return - (1.5 * curr[2][-1].std_return)
                    if return_value <= loww_band and curr[3].Prev_Action_was_Buy == False and return_value != 0: # and  (return_value < 0) and (return_value_1 < 0)
                        curr[3].buy_curr(avg_price)
                except:
                    pass

# A dictionary defining the set of currency pairs we will be pulling data for
currency_pairs = [["AUD","USD",[],portfolio("AUD","USD")],
                  ["GBP","EUR",[],portfolio("GBP","EUR")],
                  ["USD","CAD",[],portfolio("USD","CAD")],
                  ["USD","JPY",[],portfolio("USD","JPY")],
                  ["USD","MXN",[],portfolio("USD","MXN")],
                  ["EUR","USD",[],portfolio("EUR","USD")],
                  ["USD","RUB",[],portfolio("USD","RUB")],
                  ["USD","CZK",[],portfolio("USD","CZK")],
                  ["USD","PLN",[],portfolio("USD","PLN")],
                  ["USD","INR",[],portfolio("USD","INR")]]

# Function to run the necessary testing on offline data
def main_offline(currency_pairs):
    # Create an engine to connect to the database
    engine = create_engine('sqlite:///final.db', echo=False, future=True)
    offline_aggregate_raw_data_tables(engine,currency_pairs) 
   
           


# In[ ]:


main_offline(currency_pairs)


# In[ ]:


# This section plots the historical returns with their corressponding bollinger bands. It also
# prints the total profits/losses for each currency pair, and the total across all currency pairs. 

# Create a subplot
fig, axs = plt.subplots(10,figsize=(10,40))
fig.tight_layout()

# Variable to keep track of the total profit across currency pairs. 
tot_profit = 0

# Loop through the currency pairs
for ind, currency in enumerate(currency_pairs):
    
    from_ = currency[0]
    to = currency[1]
    
    # The sublists in the following list represent each of the following:
    # hist_return, avg_return, std_return, avg_of_std_return, upper bollinger, lower bollinger
    returns_array = [[],[],[],[],[],[]]
    
    # Extract the data from the classes and put it into a single list for plotting
    for row in currency[2]:
        returns_array[0].append(row.hist_return)
        try:
            returns_array[1].append(row.avg_return)
        except:
            returns_array[1].append(0)
        try:
            returns_array[2].append(row.std_return)
        except:
            returns_array[2].append(0)
        try:
            returns_array[3].append(row.avg_of_std_return)
        except:
            returns_array[3].append(0)
        try:
            returns_array[4].append(row.avg_return + (1.5 * row.std_return))
        except:
            returns_array[4].append(0)
        try:
            returns_array[5].append(row.avg_return - (1.5 * row.std_return))
        except:
            returns_array[5].append(0)
            
    print("The profit/losses for "+from_+to+" calculated with numpy is: %f" % (currency[3].amount -1))
    tot_profit += currency[3].amount - 1
    
    # Plot the line graphs with bollinger bands using the propper formatting
    axs[ind].plot(range(0,len(returns_array[0])),returns_array[0]) # plot the historical returns
    axs[ind].plot(range(0,len(returns_array[4])),returns_array[4]) # plot the upper bollinger band for returns
    axs[ind].plot(range(0,len(returns_array[5])),returns_array[5]) # plot the lower bollinger band for returns
    axs[ind].set(xlabel='Time',ylabel='Return')
    axs[ind].set_title(from_+to+'  Returns Over Time')
    
# Extra formatting to make sure the axis labels do not overlap the titles
plt.subplots_adjust(left=0.1,
                    bottom=0.1, 
                    right=0.9, 
                    top=0.9, 
                    wspace=0.4, 
                    hspace=0.4)

print("Total profit across currencies is: %f" % tot_profit)


# -*- coding: utf-8 -*-
"""
Created on Thu May  4 10:48:37 2017

@author: ciao
"""

import numpy as np
import pandas as pd
import pandas_datareader.data as web
#import matplotlib.pyplot as plt
import scipy.optimize as sco
import scipy.interpolate as sci


# ======================================================== Portfolio Optimization =================================================
# ==================================================================================================================================
tickers = ['IBM','MSFT','YHOO','SBUX','NVDA']
numasst = len(tickers)  #number of assets

data = pd.DataFrame()
for ticker in tickers:
    data[ticker] = web.DataReader(ticker, data_source='yahoo',start = '2010-04-27', end='2017-04-27')['Adj Close']

''' DJIA prices as benchmark for regression analysis'''
DJ = pd.DataFrame()
DJ['DJIA'] = web.DataReader('DJIA', data_source='yahoo',start = '2010-04-27', end='2017-04-27')['Adj Close']


'''Generate random portfolio weights'''
weights = np.random.random(numasst)
weights /=np.sum(weights)  #print out an array of randomly generated weights for each stock

data.columns = tickers
#(data/data.ix[0]).plot(figsize=(8,5))  #normalize the data as the oldest price is 1 to calculate log returns
rtns = np.log(data/data.shift(1)) #log returns with lag = 1
#returns.plot(figsize=(8,5))
rtns.mean()*252 #annulized returns based on 252 trading days
#rtns.mean()
rtns.cov()*252
#rtns.cov()

'''Define a function to return portfolio basic statistics'''
def PFstats(weights):
    weights = np.array(weights)
    weights /=np.sum(weights)
    pfrtn = np.sum(rtns.mean()*weights)*252
    #pfrtn = np.sum(rtns.mean()*weights)
    pfstdv = np.sqrt(np.dot(weights.T, np.dot(rtns.cov()*252, weights)))   # dot product of two vectors, i.e., returns and weights
    #pfstdv = np.sqrt(np.dot(weights.T, np.dot(rtns.cov(), weights)))
    return np.array([pfrtn, pfstdv, pfrtn/pfstdv])

'''Define a function to compute Sharpe Ratio'''
def get_sharpe(weights):
    '''the third column of PFstats is the Sharpe Ratio'''
    return -PFstats(weights)[2]  

'''Constrains for optimization'''
cons = ({'type': 'eq', 'fun':lambda x: np.sum(x)-1})  
# The minimize function provides a common interface to unconstrained and 
#constrained minimization algorithms for multivariate scalar functions in scipy.optimize
'''Set up boundary constraints so that short-selling is not allowed'''
boundary = tuple((0, 1) for x in range(numasst))  
# Weight (params) values are between 0 and 1 because NO short-selling
# Strategy 1 all the funds will be distributed to those five stocks and all positions are long

'''Use equal distributions for initial guess of the optimal weight'''
''' --- >Get the values from Twitter Frequency<---'''
init_guess = numasst*[1./numasst,]  #1. means float; comma means to separate the values in a tuple by comma

'''Optimization using scipy.optimize'''
optim = sco.minimize(get_sharpe, init_guess, method='SLSQP', bounds=boundary, constraints=cons)
# SLSQP stands for Sequential Least Squares Programming optimization algorithm
optim['x'].round(6)

'''The mean return, mean volatility, and Sharpe Ratio using the weights from the optimization'''
PFstats(optim['x']).round(6)

'''Find the MVP'''
def get_MVP(weights):
    '''the second column of PFstats is Variance'''
    return PFstats(weights)[1] ** 2

minvar = sco.minimize(get_MVP, init_guess, method='SLSQP', bounds=boundary, constraints=cons)
minvar['x'].round(6)
PFstats(minvar['x']).round(6)


'''Find the Efficient Frontier'''
cons = ({'type': 'eq', 'fun':lambda x: PFstats(x)[0] - tgrtn},
        {'type': 'eq', 'fun':lambda x: np.sum(x)-1})
boundary_new = tuple((0, 1) for x in weights)

def get_EF(weights):
    '''the second column of PFstats is mean Volatility'''
    return PFstats(weights)[1]

'''Solve the Markowitz Problem (target return and volatility)'''
tgrtns = np.linspace(0.0, 0.25)  #np.linespace(start,stop,num=num evenly spaced samples <default is 50>)
tgstdv = []
for tgrtn in tgrtns:
    cons = ({'type': 'eq', 'fun':lambda x: PFstats(x)[0] - tgrtn},
        {'type': 'eq', 'fun':lambda x: np.sum(x)-1})
    results = sco.minimize(get_EF, init_guess, method='SLSQP', bounds=boundary_new, constraints=cons)
    tgstdv.append(results['fun'])
tgstdv = np.array(tgstdv)
 

''' Find CML using CAPM with risk-free rate being 5%'''
#For the spline interpolation, only consider portfolios under the EF
i = np.argmin(tgstdv)  #return indices of the min values along an axis
pfbystdv = tgstdv[i:]
pfbyrtn = tgrtns[i:]

tck = sci.splrep(pfbyrtn,pfbystdv)  

def func(x):
    '''Spline approximation for the EF'''
    return sci.splev(x, tck, der=0)

def firstder(x):
    return sci.splev(x, tck, der=1)

def get_CAPM(param,rf=0.05):
    capm_1 = rf - param[0]
    capm_2 = rf + param[1]*param[2] - func(param[2])
    capm_3 = param[1] - firstder(param[2])
    
    return capm_1, capm_2, capm_3
    
# fsolve finds the roots of a polynomial; fsolve(equation,X0,...)
#where X0 is an array specifying the starting estimate for the roots of func(x) = 0
optim_rf = sco.fsolve(get_CAPM, [0.05,0.02,2])   #randomly chose beta and x 

#Check if all CAPMs are zero
np.round(get_CAPM(optim_rf),6)

'''
#=================================== Plots ===================================
"""Plot Capital Market Line with risk-free rate of 5%"""
plt.figure(figsize=(8,4))
cx = np.linspace(0.0,0.3)
plt.plot(cx, optim_rf[0]+optim_rf[1]*cx, 'k',lw=1.5)  #CML
plt.plot(pfbystdv,pfbyrtn,'b',lw=2.5)  #EF
plt.plot(optim_rf[2], func(optim_rf[2]), 'r*', markersize=10.0)
plt.grid(True)
plt.xlabel('Expected Volatility')
plt.ylabel('Expected Return')
plt.colorbar(label='Sharpe Ratio')
#=============================================================================
'''


'''Calculate weights of the optimal tangent portfolio with rf = 5%'''
cons_new =({'type': 'eq', 'fun':lambda x: PFstats(x)[0] - func(optim_rf[2])},
           {'type': 'eq', 'fun':lambda x: np.sum(x)-1})
results_new = sco.minimize(get_EF, init_guess, method='SLSQP', bounds=boundary_new, constraints=cons_new)

results_new['x'].round(6)

#New Sharpe Ratio with risk-free asset at 5%
sharpe_rf = PFstats(results_new['x'])[2].round(2)


index = [t for t in tickers]
w1 = np.split(optim['x'].round(4),numasst)
df1 = pd.DataFrame(w1, index=index)
w2 = np.split(results_new['x'].round(4),numasst)
df2 = pd.DataFrame(w2, index=index)


print(str('Optimal weights of each stock without risk-free asset are: '))
print(df1)
print(str('and the Sharpe Ratio is: ') + PFstats(optim['x'][2].round(4)))

print(str('Optimal weights of each stock with a risk-free asset are: '))
print(df2)
print(str('and the Sharpe Ratio is: ') + sharpe_rf)


  











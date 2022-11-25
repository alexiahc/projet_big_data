#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 15:46:33 2022

@author: alexi
"""

import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

#%%
df = pd.read_csv("2000.csv")

for col in ["ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]:
    df = df.drop(col, axis=1)
#%%

y = df.ArrDelay
X = df.drop('ArrDelay', axis=1)

X_train, X_test, y_train, y_test = train_test_split (X, y,
                                                     test_size=0.5, random_state=1)

X_train.dtypes

X_train.groupby('UniqueCarrier').count()
# col UniqueCarrier -> 11 diff peut faire one hot encoding 

d = X_train.groupby('TailNum').count()
d.sort_values(by=['Year'])
plt.hist(d.Year[d.Year<1400])

# TailNum 4030 diff -> garde les plus nombreux ? UNKNOWN 52000, max 1350, min à 1 
# a voir si untilise slmt les 10 avec le plus de repetition ou si fait des quantiles 
# en fonction du nombre d'occurences ? 
# peut regrouper ceux en dessous de 800 occurences ensemble dans 'autre' et garder le nom des autres 
# et faire one hot encoding avec 

d = X_train.groupby('Origin').count()
plt.hist(d.Year)

# peut regrouper ceux en dessous de 60 000 occurences ensemble comme 'autres' et garder 
# le vrai nom pour les autres puis faire one hot encoding

# meme chose pour dest (repartition similaire) 

# Tri des colonnes

# cancellation code -> inutiles 
# changer nan en 0 pour DepTime -> correspond à non départ et à nan pour y_train 
# valeur de non arrivage pour y_train ? 

y_train.describe()
# min à -90 et max à 100 
# choisit une valeur par défault ? ou peut garder nan ? 

# Flight number pas utile ? 

#%%

import seaborn as sns 
train = X_train.copy()
train['ArrDelay'] = y_train.copy()

correlation = train.corr()
k= train.shape[1]
cols = correlation.nlargest(k,'ArrDelay')['ArrDelay'].index

f, ax = plt.subplots(figsize = (14,12))
sns.heatmap(np.corrcoef(train[cols].values.T), vmax=.8, linewidths=0.01,square=True,annot=True,cmap='viridis',
            linecolor="white",xticklabels = cols.values ,annot_kws = {'size':12},yticklabels = cols.values)













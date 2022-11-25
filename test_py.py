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
                                                     test_size=0.6, random_state=1)

# col UniqueCarrier -> 11 diff peut faire one hot encoding 

# TailNum 4030 diff -> garde les plus nombreux ? UNKNOWN 52000, max 1350, min à 1 
# a voir si untilise slmt les 10 avec le plus de repetition ou si fait des quantiles 
# en fonction du nombre d'occurences ? 
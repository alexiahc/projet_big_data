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
plt.figure()
plt.hist(d.Year[d.Year<1400])

# TailNum 4030 diff -> garde les plus nombreux ? UNKNOWN 52000, max 1350, min à 1 
# a voir si untilise slmt les 10 avec le plus de repetition ou si fait des quantiles 
# en fonction du nombre d'occurences ? 
# peut regrouper ceux en dessous de 800 occurences ensemble dans 'autre' et garder le nom des autres 
# et faire one hot encoding avec 

d = X_train.groupby('Origin').count()
plt.figure()
plt.hist(d.Year)

# peut regrouper ceux en dessous de 60 000 occurences ensemble comme 'autres' et garder 
# le vrai nom pour les autres puis faire one hot encoding

# meme chose pour dest (repartition similaire) 

# Tri des colonnes

# cancellation code -> inutiles 
X_train.drop('CancellationCode', axis=1, inplace=True)

# changer nan en 0 pour DepTime -> correspond à non départ et à nan pour y_train 
# valeur de non arrivage pour y_train ? 
# X_train.DepTime.fillna(0)

# ou est-ce que on garde slmt les lignes avec un delais a reelment prevoir ? 
# di deptime est null alors arrdelay null aussi -> vire les lignes 
# correspondante 
# comment transcrire dans le modele ? ne prevoit que les données non nulles ? 
X_train = X_train[X_train.DepTime.isnull()==False]
index = X_train.index 

y_train = y_train[index]
y_train.describe()
# min à -100 et max 100 environ 
# pour les null, choisit une valeur par défault ? ou peut garder nan ? 
# supprimer les colonnes ? car null pas de sens ici et peut pas trop 
# remplacer sinon ça va fausser les resultats peut etre 

# supprime les lignes avec null 
y_train = y_train[y_train.isnull() == False]
index = y_train.index
X_train = X_train.loc[index]

# Flight number pas utile ? 

#%% 

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer

enc_cols = [col for col in X_train.columns if (X_train[col].dtype == "object")]

# flm de faire one hot encoding pour les tests pcq c long à calculer, c juste pour
# pouvoir faire la matrice de correlation et les tests 

encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value = -1)
enc_cols_train = pd.DataFrame(encoder.fit_transform(X_train[enc_cols]), columns = enc_cols)
enc_cols_train.index = X_train.index

num_cols = [col for col in X_train.columns if (X_train[col].dtype != "object")]
X_num = X_train[num_cols]

X_train = pd.concat([enc_cols_train, X_num], axis=1)

imp_cat = SimpleImputer(strategy='most_frequent')
columns = X_train.columns
index = X_train.index
X_train = pd.DataFrame(imp_cat.fit_transform(X_train), columns=columns, index=index)

scaler = StandardScaler()

# standardize the data 
X_train = pd.DataFrame(scaler.fit_transform(X_train), columns = X_train.columns) 

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

# sans données standardisée :
    
# correlation avec dep delay , taxi out, dep time ->> dep delay à 0.9 ?? bcp 
# supp CRS dep time (mm que dep time)
# supp crs elapsed time (mm que distance)

# avec données standardisées :
# correlation à moins de 0.001 -> pas de correlation lineaire 
# besoin de croiser des colonnes 

# dist et CRSelapsed tim tjr 0.99 corr -> garde qu'un 
# idem deptime et crsdeptime 















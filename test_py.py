#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 15:46:33 2022

@author: alexi
"""
#test
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
import seaborn as sns
import scipy.stats as stats

#%%
df = pd.read_csv("1994.csv")

# colonne a enlever des le debut (c'est dans le sujet)
df.drop(["ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", 
         "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", 
         "LateAircraftDelay"], axis=1, inplace=True)
#%%

y = df.ArrDelay
X = df.drop('ArrDelay', axis=1)

X_train, X_test, y_train, y_test = train_test_split (X, y,
                                                     test_size=0.9, random_state=1)

X_train.dtypes

X_train.groupby('UniqueCarrier').count()
# col UniqueCarrier -> 11 diff peut faire one hot encoding 

d = X_train.groupby('TailNum').count()
d.sort_values(by=['Year'], ascending=False, inplace=True)
plt.figure()
plt.hist(d.Year[d.Year<1400])

index = d.index 
cols_utiles = index[1:10]
def replace_categ(x):
    if x in cols_utiles:
        return x 
    else:
        return 'Autre'
X_train.TailNum = X_train.TailNum.map(replace_categ)

# TailNum 4030 diff -> garde les plus nombreux ? UNKNOWN 52000, max 1350, min à 1 
# a voir si utilise slmt les 10 avec le plus de repetition ou si fait des quantiles 
# en fonction du nombre d'occurences ? 
# peut regrouper ceux en dessous de 800 occurences ensemble dans 'autre' et garder le nom des autres 
# et faire one hot encoding avec 

d = X_train.groupby('Origin').count()
plt.figure()
plt.hist(d.Year)

index = d.index 
cols_utiles = index[1:10]
def replace_categ(x):
    if x in cols_utiles:
        return x 
    else:
        return 'Autre'
X_train.Origin = X_train.Origin.map(replace_categ)

# peut regrouper ceux en dessous de 60 000 occurences ensemble comme 'autres' et garder 
# le vrai nom pour les autres puis faire one hot encoding

d = X_train.groupby('Dest').count()
plt.figure()
plt.hist(d.Year)

index = d.index 
cols_utiles = index[1:10]
def replace_categ(x):
    if x in cols_utiles:
        return x 
    else:
        return 'Autre'
X_train.Dest = X_train.Dest.map(replace_categ)

# meme chose pour dest (repartition similaire) 


# Tri des colonnes

# cancellation code -> inutiles 
X_train.drop('CancellationCode', axis=1, inplace=True)

# Flight number pas utile ? 

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

#%%

plt.figure()
plt.hist(y_train)

plt.figure()
plt.hist(np.log(y_train[y_train !=0]))
# asymétrie pour les log(y_train) pour y_train non nul 
#-> peut rendre la forme de y_train normale pour les modèles (applique log et peut etre 
# asym)

plt.figure()
plt.hist(np.log(y_train[y_train!=0]),orientation = 'vertical',histtype = 'bar')

plt.figure()
sns.histplot(np.log(y_train[y_train!=0]), kde=True)

plt.figure()
sns.distplot(np.log(y_train[y_train!=0]), kde=False, fit=stats.lognorm)

plt.figure()
sns.distplot(np.log(y_train[y_train!=0]), kde=False, fit=stats.johnsonsu)


#%% PREPROCESSING FAIT ICI SLMT SUR TRAIN MAIS À FAIRE AUSSI SUR TEST 

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

# one hot encoding 

OH_cols = [col for col in X_train.columns if (X_train[col].dtype == "object")]

OH_encoder = OneHotEncoder(handle_unknown='ignore', sparse=False)

OH_encoder.fit(X_train[OH_cols])
OH_cols_name = OH_encoder.get_feature_names_out(OH_cols)

OH_cols_train = pd.DataFrame(OH_encoder.transform(X_train[OH_cols]), columns = OH_cols_name)
# OH_cols_test = pd.DataFrame(OH_encoder.transform(X_test[OH_cols]), columns = OH_cols_name)

OH_cols_train.index = X_train.index
# OH_cols_test.index = X_test.index

obj_cols = [col for col in X_train.columns if X_train[col].dtype == "object"]

# take only numerical values to concatenate 
num_X_train = X_train.drop(obj_cols, axis=1)
# num_X_test = X_test.drop(obj_cols, axis=1)

X_train = pd.concat([num_X_train, OH_cols_train], axis=1)
# OH_X_test = pd.concat([num_X_test, OH_cols_test], axis=1)

#%%
# pour les valeurs nulles restantes remplace par la plus frequente 
imp_cat = SimpleImputer(strategy='most_frequent')
imp_cat.fit_transform(X_train)
# imp_cat.transform(X_test)

scaler = StandardScaler()
# standardize the data 
X_train = pd.DataFrame(scaler.fit_transform(X_train), columns = X_train.columns) 
# X_test = pd.DataFrame(scaler.transform(X_test), columns = X_test.columns) 

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
X_train.drop(['CRSElapsedTime', 'CRSDepTime', 'CRSArrTime'] , axis=1, inplace=True)
X_test.drop(['CRSElapsedTime', 'CRSDepTime', 'CRSArrTime'] , axis=1, inplace=True)

#%% 
from sklearn.feature_selection import SelectKBest, chi2

# pb de valeur nulle ? a regler 
X_train.drop(['Distance', 'TaxiOut'] , axis=1, inplace=True)
X_test.drop(['Distance', 'TaxiOut'] , axis=1, inplace=True)
#%%

fs_k_best_chi2 = SelectKBest(k=4)
fs_k_best_chi2.fit(X_train, y_train)
col_filter = fs_k_best_chi2.get_support()
df_k_best_chi2 = X_train.iloc[:, col_filter]

print(df_k_best_chi2)

 ### creer un model de random forest + chercher l'accurary 
 ### creer un model de k nearest neighbor + idem 
 ### creer un model de gradient boosting + idem 
 ## en python en geenral ca se code :
# from sklearn.ensemble import GradientBoostingRegressor
# gbc = GradientBoostingRegressor()
# gbc.set_params(n_estimators=n)
# gbc.fit(X_train_mod, y_train_f)
# gbc.score(X_valid, y_valid)
# gbc.score(X_test, y_test)
 
 ## comparer les accuracy (peut tester avec différents paramètres 
 # les modeles si besoin d'augmenter les accuracy)











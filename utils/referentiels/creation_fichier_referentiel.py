#!/usr/bin/env python
# coding: utf-8

# In[14]:


import pandas as pd


# On procède en deux étapes :
# - on part du fichier des valeurs autorisées Koha
# - on le fusionne avec un fichier de base (codes label notamment)

# In[15]:


referentiels = pd.read_csv("referentiels_base.csv")


# In[16]:


koha_av = pd.read_csv("koha_av.csv")
koha_av = koha_av[['category', 'authorised_value', 'lib']]
koha_av['category'] = koha_av['category'].str.lower()
koha_av['category'] = "koha_av_" + koha_av['category']
koha_av.columns = ['referentiel', 'cle', 'valeur']


# In[17]:


referentiels = pd.concat([referentiels, koha_av])


# In[18]:


referentiels.to_csv("referentiels.csv", index=False)


# In[19]:


referentiels['referentiel'].value_counts()


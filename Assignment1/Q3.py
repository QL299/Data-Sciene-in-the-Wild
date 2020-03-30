#!/usr/bin/env python
# coding: utf-8

# In[77]:


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib


# In[45]:


weather_1 = pd.read_csv('weather.csv')


# In[13]:


#print(weather)


# In[47]:


print(weather.head())


# In[81]:


#figure
fig, ax1 = plt.subplots()
fig.set_size_inches(13, 10)

#labels
ax1.set_xlabel('Snowfall(inch)')
ax1.set_ylabel('Snow depth(inch)')
ax1.set_title('Relationship Between Snowfall and Snow depth')

#c sequence
c = weather['SNOW']

#plot
plt.scatter( weather['SNOW'], weather['SNWD'],c=c, cmap = 'plasma',alpha =0.3)
cbar = plt.colorbar()
cbar.set_label('Snowfall(inch)')
#plt.savefig('SNOW vs. SNWD.png')


# In[60]:


weather_2 = pd.read_csv('weather_2.csv')


# In[49]:


print(weather_2.head())


# In[87]:


data = weather.dropna(subset = ['SNOW'])
data_2 = data.dropna(subset = ['TMAX'])


# In[88]:


print(data.head())


# In[91]:


#figure
fig, ax1 = plt.subplots()
fig.set_size_inches(13, 10)

#labels
ax1.set_xlabel('Snowfall(inch)')
ax1.set_ylabel('Maximum temperature(F)')
ax1.set_title('Relationship Between Maximum temperature and Snowfall')

#c sequence
c = data_2['SNOW']

#plot
plt.scatter(data_2['TMAX'], data_2['SNOW'],c=c, cmap = 'viridis', alpha =0.3)
cbar = plt.colorbar()
cbar.set_label('Maximum temperature(F)')
plt.savefig('TMAX vs. SNOW.png')


# In[111]:


p3 = sns.relplot(y="WSF5", x="AWND", color = 'skyblue',facet_kws=dict(sharex=False),kind = "line", legend = "full", data = weather, height = 8, aspect = 1.3)
p3.set(xlabel='Average daily wind speed(mile/hour)', ylabel='Fastest 5-second wind speed(mile/hour)')
p3.set(title = "Relationship between Average daily wind speed and Fastest 5-second wind speed")
p3.savefig('AWND vs. WSF5.png')


# In[ ]:





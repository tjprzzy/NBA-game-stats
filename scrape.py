import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import unicodedata
import numpy
from tqdm import tqdm
from time import sleep

#Select the year you want to pull data from - only limited to single years for the time being.
year = 2021

#Comment out any months you DO NOT want game data pulled from:
months = [
'october', 
'november',
'december',
'january',
#'february',
#'march',
#'april',
#'may',
#'june',
#'july',
#'august',
#'september'
]

links = []
links_tmp = []

with tqdm(total=len(months),desc='Link Creation Progress') as pbar:
    for month in range(len(months)):
        url = 'https://www.basketball-reference.com/leagues/NBA_'+str(year)+'_games-'+months[month]+'.html'
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')

        links_tmp = [game['href'] for game in soup.find_all('a', text='Box Score')]
        base_url = "https://www.basketball-reference.com"
        links_tmp = [base_url + link for link in links_tmp]
        for link in links_tmp:
            links.append(link)
        pbar.update(1)

print('Links successfully created for '+ ', '.join(months)+' of '+ str(year) + '.')

stats = []
with tqdm(total=len(links),desc='Scraping Progress') as pbar:
    for link in range(len(links)):
    #for link in range(150):
        tmp = links[link]
        page = requests.get(tmp)
        soup = BeautifulSoup(page.text, 'html.parser')

        teams = [game['href'] for game in soup.find_all('a', {'itemprop': 'name'})]
        teams = [teams[0][7:10], teams[1][7:10]]

        stats_tmp = []
        for team in range(len(teams)):
            basic_stats = soup.find('table',{'id': 'box-'+teams[team]+'-game-basic'})
            adv_stats = soup.find('table',{'id': 'box-'+teams[team]+'-game-advanced'})

            #Player Names
            player_names = [[th['data-append-csv'] for th in basic_stats.findAll('tr')[1:][i].findAll('th')] for i in range(len(basic_stats.findAll('tr')[1:]))]
            player_names = player_names[1:6] + player_names[7:-1]
            df_player_names = pd.DataFrame(player_names)

            #Starters
            starters = ['Y','Y','Y','Y','Y'] + ['N']*(len(player_names)-5)
            df_starters = pd.DataFrame(starters)
            
            #Player Basic Stats
            team_stats_basic = [[td.getText() for td in basic_stats.findAll('tr')[1:][i].findAll('td')]
                    for i in range(len(basic_stats.findAll('tr')[1:]))]
            team_stats_basic = team_stats_basic[1:6] + team_stats_basic[7:-1]
            df_team_stats_basic = pd.DataFrame(team_stats_basic)

            #Player Advanced Stats
            team_stats_adv = [[td.getText() for td in adv_stats.findAll('tr')[1:][i].findAll('td')]
                    for i in range(len(adv_stats.findAll('tr')[1:]))]
            team_stats_adv = team_stats_adv[1:6] + team_stats_adv[7:-1]
            df_team_stats_adv = pd.DataFrame(team_stats_adv)

            #Game ID
            game_id = links[link][47:59]
            game_id = [game_id for i in range(len(player_names))]
            df_game_id = pd.DataFrame(game_id)

            #Team ID
            team_name = [teams[team] for i in range(len(player_names))]
            df_team_name = pd.DataFrame(team_name)

            #Consolidating the Data:
            stats_tmp = pd.concat([df_game_id,df_team_name,df_player_names,df_starters,df_team_stats_basic,df_team_stats_adv],axis=1)
            stats.append(stats_tmp)
        pbar.update(1)
    
    output_raw = pd.concat(stats)

output = output_raw
#Add a header to the Data
header = ['Game_ID','Team','Player','Starter','Min','FG','FGA','FG%','3P','3PA','3P%','FT','FTA','FT%','ORB','DRB','TRB','AST','STL','BLK','TOV','PF','PTS','P/M',
          'del','TS%','eFG%','3PAr', 'FTr','ORB%','DRB%','TRB%','AST%','STL%','BLK%','TOV%','USG%','ORtg','DRtg','BPM']
output.columns = header

#Index Column - Reset IDs
output = output.reset_index(drop=True)

#Player Column - Replace any special characters with the 26 basic alphabetic characters.
output['Player'] = output['Player'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

#MP - Convert to decimal
for row in range(len(output['Min'])):
    if ":" in output['Min'][row]:
        output['Min'][row] = round(float(output['Min'][row].split(':')[0]+str(int(output['Min'][row].split(':')[1])/60)[1:]),2)
    else:
        output['Min'][row] = 0

#Delete redundant Minutes Played (del) column:
output = output.drop(['del'],axis=1)

#Convert statistical columns to float type in order to calculate fantasy points
output['PTS'] = pd.to_numeric(output['PTS'],downcast='float')
output['3P'] = pd.to_numeric(output['3P'],downcast='float')
output['TRB'] = pd.to_numeric(output['TRB'],downcast='float')
output['AST'] = pd.to_numeric(output['AST'],downcast='float')
output['STL'] = pd.to_numeric(output['STL'],downcast='float')
output['BLK'] = pd.to_numeric(output['BLK'],downcast='float')
output['TOV'] = pd.to_numeric(output['TOV'],downcast='float')

#Dealing with Double Doubles/Triple Doubles 
isTen = lambda x:int(x>=10)
countTens = lambda row: numpy.clip(isTen(row['PTS']) + isTen(row['TRB']) + isTen(row['AST']) + isTen(row['STL']) + isTen(row['BLK']),1,3)-1
output['DDBonus'] = output.apply(countTens,axis=1)

#Calculating Fantasy Points
output['Fp'] = 1*output['PTS'] + 0.5*output['3P'] + 1.25*output['TRB'] + 1.5*output['AST'] + 2*output['STL'] + 2*output['BLK'] - 0.5*output['TOV'] + 1.5*output['DDBonus']

#Calculating Fantasy Points per Minute
FpMin = []
for row in range(len(output['Min'])):
    if output['Min'][row] == 0:
        FpMin.append(0)
    else:
        FpMin.append(round(output['Fp'][row] / output['Min'][row],3))
output['Fp/Min'] = FpMin
        
#Replace any blank cells with 0
output = output.fillna(0)
output = output.replace('',0)

output.to_csv('2020stats.csv')

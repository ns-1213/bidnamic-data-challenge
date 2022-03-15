import pandas as pd
import sqlalchemy

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:nick@localhost:5432/postgres')


ad_groups_raw = pd.DataFrame(pd.read_csv('adgroups.csv'))
search_terms = pd.DataFrame(pd.read_csv('search_terms.csv'))
campaigns = pd.DataFrame(pd.read_csv('campaigns.csv'))

ad_groups_raw['alias'] = ad_groups_raw['alias'].apply(lambda x:x.split(' - '))
alias_df = pd.DataFrame(ad_groups_raw['alias'].to_list(),columns = ['shift','shopping','country','campaign structure value','priority','random string','hash'])
ad_groups = pd.concat([ad_groups_raw,alias_df], axis = 1)
ad_groups.drop('alias',axis = 1,inplace=True)
search_terms['roas'] = search_terms['conversion_value']/search_terms['cost']



ad_groups.to_sql('ad_groups',engine,if_exists='replace',index=False)
search_terms.to_sql('search_terms',engine,if_exists='replace',index=False)
campaigns.to_sql('campaigns',engine,if_exists='replace',index=False)

"""
this data can then be queired using queries such as 

select "campaign structure value",sum(s.roas)
from search_terms s
left join ad_groups a on a.ad_group_id = s.ad_group_id
group by "campaign structure value"
"""
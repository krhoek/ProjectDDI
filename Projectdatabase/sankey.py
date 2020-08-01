from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect
import seaborn as sns
import pandas as pd
import plotly
import plotly.graph_objects as go

DATABASE_CONNECTION = f'mssql+pyodbc://ddi.sa.shared:K&9*2hZqxE63=uU;@eiklsfzhfc.database.windows.net:1433/clientjourney?driver=ODBC Driver 17 for SQL Server'

engine = create_engine(DATABASE_CONNECTION,echo = True)
session = Session(engine)

# Select producten and touchpoints
data_producten = pd.read_sql_query("Select product_id, financiering from producten", session.bind)
data_touchpoints = pd.read_sql_query("Select client_id, target_id, touchpoint_tijd from  touchpoint Where touchpoint.type_touchpoint_id = 'tt_1'" , session.bind)

# Rename column product_id to target_id
data_producten.rename(columns={'product_id': 'target_id'}, inplace=True)

# Merge producten and touchpoints
data = pd.merge(data_producten,data_touchpoints, on ='target_id', how = 'inner')

# Drop column target_id
data.drop(columns='target_id', axis = 1, inplace = True)

# Rename column x to target_id
data.rename(columns={'financiering': 'target_id'}, inplace=True)

# Check if touchpoint_tijd are Pandas Datetime types:
data['touchpoint_tijd'] = pd.to_datetime(data['touchpoint_tijd'], unit='d')

# Based on the time of touchpoint we can compute the rank of each touchpoint
# a) Sort ascendingly per client_id and touchpoint_tijd
data.sort_values(['client_id', 'touchpoint_tijd'], ascending=[True,True], inplace=True)

# b) Group by client_id
grouped = data.groupby('client_id')

# c) Define a ranking function based on touchpoint_tijd, using the method = 'first' param to ensure no touchpoint have the same rank
def rank(x): return x['touchpoint_tijd'].rank(method='first').astype(int)

# d) Apply the ranking function to the data DF into a new "rank_touchpoint" column
data["rank_touchpoint"] = grouped.apply(rank).reset_index(0, drop=True)


# Add, each row, the information about the next_touchpoint
# a) Regroup by client_id
grouped = data.groupby('client_id')

# b) The shift function allows to access the next row's data. Here, we'll want the touchpoint name
def get_next_touchpoint(x): return x['target_id'].shift(-1)

# c) Apply the function into a new "next_touchpoint" column
data["next_touchpoint"] = grouped.apply(
    lambda x: get_next_touchpoint(x)).reset_index(0, drop=True)


# Likewise, we can compute time from each touchpoint to its next touchpoint:
# a) Regroup by client_id 
grouped = data.groupby('client_id')

# b) We make use one more time of the shift function:
def get_time_diff(
    x): return x['touchpoint_tijd'].shift(-1) - x['touchpoint_tijd']

# c) Apply the function to the data DF into a new "time_to_next" column
data["time_to_next"] = grouped.apply(
    lambda x: get_time_diff(x)).reset_index(0, drop=True)

# Here we'll plot the journey up to the 10th action. This can be achieved by filtering the dataframe based on the rank_touchpoint column that we computed:
data = data[data.rank_touchpoint < 10]



# Working on the nodes_dict

all_touchpoints = list(data.target_id.unique())

# Create a set of colors that you'd like to use in your plot.
palette = ['50BE97', 'E4655C', 'FCC865',
           'BFD6DE', '3E5066', '353A3E', 'E6E6E6']
#  Here, I passed the colors as HEX, but we need to pass it as RGB. This loop will convert from HEX to RGB:
for i, col in enumerate(palette):
    palette[i] = tuple(int(col[i:i+2], 16) for i in (0, 2, 4))

# Append a Seaborn complementary palette to your palette in case you did not provide enough colors to style every touchpoint
complementary_palette = sns.color_palette(
    "deep", len(all_touchpoints) - len(palette))
if len(complementary_palette) > 0:
    palette.extend(complementary_palette)

output = dict()
output.update({'nodes_dict': dict()})

i = 0
for rank_touchpoint in data.rank_touchpoint.unique(): # For each rank of touchpoint...
    # Create a new key equal to the rank...
    output['nodes_dict'].update(
        {rank_touchpoint: dict()}
    )
    
    # Look at all the touchpoints that were done at this step of the funnel...
    all_touchpoints_at_this_rank = data[data.rank_touchpoint ==
                                   rank_touchpoint].target_id.unique()
    
    # Read the colors for these touchpoints and store them in a list...
    rank_palette = []
    for touchpoint in all_touchpoints_at_this_rank:
        rank_palette.append(palette[list(all_touchpoints).index(touchpoint)])
    
    # Keep trace of the touchpoints' names, colors and indices.
    output['nodes_dict'][rank_touchpoint].update(
        {
            'sources': list(all_touchpoints_at_this_rank),
            'color': rank_palette,
            'sources_index': list(range(i, i+len(all_touchpoints_at_this_rank)))
        }
    )
    # Finally, increment by the length of this rank's available touchpoints to make sure next indices will not be chosen from existing ones
    i += len(output['nodes_dict'][rank_touchpoint]['sources_index'])


# Working on the links_dict

output.update({'links_dict': dict()})

# Group the DataFrame by client_id and rank_touchpoint
grouped = data.groupby(['client_id', 'rank_touchpoint'])

# Define a function to read the sources, targets, values and time from touchpoint to next_touchpoint:
def update_source_target(user):
    try:
        # user.name[0] is the user's user_id; user.name[1] is the rank of each action
        # 1st we retrieve the source and target's indices from nodes_dict
        source_index = output['nodes_dict'][user.name[1]]['sources_index'][output['nodes_dict']
                                                                           [user.name[1]]['sources'].index(user['target_id'].values[0])]
        target_index = output['nodes_dict'][user.name[1] + 1]['sources_index'][output['nodes_dict']
                                                                               [user.name[1] + 1]['sources'].index(user['next_touchpoint'].values[0])]


         # If this source is already in links_dict...
        if source_index in output['links_dict']:
            # ...and if this target is already associated to this source...
            if target_index in output['links_dict'][source_index]:
                # ...then we increment the count of users with this source/target pair by 1, and keep track of the time from source to target
                output['links_dict'][source_index][target_index]['unique_users'] += 1
                output['links_dict'][source_index][target_index]['avg_time_to_next'] += user['time_to_next'].values[0]
            # ...but if the target is not already associated to this source...
            else:
                # ...we create a new key for this target, for this source, and initiate it with 1 user and the time from source to target
                output['links_dict'][source_index].update({target_index:
                                                           dict(
                                                               {'unique_users': 1,
                                                                'avg_time_to_next': user['time_to_next'].values[0]}
                                                           )
                                                           })
        # ...but if this source isn't already available in the links_dict, we create its key and the key of this source's target, and we initiate it with 1 user and the time from source to target
        else:
            output['links_dict'].update({source_index: dict({target_index: dict(
                {'unique_users': 1, 'avg_time_to_next': user['time_to_next'].values[0]})})})
    except Exception as e:
        pass

# Apply the function to your grouped Pandas object:
grouped.apply(lambda user: update_source_target(user)) 

targets = []
sources = []
values = []
time_to_next = []

for source_key, source_value in output['links_dict'].items():
    for target_key, target_value in output['links_dict'][source_key].items():
        sources.append(source_key)
        targets.append(target_key)
        values.append(target_value['unique_users'])
        time_to_next.append(str(pd.to_timedelta(
            target_value['avg_time_to_next'] / target_value['unique_users'])).split(':')[0]) # Split to remove the milliseconds information

labels = []
colors = []
for key, value in output['nodes_dict'].items():
    labels = labels + list(output['nodes_dict'][key]['sources'])
    colors = colors + list(output['nodes_dict'][key]['color'])

for idx, color in enumerate(colors):
    colors[idx] = "rgb" + str(color) + ""

fig = go.Figure(data=[go.Sankey(
    node=dict(
        thickness=10,  # default is 20
        line=dict(color="black", width=0.5),
        label=labels,
        color=colors
    ),
    link=dict(
        source=sources,
        target=targets,
        value=values,
        label=time_to_next,
        hovertemplate='%{value} unique users went from %{source.label} to %{target.label}.<br />' +
        '<br />It took them %{label} in average.<extra></extra>',
    ))])

fig.update_layout(autosize=True, title_text="Client journey", font=dict(size=10), plot_bgcolor='white')

fig.show()
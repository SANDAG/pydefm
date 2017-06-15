import matplotlib.pyplot as plt
from pysandag import database
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import numpy as np
from bokeh.io import curdoc
from bokeh.layouts import row, widgetbox
from bokeh.models import ColumnDataSource, LabelSet
from bokeh.models.widgets import Slider, TextInput
from bokeh.plotting import figure, output_file, show


defm_engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

db_connection_string = database.get_connection_string('model_config.yml', 'in_db')
sql_in_engine = create_engine(db_connection_string)

results_sql = '''SELECT "Population" as pop_py
                        ,"Run_id"
                        ,"Year"
                        ,deaths as deaths_py
                        ,mig_in - mig_out as net_mig_py
                        ,new_born as births_py
                FROM defm.population_summary
                WHERE "Run_id" = 3;'''

results_df = pd.read_sql(results_sql, defm_engine, index_col='Year')


dof_sql = '''
            SELECT TOP 1000 [county_name]
                  ,[calendar_yr] as Year
                  ,[end_population] as pop_dof
                  ,[births] as births_dof
                  ,[deaths] as deaths_dof
                  ,[net_migration] as net_mig_dof
              FROM [socioec_data].[ca_dof].[coc_calendar_projections_2017]
              WHERE county_name = 'San Diego'
            '''

dof_df = pd.read_sql(dof_sql, sql_in_engine, index_col='Year')


sas_sql = '''SELECT [yr] as Year
            ,[p] as pop_sas_1005
            ,d as deaths_sas_1005
            ,b_hp as births_sas_1005
            ,mig_net as net_mig_sas_1005
            FROM [isam].[demographic_output].[summary]
            WHERE sim_id = 1005
            '''

sas_df = pd.read_sql(sas_sql, sql_in_engine, index_col='Year')


# join the 3 data frames
df = dof_df.join(results_df)
df = df.join(sas_df)

'''
# Population graphs
a = df[['pop_py', 'pop_dof', 'pop_sas_1005']].plot()
plt.title('Main difference: applying survival rates to new born ')
plt.savefig('temp/pop.png')
plt.close()


# Deaths summary graphs
b = df[['deaths_py', 'deaths_dof', 'deaths_sas_1005']].plot()
plt.title('Main difference: not allowing GQ to die')
plt.savefig('temp/deaths.png')
plt.close()


# Births summary graphs
c = df[['births_py', 'births_dof', 'births_sas_1005']].plot()
plt.title('Main difference: not allowing mil_dep to have new borns')
plt.savefig('temp/births.png')
plt.close()


# Net migration summary graphs
d = df[['net_mig_py', 'net_mig_dof', 'net_mig_sas_1005']].plot()
plt.title('Blue amd red lines are ~ the same')
plt.savefig('temp/net_mig.png')
plt.close()
'''

plot2 = figure(plot_height=800, plot_width=800, title="Main difference: applying survival rates to new born",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label = "Population",
                 x_axis_label = "Year")

plot2.line(df.index.tolist(), df['pop_py'], line_width=2, legend="Population Python")
plot2.line(df.index.tolist(), df['pop_sas_1005'], line_width=2, legend="Population SAS (1005)", line_color="orange", line_dash=[4, 4])
plot2.line(df.index.tolist(), df['pop_dof'], line_width=2, legend="Population DOF", line_color="green", line_dash=[4, 4])


# DEATHS
plot3 = figure(plot_height=800, plot_width=800, title="Main difference: not allowing GQ to die",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Deaths",
               x_axis_label="Year")

plot3.line(df.index.tolist(), df['deaths_py'], line_width=2, legend="Deaths Python")
plot3.line(df.index.tolist(), df['deaths_sas_1005'], line_width=2, legend="Deaths SAS (1005)", line_color="orange", line_dash=[4, 4])
plot3.line(df.index.tolist(), df['deaths_dof'], line_width=2, legend="Deaths DOF", line_color="green", line_dash=[4, 4])


# Births
plot4 = figure(plot_height=800, plot_width=800, title="Main difference: not allowing GQ to have new borns",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Births",
                 x_axis_label="Year")

plot4.line(df.index.tolist(), df['births_py'], line_width=2, legend="Births Python")
plot4.line(df.index.tolist(), df['births_sas_1005'], line_width=2, legend="Births SAS (1005)", line_color="orange", line_dash=[4, 4])
plot4.line(df.index.tolist(), df['births_dof'], line_width=2, legend="Births DOF", line_color="green", line_dash=[4, 4])


# Net Migration
plot5 = figure(plot_height=800, plot_width=800, title="~ Equal",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Net Migration",
                 x_axis_label="Year")

plot5.line(df.index.tolist(), df['net_mig_py'], line_width=2, legend="Births Python")
plot5.line(df.index.tolist(), df['net_mig_sas_1005'], line_width=2, legend="Births SAS (1005)", line_color="orange", line_dash=[4, 4])
plot5.line(df.index.tolist(), df['net_mig_dof'], line_width=2, legend="Births DOF", line_color="green", line_dash=[4, 4])


# Bokeh time series graphs
x = df['deaths_py']
y = df['births_py']
source = ColumnDataSource(data=df[['deaths_py', 'births_py']])


# Set up plot
plot = figure(plot_height=800, plot_width=800, title="Population",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label = "Number",
                 x_axis_label = "Deaths vs Births")

print source.data['deaths_py'].loc[2011]

plot.vbar(x=[1, 2], width=0.5, bottom=0, top=[source.data['deaths_py'].loc[2011], source.data['births_py'].loc[2011]], source=source)

# show(plot)
# Set up widgets
text = TextInput(title="Graph", value='Population over time')
Year = Slider(title="Year", value=2011, start=2011, end=2050, step=1)


# Set up callbacks
def update_title(attrname, old, new):
    plot.title.text = text.value

text.on_change('value', update_title)


def update_plot(attrname, old, new):

    # Get the current slider values
    yr = Year.value

    source = ColumnDataSource(data=df[['deaths_py', 'births_py']])
    plot.vbar(x=[1, 2], width=0.5, bottom=0, top=[source.data['deaths_py'].loc[yr], source.data['births_py'].loc[yr]],
              source=source)

for w in [Year]:
    w.on_change('value', update_plot)


# Set up layouts and add to document
inputs = widgetbox(text, Year)

curdoc().add_root(row(inputs, plot, width=800))
curdoc().add_root(row(plot2, width=800))
curdoc().add_root(row(plot3, width=800))
curdoc().add_root(row(plot4, width=800))
curdoc().add_root(row(plot5, width=800))

curdoc().title = "Sliders"
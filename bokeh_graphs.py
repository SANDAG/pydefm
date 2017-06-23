import matplotlib.pyplot as plt
from pysandag import database
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import numpy as np
from bokeh.io import curdoc, gridplot
from bokeh.layouts import row, widgetbox, column
from bokeh.models import ColumnDataSource, LabelSet, Plot, DataRange1d, LinearAxis, Grid, LassoSelectTool, WheelZoomTool, SaveTool, ResetTool
from bokeh.models.widgets import Slider, TextInput
from bokeh.plotting import figure, output_file, show
from bokeh.charts import Bar, output_file, show
from bokeh.models.glyphs import HBar
from bokeh.models import (
    ColumnDataSource, HoverTool, SingleIntervalTicker, Slider, Button, Label,
    CategoricalColorMapper, ranges
)
from bokeh.layouts import layout


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
                WHERE "Run_id" = 55;'''

results_df = pd.read_sql(results_sql, defm_engine, index_col='Year')

results_inc_sql = '''SELECT  yr as "Year",
                            "Interest" as interest_py,
                             "Other" as other_py,
                             "Public_Assistance" as public_assistance_py,
                             "Retirement" as retirement_py,
                             "Supplemental_Social_Security" as supplemental_social_security_py,
                             "Social_Security" as social_security_py
                         FROM defm.non_wage_income
                         WHERE run_id = 3;'''

results_inc_df = pd.read_sql(results_inc_sql, defm_engine, index_col='Year')

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
            ,d_hp as deaths_sas_1005
            ,b_nonmil as births_sas_1005
            ,mig_net as net_mig_sas_1005
            FROM [isam].[demographic_output].[summary]
            WHERE sim_id = 1005
            '''

sas_df = pd.read_sql(sas_sql, sql_in_engine, index_col='Year')

sas_inc_sql = '''
    SELECT
      yr as Year,
      Interest as Interest_sas,
      Other as Other_sas,
      Public_Assistance as Public_Assistance_sas,
      Retirement as Retirement_sas,
      Supplemental_Social_Security as Supplemental_Social_Security_sas,
      Social_Security as Social_Security_sas
    FROM [isam].[economic_output].[unearned_income]
    '''

sas_inc_df = pd.read_sql(sas_inc_sql, sql_in_engine, index_col='Year')


# Age distribution

pop_sql = '''SELECT age, race_ethn, sex, type, mildep, persons, households, yr
                FROM defm.population
                WHERE run_id = 55 and age < 102
                '''
pop_df = pd.read_sql(pop_sql, defm_engine, index_col=None)

min_year = pop_df['yr'].min()

# sum by age
pop_sum_df = pd.DataFrame(pop_df['persons'].groupby([pop_df['yr'], pop_df['age'],  pop_df['sex']]).sum())
pop_sum_df.rename(columns={'persons': 'persons_by_age'}, inplace=True)

yr_sum_df = pd.DataFrame(pop_df['persons'].groupby([pop_df['yr']]).sum())
pop_sum_df = pop_sum_df.reset_index(drop=False)
yr_sum_df = yr_sum_df.reset_index(drop=False)
pop_sum_df = pop_sum_df.set_index(['yr'])
yr_sum_df = yr_sum_df.set_index(['yr'])

pop_sum_df = pop_sum_df.join(yr_sum_df)

pop_sum_df['ratio'] = pop_sum_df['persons_by_age'] / pop_sum_df['persons']


pop_sum_df_m = pop_sum_df.loc[pop_sum_df['sex'] == 'M']
pop_sum_df_f = pop_sum_df.loc[pop_sum_df['sex'] == 'F']

# sum by race

pop_sum_race_df = pd.DataFrame(pop_df['persons'].groupby([pop_df['yr'], pop_df['race_ethn']]).sum())
pop_sum_race_df.rename(columns={'persons': 'persons_by_race'}, inplace=True)
pop_sum_race_df = pop_sum_race_df.reset_index(drop=False)
pop_sum_race_df = pop_sum_race_df.set_index(['yr'])

pop_sum_race_df = pop_sum_race_df.join(yr_sum_df)

pop_sum_race_df['ratio'] = pop_sum_race_df['persons_by_race'] / pop_sum_race_df['persons']


# pop_sum_df_m['age'] = ((pop_sum_df_m['age'] * 2 + 1)/2)
# pop_sum_df_f['age'] = (pop_sum_df_f['age'] * 2)

# join the 3 data frames
df = dof_df.join(results_df)
df = df.join(sas_df)

df2 = sas_inc_df.join(results_inc_df)

plot2 = figure(plot_height=800, plot_width=1400, title="Main difference: applying survival rates to new born",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label = "Population",
                 x_axis_label = "Year")

plot2.line(df.index.tolist(), df['pop_py'], line_width=2, legend="Population Python")
plot2.line(df.index.tolist(), df['pop_sas_1005'], line_width=2, legend="Population SAS (1005)", line_color="orange", line_dash=[4, 4])
plot2.line(df.index.tolist(), df['pop_dof'], line_width=2, legend="Population DOF", line_color="green", line_dash=[4, 4])


# DEATHS
plot3 = figure(plot_height=800, plot_width=1400, title="Main difference: not allowing GQ to die",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Deaths",
               x_axis_label="Year")

plot3.line(df.index.tolist(), df['deaths_py'], line_width=2, legend="Deaths Python")
plot3.line(df.index.tolist(), df['deaths_sas_1005'], line_width=2, legend="Deaths SAS (1005)", line_color="orange", line_dash=[4, 4])
plot3.line(df.index.tolist(), df['deaths_dof'], line_width=2, legend="Deaths DOF", line_color="green", line_dash=[4, 4])


# Births
plot4 = figure(plot_height=800, plot_width=1400, title="Main difference: not allowing GQ to have new borns",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Births",
                 x_axis_label="Year")

plot4.line(df.index.tolist(), df['births_py'], line_width=2, legend="Births Python")
plot4.line(df.index.tolist(), df['births_sas_1005'], line_width=2, legend="Births SAS (1005)", line_color="orange", line_dash=[4, 4])
plot4.line(df.index.tolist(), df['births_dof'], line_width=2, legend="Births DOF", line_color="green", line_dash=[4, 4])


# Net Migration

plot5 = figure(plot_height=800, plot_width=1400, title="~ Equal",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Net Migration",
                 x_axis_label="Year")

plot5.line(df.index.tolist(), df['net_mig_py'], line_width=2, legend="Net Migration Python")
plot5.line(df.index.tolist(), df['net_mig_sas_1005'], line_width=2, legend="Net Migration SAS (1005)", line_color="orange", line_dash=[4, 4])
plot5.line(df.index.tolist(), df['net_mig_dof'], line_width=2, legend="Net Migration DOF", line_color="green", line_dash=[4, 4])


# Income results

plot6 = figure(plot_height=800, plot_width=1400, title="Income Model",
              tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label="Dollars ($)",
                 x_axis_label="Year")

plot6.line(df2.index.tolist(), df2['interest_py'], line_width=2, legend="Interest PY")
plot6.line(df2.index.tolist(), df2['Interest_sas'], line_width=2, legend="Interest SAS", line_color="orange", line_dash=[4, 4])

plot6.line(df2.index.tolist(), df2['other_py'], line_width=2, legend="Other PY", line_color="red")
plot6.line(df2.index.tolist(), df2['Other_sas'], line_width=2, legend="Other SAS", line_color="orange", line_dash=[4, 4])

plot6.line(df2.index.tolist(), df2['public_assistance_py'], line_width=2, legend="Public Assistance PY", line_color="green")
plot6.line(df2.index.tolist(), df2['Public_Assistance_sas'], line_width=2, legend="Public Assistance SAS", line_color="orange", line_dash=[4, 4])

plot6.line(df2.index.tolist(), df2['retirement_py'], line_width=2, legend="Retirement PY", line_color="black")
plot6.line(df2.index.tolist(), df2['Retirement_sas'], line_width=2, legend="Retirement SAS", line_color="orange", line_dash=[4, 4])

plot6.line(df2.index.tolist(), df2['supplemental_social_security_py'], line_width=2, legend="Supplemental Social Security PY", line_color="purple")
plot6.line(df2.index.tolist(), df2['Supplemental_Social_Security_sas'], line_width=2, legend="Supplemental Social Security SAS", line_color="orange", line_dash=[4, 4])

plot6.line(df2.index.tolist(), df2['social_security_py'], line_width=2, legend="Social Security PY", line_color="blue")
plot6.line(df2.index.tolist(), df2['Social_Security_sas'], line_width=2, legend="Social Security SAS", line_color="orange", line_dash=[4, 4])

plot6.legend.location = "top_left"
plot6.legend.background_fill_alpha = 0.5

pop_sum_df_m['ratio'] = (pop_sum_df_m['ratio'] * -1)

# Bokeh time series graphs
y_m = pop_sum_df_m['age'].loc[min_year].tolist()
right_m = pop_sum_df_m['ratio'].loc[min_year].tolist()

y_f = pop_sum_df_f['age'].loc[min_year].tolist()
right_f = pop_sum_df_f['ratio'].loc[min_year].tolist()

pop_sum_race_df = pop_sum_race_df.round(2)
x = pop_sum_race_df['race_ethn'].loc[min_year].tolist()
y = pop_sum_race_df['ratio'].loc[min_year].tolist()

source = ColumnDataSource(data=dict(y_m=y_m, right_m=right_m, y_f=y_f, right_f=right_f))

source2 = ColumnDataSource(data=dict(x=x, y=y))


# Set up plot
plot = figure(plot_height=400, plot_width=800, title="Population",
                tools="crosshair,pan,reset,save,wheel_zoom", y_axis_label = "Age",
                     x_axis_label = "Percentage of the total population", x_range=ranges.Range1d(start=-.01, end=.01),
              y_range=ranges.Range1d(start=-1, end=102))

glyph1 = HBar(y="y_m", right="right_m", left=0, height=0.5, fill_color="blue", name="Male")
plot.add_glyph(source, glyph1)

glyph2 = HBar(y="y_f", right="right_f", left=0, height=0.5, fill_color="orange", name="Female")
plot.add_glyph(source, glyph2)

plot.xaxis.bounds = (-.01, .01)

plot7 = figure(plot_height=400, plot_width=800, title="Population distribution by race",
                tools="crosshair,pan,reset,save,wheel_zoom",
        x_axis_label = "race",
        y_axis_label = "percentage",
        x_minor_ticks=2,
        x_range = source2.data["x"],
        y_range= ranges.Range1d(start=0, end=.6))


labels = LabelSet(x='x', y='y', text='y', level='glyph',
        x_offset=-13.5, y_offset=0, source=source2, render_mode='canvas')

plot7.vbar(source=source2, x='x', top='y', bottom=0, width=0.3, color="blue")
plot7.add_layout(labels)
show(plot)
# Set up widgets


def animate_update():
    year = Year.value + 1
    if year > 2049:
        year = min_year
    Year.value = year


def update_plot(attrname, old, new):

    # Get the current slider values
    yr = Year.value

    y_m = pop_sum_df_m['age'].loc[yr].tolist()
    right_m = pop_sum_df_m['ratio'].loc[yr].tolist()

    y_f = pop_sum_df_f['age'].loc[yr].tolist()
    right_f = pop_sum_df_f['ratio'].loc[yr].tolist()

    x = pop_sum_race_df['race_ethn'].loc[yr].tolist()
    y = pop_sum_race_df['ratio'].loc[yr].tolist()

    source.data = dict(y_m=y_m, right_m=right_m, y_f=y_f, right_f=right_f)
    source2.data = dict(x=x, y=y)

Year = Slider(title="Year", value=min_year, start=min_year, end=2050, step=1)

Year.on_change('value', update_plot)


def animate():
    if button.label == '► Play':
        button.label = '❚❚ Pause'
        curdoc().add_periodic_callback(animate_update, 300)
    else:
        button.label = '► Play'
        curdoc().remove_periodic_callback(animate_update)

button = Button(label='► Play', width=60)
button.on_click(animate)

layout = layout([
    [plot],
    [Year, button],
    [plot7],
    [plot2],
    [plot3],
    [plot4],
    [plot5],
    [plot6],
], sizing_mode='scale_width')

curdoc().add_root(layout)
'''
for w in [Year]:
    w.on_change('value', update_plot)


# Set up layouts and add to document
inputs = widgetbox(Year)

curdoc().add_root(column(plot, inputs, plot7, width=4000))


curdoc().add_root(row(plot2, width=800))
curdoc().add_root(row(plot3, width=800))
curdoc().add_root(row(plot4, width=800))
curdoc().add_root(row(plot5, width=800))
curdoc().add_root(row(plot6, width=800))
'''
curdoc().title = "Sliders"


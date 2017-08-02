from flask import Flask
from flask import request, redirect
from flask import render_template
import os
import luigi
import shutil
import defm_luigi as defm
import inc_luigi as inc
import emp_luigi as emp
from db import extract
import shutil
import pandas
import luigi.contrib.hadoop
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
from bokeh.models import (
    ColumnDataSource, HoverTool, BoxZoomTool, WheelZoomTool, PanTool, SaveTool, NumeralTickFormatter
)
from bokeh.charts import TimeSeries
from bokeh.embed import components
from db import sql
from bokeh.resources import INLINE
from bokeh.plotting import figure
from pysandag import database
from bokeh.palettes import Spectral4
from bokeh.layouts import row
import warnings
warnings.filterwarnings('ignore', category=pandas.io.pytables.PerformanceWarning)
defm_engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

db_connection_string = database.get_connection_string('model_config.yml', 'in_db')
sql_in_engine = create_engine(db_connection_string)

in_query = getattr(sql, 'max_run_id')
db_run_id = pd.read_sql(in_query, defm_engine, index_col=None)

run_id = db_run_id['id'].iloc[0]


class CombinedSimulation(luigi.Task):
    start = luigi.Parameter()
    end = luigi.Parameter()
    dem = luigi.Parameter()
    econ = luigi.Parameter()

    def requires(self):
        return {'def': defm.Iter(start=self.start, end=self.end, dem=self.dem, econ=self.econ),
                'inc': inc.IncomeByType(econ=self.econ, dem=self.dem),
                'emp': emp.PersonalIncome(econ=self.econ, dem=self.dem)}

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'Completed combined simulation'


app = Flask(__name__)


def create_figure(df, current_feature_name):
    p = figure(width=600, height=400, tools=[BoxZoomTool(), WheelZoomTool(), PanTool(), SaveTool()],
               title=current_feature_name, y_axis_label=current_feature_name,
                 x_axis_label="Year")
    p.line(df.index.tolist(), df[current_feature_name], line_width=2, legend=False, line_color="red")

    source = ColumnDataSource(
        {'x': df.index.values.tolist(), 'y': df[current_feature_name].values, 'y2': df[current_feature_name].map('{:,.0f}'.format).tolist()})

    p.scatter('x', 'y', source=source, fill_alpha=0, line_alpha=.95, line_color="black", fill_color="blue", name='scat')
    hover = HoverTool(names=["scat"],
                      tooltips=[("Year ", "@x"), (current_feature_name, "@y2")]
                      )
    p.add_tools(hover)
    p.yaxis[0].formatter = NumeralTickFormatter(format="0,0.0")

    return p


@app.route('/')
def my_form():

    econ_sim_ids = extract.create_df('econ_sim_ids', 'econ_sim_ids_table', rate_id=0, index=None)
    dem_sim_ids = extract.create_df('dem_sim_ids', 'dem_sim_ids_table', rate_id=0, index=None)
    dems = dem_sim_ids['demographic_simulation_id'].tolist()
    econs = econ_sim_ids['economic_simulation_id'].tolist()
    startyear = range(2011, 2050)
    endyear = range(2012, 2051)

    return render_template("my-form.html", result1=dems, result2=econs, startyear=startyear, endyear=endyear)


def create_figure_rate(df, rate_id, race, column_name):

    df = df.loc[(df.rate_id == rate_id) & (df.race == race)]

    p = figure(width=600, height=400, tools=[BoxZoomTool(), WheelZoomTool(), PanTool(), SaveTool()],
               title=column_name, y_axis_label=column_name, x_axis_label="Year")

    p.line(df.Year.tolist(), df[column_name], line_width=2, legend=False, line_color="red")

    source = ColumnDataSource(
        {'x': df.Year.tolist(), 'y': df[column_name].values, 'y2': df[column_name].map('{:,.5}'.format).tolist()})

    p.scatter('x', 'y', source=source, fill_alpha=0, line_alpha=.95, line_color="black", fill_color="blue", name='scat')
    hover = HoverTool(names=["scat"],
                      tooltips=[("Year ", "@x"), (column_name, "@y2")]
                      )
    p.add_tools(hover)
    p.yaxis[0].formatter = NumeralTickFormatter(format="0.00")
    return p


@app.route('/', methods=['POST'])
def my_form_post():
        dem = request.form['dem']
        econ = request.form['econ']
        start_year = request.form['starty']
        end_year = request.form['endy']
        # os.system("luigid")

        luigi.run(main_task_cls=CombinedSimulation, cmdline_args=['--start=' + str(start_year), '--end=' + str(end_year), '--dem=' + str(dem), '--econ=' + str(econ)])

        return redirect('/bokeh')


@app.route('/bokeh')
def my_form_post2():
        return render_template("bokeh-results.html")


results_sql = '''SELECT "Population" as "Population"
                        ,"Year"
                        ,deaths_hhp_non_mil as "Deaths"
                        ,mig_in - mig_out as "Net Migration"
                        ,new_born as "Births"
                FROM defm.population_summary
                WHERE "Run_id" =''' + str(run_id) + ''' and yr >2010 ORDER BY "Year" '''
results_df = pd.read_sql(results_sql, defm_engine, index_col='Year')
feature_names = results_df.columns[0:].values.tolist()


@app.route('/bokeh/pop')
def my_form_post_pop():

    # Determine the selected feature
    current_feature_name = request.args.get("feature_name")

    if current_feature_name is None:
        current_feature_name = "Population"

    # Create the plot
    plot1 = create_figure(results_df, current_feature_name)
    # Embed plot into HTML via Flask Render
    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    # render template
    script, div = components(plot1)
    html = render_template(
        'result-form.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        feature_names=feature_names,
        current_feature_name=current_feature_name
    )
    return html


race_sql = '''SELECT  yr as "Year",
        SUM(CASE WHEN race_ethn = 'B' THEN persons ELSE 0 END) as "Black",
        SUM(CASE WHEN race_ethn = 'H' THEN persons ELSE 0 END) as "Hispanic",
        SUM(CASE WHEN race_ethn = 'S' THEN persons ELSE 0 END) as "Asian",
        SUM(CASE WHEN race_ethn = 'W' THEN persons ELSE 0 END) as "White",
        SUM(CASE WHEN race_ethn = 'O' THEN persons ELSE 0 END) as "Other"
        FROM defm.population
        WHERE run_id=''' + str(run_id) + ''' and yr >2010 GROUP BY yr  ORDER BY yr'''

race_df = pd.read_sql(race_sql, defm_engine, index_col='Year')
race_cat1 = race_df.columns[0:].values.tolist()


@app.route('/bokeh/pop_race')
def my_form_post_pop_by_race():

    # Determine the selected feature
    current_feature_name = request.args.get("feature_name")

    if current_feature_name is None:
        current_feature_name = "White"

    # Create the plot
    plot1 = create_figure(race_df, current_feature_name)
    # Embed plot into HTML via Flask Render
    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()
    p2 = figure(width=600, height=400, tools=[BoxZoomTool(), WheelZoomTool(), PanTool(), SaveTool()],
               title="All Races",  x_axis_label="Year")

    for r, color in zip(race_cat1, Spectral4):
        p2.line(race_df.index.tolist(), race_df[r], line_width=2, legend=r, color=color)
        p2.yaxis[0].formatter = NumeralTickFormatter(format="0,0.0")

    p2.legend.location = "top_left"
    p2.legend.background_fill_alpha = 0.5

    # render template
    script, div = components(row(plot1, p2))
    html = render_template(
        'result-form-pop-race.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        feature_names=race_cat1,
        current_feature_name=current_feature_name
    )
    return html


econ_sql = '''SELECT yr as "Year",
                     labor_force,
                     unemployed,
                     work_force,
                     work_force_outside,
                     work_force_local,
                     jobs_local,
                     jobs_total,
                     jobs_external,
                     avg_wage,
                     jobs_total_wages,
                     jobs_local_wages,
                     jobs_external_wages,
                     wf_outside_wages,
                     military_income,
                     unearned_income,
                     "Selfemp_Income",
                     personal_income,
                     taxable_retail_sales
                     FROM defm.emp_summary WHERE run_id = ''' + str(run_id) + ''' ORDER BY yr'''

econ_df = pd.read_sql(econ_sql, defm_engine, index_col='Year')
econ_cat = econ_df.columns[0:].values.tolist()


@app.route('/bokeh/econ')
def my_form_post_econ():

    # Determine the selected feature
    current_feature_name = request.args.get("feature_name")

    if current_feature_name is None:
        current_feature_name = "personal_income"

    # Create the plot
    plot1 = create_figure(econ_df, current_feature_name)
    # Embed plot into HTML via Flask Render
    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    # render template
    script, div = components(plot1)
    html = render_template(
        'result-form-econ.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        feature_names=econ_cat,
        current_feature_name=current_feature_name
    )
    return html

birth_sql = '''SELECT [birth_rate_id] as rate_id
      ,[yr] as "Year"
      ,CASE
            WHEN race = 'B' THEN 'Black'
            WHEN race = 'H' THEN 'Hispanic'
            WHEN race = 'S' THEN 'Asian'
            WHEN race = 'W' THEN 'White'
            WHEN race = 'O' THEN 'Other' ELSE 'None' END as race
      ,sum([birth_rate]) as fertility_rates
      FROM [isam].[demographic_rates].[birth_rates]
      GROUP BY yr, race, birth_rate_id
	  ORDER BY race, yr

      '''


birth_df = pd.read_sql(birth_sql, sql_in_engine, index_col=None)
rate_id_cat = birth_df.rate_id.unique()
race_cat = birth_df.race.unique()


@app.route('/birth_rates')
def my_form_post_brith_rates():

    # Determine the selected feature
    current_rate_id = request.args.get("rate")
    current_race_list = request.args.getlist("race_list1")

    if current_rate_id is None:
        current_rate_id = 101

    if len(current_race_list) == 0:
        current_race_list = race_cat

    # Create the plot
    # Embed plot into HTML via Flask Render
    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()
    name_str = " "
    for r in current_race_list:
        name_str = name_str + str(r) + ", "
    p2 = figure(width=600, height=400, tools=[BoxZoomTool(), WheelZoomTool(), PanTool(), SaveTool()],
                title=str(name_str), x_axis_label="Year", y_axis_label="Fertility Rates")

    for r, color in zip(current_race_list, Spectral4):
        df = birth_df.loc[(birth_df.rate_id == int(current_rate_id)) & (birth_df.race == r)]
        p2.line(df.Year.tolist(), df['fertility_rates'], line_width=2, legend=r, color=color)
        p2.yaxis[0].formatter = NumeralTickFormatter(format="0,0.0")
        source = ColumnDataSource(
            {'x': df.Year.tolist(), 'y': df['fertility_rates'], 'y2': df['fertility_rates'].map('{:,.2}'.format).tolist()})

        p2.scatter('x', 'y', source=source, fill_alpha=0, line_alpha=.95, line_color="black", fill_color="blue",
                   name='scat')
        hover = HoverTool(names=["scat"],
                          tooltips=[("Year ", "@x"), ("fertility_rates", "@y2"), ("Race", r)]
                          )

    p2.legend.location = "top_right"
    p2.legend.background_fill_alpha = 0.25
    p2.add_tools(hover)

    # render template
    script, div = components(row(p2))
    html = render_template(
        'birth-rates.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        rate_id_list=rate_id_cat,
        current_rate_id=current_rate_id,
        race_list=race_cat,
        current_race_list=current_race_list
    )
    return html


if __name__ == '__main__':
    shutil.rmtree('temp')
    os.makedirs('temp')
    app.run()

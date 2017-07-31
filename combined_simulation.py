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
    ColumnDataSource, HoverTool, BoxZoomTool, WheelZoomTool, PanTool, SaveTool
)
from bokeh.charts import TimeSeries
from bokeh.embed import components
from db import sql
from bokeh.resources import INLINE
from bokeh.plotting import figure

import warnings
warnings.filterwarnings('ignore', category=pandas.io.pytables.PerformanceWarning)
defm_engine = create_engine(get_connection_string("model_config.yml", 'output_database'))

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
    p = figure(width=600, height=400, tools=[HoverTool(), BoxZoomTool(), WheelZoomTool(), PanTool(), SaveTool()],
               title=current_feature_name, y_axis_label=current_feature_name,
                 x_axis_label="Year")
    p.line(df.index.tolist(), df[current_feature_name], line_width=2, legend=False, line_color="red")

    source = ColumnDataSource(
        {'x': df.index.values.tolist(), 'y': df[current_feature_name].values, 'y2': df[current_feature_name].map('{:,.0f}'.format).tolist()})

    p.scatter('x', 'y', source=source, fill_alpha=0, line_alpha=.95, line_color="black", fill_color="blue")
    hover = p.select(dict(type=HoverTool))
    hover.tooltips = [("Year ", "@x"),  (current_feature_name, "@y2")]
    hover.mode = 'mouse'
    hover.line_policy = 'nearest'
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
                WHERE "Run_id" =''' + str(run_id)
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
        WHERE run_id=''' + str(run_id) + ''' GROUP BY yr  ORDER BY yr'''

race_df = pd.read_sql(race_sql, defm_engine, index_col='Year')
race_cat = race_df.columns[0:].values.tolist()


@app.route('/bokeh/pop_age')
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

    # render template
    script, div = components(plot1)
    html = render_template(
        'result-form-pop-race.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
        feature_names=race_cat,
        current_feature_name=current_feature_name
    )
    return html

if __name__ == '__main__':
    shutil.rmtree('temp')
    os.makedirs('temp')
    app.run()

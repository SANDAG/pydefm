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
from db import sql
from pysandag import database
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


@app.route('/')
def my_form():

    econ_sim_ids = extract.create_df('econ_sim_ids', 'econ_sim_ids_table', rate_id=0, index=None)
    dem_sim_ids = extract.create_df('dem_sim_ids', 'dem_sim_ids_table', rate_id=0, index=None)
    dems = zip(dem_sim_ids['demographic_simulation_id'], dem_sim_ids['desc_short'])
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
                WHERE "Run_id" =''' + str(run_id) + ''' and "Year"  >2010 ORDER BY "Year" '''
results_df = pd.read_sql(results_sql, defm_engine, index_col='Year')
feature_names = results_df.columns[0:].values.tolist()


@app.route('/bokeh/pop')
def my_form_post_pop():

    # Determine the selected feature
    current_feature_name = request.args.get("feature_name")

    if current_feature_name is None:
        current_feature_name = "Population"

    listx= results_df.index.values.tolist()
    new_list = []
    for item in listx:
        new_list.append(str(item))
    listy= results_df[current_feature_name].tolist()
    new_list2 = []
    for item in listy:
        new_list2.append(int(item))

    chart = {"renderTo": 'chart_ID', "type": 'line', "height": 450}
    series = [{"name": str(current_feature_name), "data": new_list2}]
    title = {"text": str(current_feature_name) + ' Trends'}
    xAxis = {"title": {"text": 'Year'}, "categories": new_list}
    yAxis = {"title": {"text": 'Persons'}}
    # render template
    html = render_template(
        'result-form.html',
        feature_names=feature_names,
        current_feature_name=current_feature_name,
        chartID='chart_ID', chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis
    )
    return html


race_sql = '''SELECT  yr as "Year",
        SUM(CASE WHEN race_ethn = 'B' THEN persons ELSE 0 END) as "Black (NH)",
        SUM(CASE WHEN race_ethn = 'H' THEN persons ELSE 0 END) as "Hispanic",
        SUM(CASE WHEN race_ethn = 'S' THEN persons ELSE 0 END) as "Asian (NH)",
        SUM(CASE WHEN race_ethn = 'W' THEN persons ELSE 0 END) as "White (NH)",
        SUM(CASE WHEN race_ethn = 'O' THEN persons ELSE 0 END) as "Other (NH)"
        FROM defm.population
        WHERE run_id=''' + str(run_id) + ''' and yr >2010 GROUP BY yr  ORDER BY yr'''

race_df = pd.read_sql(race_sql, defm_engine, index_col='Year')
race_cat1 = race_df.columns[0:].values.tolist()


@app.route('/bokeh/pop_race')
def my_form_post_pop_by_race():

    # Determine the selected feature
    current_race_list = request.args.getlist("race_list1")

    if len(current_race_list) == 0:
        current_race_list = race_cat1

    listx = race_df.index.values.tolist()
    new_list = []
    for item in listx:
        new_list.append(str(item))

    series = []
    for x in current_race_list:
        listy = race_df[str(x)].tolist()
        new_list2 = []
        for item in listy:
            new_list2.append(float(item))
        series.append({"name": str(x), "data": new_list2})

    chart = {"renderTo": 'chart_ID', "type": 'area', "height": 600, "width": 1000}
    title = {"text": 'Population by Race Trends'}
    xAxis = {"title": {"text": 'Year'}, "categories": new_list}
    yAxis = {"title": {"text": 'Count / Persons'}}

    plotOptions = {"area": {"stacking": 'percent'},  "lineColor": '#ffffff',
            "lineWidth": '1',
            "marker": {
                "lineWidth": '1',
                "lineColor": '#ffffff'
            }
        }
    # render template
    html = render_template(
        'result-form-pop-race.html',
        race_list=race_cat1,
        current_race_list=current_race_list,
        chartID='chart_ID', chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis, plotOptions=plotOptions
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

    listx = econ_df.index.values.tolist()
    new_list = []
    for item in listx:
        new_list.append(str(item))
    listy = econ_df[current_feature_name].tolist()
    new_list2 = []
    for item in listy:
        new_list2.append(float(item))

    chart = {"renderTo": 'chart_ID', "type": 'line', "height": 450}
    series = [{"name": str(current_feature_name), "data": new_list2}]
    title = {"text": str(current_feature_name) + ' Trends'}
    xAxis = {"title": {"text": 'Year'}, "categories": new_list}
    yAxis = {"title": {"text": 'Count / Persons'}}
    # render template
    html = render_template(
        'result-form-econ.html',
        feature_names=econ_cat,
        current_feature_name=current_feature_name,
        chartID='chart_ID', chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis
    )
    return html

birth_sql = '''SELECT [birth_rate_id] as rate_id
      ,[yr] as "Year"
      ,CASE
            WHEN race = 'B' THEN 'Black (NH)'
            WHEN race = 'H' THEN 'Hispanic'
            WHEN race = 'S' THEN 'Asian (NH)'
            WHEN race = 'W' THEN 'White (NH)'
            WHEN race = 'O' THEN 'Other (NH)' ELSE 'None' END as race
      ,sum([birth_rate]) as fertility_rates
      FROM [isam].[demographic_rates].[birth_rates]
      GROUP BY yr, race, birth_rate_id
      ORDER BY race, yr '''


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

    listx = birth_df.Year.unique()
    new_list = []
    for item in listx:
        new_list.append(str(item))

    series = []
    for x in current_race_list:
        df = birth_df.loc[(birth_df.rate_id == int(current_rate_id)) & (birth_df.race == str(x))]
        listy = df['fertility_rates'].tolist()
        new_list2 = []
        for item in listy:
            new_list2.append(float(item))
        series.append({"name": str(x), "data": new_list2})

    chart = {"renderTo": 'chart_ID', "type": 'line', "height": 600, "width": 1000}
    title = {"text": 'Fertility Rates by Race'}
    xAxis = {"title": {"text": 'Year'}, "categories": new_list}
    yAxis = {"title": {"text": 'Fertility Rates'}}
    # render template
    html = render_template(
        'birth-rates.html',
        rate_id_list=rate_id_cat,
        current_rate_id=current_rate_id,
        race_list=race_cat,
        current_race_list=current_race_list,
        chartID='chart_ID', chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis
    )
    return html


death_sql = '''SELECT [death_rate_id] as rate_id
      ,[yr] as "Year"
      ,[age]
      ,CASE
            WHEN [race]+[sex] = 'BF' THEN 'Black (NH) - Female'
            WHEN [race]+[sex] = 'BM' THEN 'Black (NH) - Male'
            WHEN [race]+[sex]= 'HF' THEN 'Hispanic - Female'
            WHEN [race]+[sex]= 'HM' THEN 'Hispanic - Male'
            WHEN [race]+[sex] = 'SF' THEN 'Asian (NH) - Female'
            WHEN [race]+[sex] = 'SM' THEN 'Asian (NH) - Male'
            WHEN [race]+[sex] = 'WF' THEN 'White (NH) - Female'
            WHEN [race]+[sex] = 'WM' THEN 'White (NH) - Male'
            WHEN [race]+[sex] = 'OF' THEN 'Other (NH) - Female'
            WHEN [race]+[sex] = 'OM' THEN 'Other (NH) - Male' ELSE 'None' END as race
      ,[death_rate]
  FROM [isam].[demographic_rates].[death_rates]
  WHERE age < 100
  ORDER BY age, yr'''


death_df = pd.read_sql(death_sql, sql_in_engine, index_col=None)
death_rate_id_cat = death_df.rate_id.unique()
death_year_cat = death_df.Year.unique()
death_race_cat = death_df.race.unique()


@app.route('/death_rates')
def my_form_post_death_rates():

    # Determine the selected feature
    current_rate_id = request.args.get("rate")
    current_year_id = request.args.get("year")
    current_race_list = request.args.getlist("race_list1")

    if current_rate_id is None:
        current_rate_id = 101

    if current_year_id is None:
        current_year_id = death_year_cat.min()

    if len(current_race_list) == 0:
        current_race_list = death_race_cat

    listx = death_df.Year.unique()
    new_list = []
    for item in listx:
        new_list.append(str(item))

    series = []
    for x in current_race_list:
        df = death_df.loc[(death_df.rate_id == int(current_rate_id)) & (death_df.race == x) &
                          (death_df.Year == int(current_year_id))]

        listy = df['death_rate'].tolist()
        new_list2 = []
        for item in listy:
            new_list2.append(float(item))
        series.append({"name": str(x), "data": new_list2})

    chart = {"renderTo": 'chart_ID', "type": 'line', "height": 600, "width": 1000}
    title = {"text": 'Death Rates by Race'}
    xAxis = {"title": {"text": 'Year'}, "categories": new_list}
    yAxis = {"title": {"text": 'Death Rates'}}
    # render template
    html = render_template(
        'death-rates.html',
        year_list=death_year_cat,
        current_year_id=current_year_id,
        rate_id_list=rate_id_cat,
        current_rate_id=current_rate_id,
        race_list=death_race_cat,
        current_race_list=current_race_list,
        chartID='chart_ID', chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis
    )
    return html

if __name__ == '__main__':
    shutil.rmtree('temp')
    os.makedirs('temp')
    app.run()

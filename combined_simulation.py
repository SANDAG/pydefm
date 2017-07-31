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
from db import sql
from forecast import util
import shutil
import pandas
import luigi.contrib.hadoop
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
from pysandag import database
import warnings
warnings.filterwarnings('ignore', category=pandas.io.pytables.PerformanceWarning)


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
    os.system("bokeh serve bokeh_graphs.py")

    return '''
    <form action="." method="BOKEH">
         <div class="w3-row-padding">
            <div class="w3-col l3 m6 w3-margin-bottom">
                 <h5 class="w3-border-bottom w3-border-light-grey w3-padding-16">Click below to check results when the simulation is complete </h5>
                 <input class="w3-button w3-black w3-section" type="submit" value="Bokeh Results" onclick="window.open('http://localhost:5006/bokeh_graphs')">
            </div>
         </div>
    </form>" onclick="window.open('http://localhost:5006/bokeh_graphs')"> '''


if __name__ == '__main__':
    shutil.rmtree('temp')
    os.makedirs('temp')
    app.run()

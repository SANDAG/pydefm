import os
import luigi
import shutil
import defm_luigi as defm
import inc_luigi as inc
import emp_luigi as emp
import sys
import time


class CombinedSimulation(luigi.Task):

    def requires(self):
        return {'def': defm.Iter(),
                'inc': inc.IncomeByType(),
                'emp': emp.SectoralPay()}

    def output(self):
        return luigi.LocalTarget('temp/data.h5')

    def run(self):
        print 'Completed combined simulation'


if __name__ == '__main__':
    os.makedirs('temp')
    luigi.run(main_task_cls=CombinedSimulation)
    shutil.rmtree('temp')

    os.system("bokeh serve bokeh_graphs.py")
    time.sleep(600)
    sys.exit()

import os
import luigi
import shutil
import defm_luigi as defm
import inc_luigi as inc
import emp_luigi as emp

if __name__ == '__main__':

    for task in [defm.Iter, inc.IncomeByType, emp.SectoralPay]:
        os.makedirs('temp')
        luigi.run(main_task_cls=task)
        shutil.rmtree('temp')

    os.system("bokeh serve bokeh_graphs.py")
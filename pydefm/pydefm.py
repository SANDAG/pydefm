import luigi
import inspect
import os
import pandas as pd
import time


class BasePopulationLoad(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('population.csv')

    def run(self):
        time.sleep(15)


class InMigrationRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('in_migration.csv')

    def run(self):
        time.sleep(15)


class OutMigrationRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('out_migration.csv')

    def run(self):
        time.sleep(15)


class BirthRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('births.csv')

    def run(self):
        time.sleep(15)


class DeathRates(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('deaths.csv')

    def run(self):
        time.sleep(15)


class DeathPop(luigi.Task):
    def requires(self):
        return {'base_pop': NonMigPop(),
                'death_rates': DeathRates()}

    def output(self):
        return luigi.LocalTarget('deaths.csv')

    def run(self):
        time.sleep(15)


class MigOutPop(luigi.Task):
    def requires(self):
        return {'base_pop': BasePopulationLoad(),
                'out_mig_rates': OutMigrationRates()}

    def output(self):
        return luigi.LocalTarget('new_population.csv')

    def run(self):
        time.sleep(15)


class BirthPop(luigi.Task):
    def requires(self):
        return {'base_pop': NonMigSurvivedPop(),
                'death_rates': BirthRates()}

    def output(self):
        return luigi.LocalTarget('deaths.csv')

    def run(self):
        time.sleep(15)


class MigInPop(luigi.Task):
    def requires(self):
        return {'base_pop': BasePopulationLoad(),
                'out_mig_rates': InMigrationRates()}

    def output(self):
        return luigi.LocalTarget('new_population.csv')

    def run(self):
        time.sleep(15)


class NonMigPop(luigi.Task):
    def requires(self):
        return {
                'out_mig': MigOutPop(),
                }

    def output(self):
        return luigi.LocalTarget('aged_population.csv')

    def run(self):
        time.sleep(15)


class NonMigSurvivedPop(luigi.Task):
    def requires(self):
        return {
                'out_mig_pop': NonMigPop(),
                'death_pop': DeathPop()
                }

    def output(self):
        return luigi.LocalTarget('aged_population.csv')

    def run(self):
        time.sleep(15)


class NewPopulation(luigi.Task):
    def requires(self):
        return {'birth_pop': BirthPop(),
                'in_mig_pop': MigInPop()}

    def output(self):
        return luigi.LocalTarget('new_population.csv')

    def run(self):
        time.sleep(15)


class AgedPopulation(luigi.Task):
    def requires(self):
        return NonMigSurvivedPop()

    def output(self):
        return luigi.LocalTarget('aged_population1.csv')

    def run(self):
        time.sleep(15)


class FinalPopulation(luigi.Task):
    def requires(self):
        return {'aged_population': AgedPopulation(),
                'new_population': NewPopulation()
                }

    def output(self):
        return luigi.LocalTarget('final_population2.csv')

    def run(self):
        time.sleep(15)


if __name__ == '__main__':
    luigi.run(main_task_cls=FinalPopulation)
    
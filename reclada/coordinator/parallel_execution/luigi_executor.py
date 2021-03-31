#!/usr/bin/python3.8

import logging
import luigi
import time
import random

logger = logging.getLogger("luigi-interface." + __name__)


class GenerateData(luigi.Task):
    out_prefix = luigi.Parameter()
    numbers_amount = luigi.IntParameter()  # type: int
    generate_sleep = luigi.FloatParameter(default=0)  # type: float

    def output(self):
        out_file = f'{self.out_prefix}_data.txt'
        return luigi.LocalTarget(out_file)

    def run(self):
        time.sleep(self.generate_sleep)
        with self.output().open('w') as f:
            for _ in range(self.numbers_amount):
                f.write(f'{random.randint(0,100)}\n')


class CalcSum(luigi.Task):
    out_prefix = luigi.Parameter()
    numbers_amount = luigi.IntParameter()  # type: int
    generate_sleep = luigi.FloatParameter(default=0)  # type: float
    calc_sleep = luigi.FloatParameter(default=0)  # type: float

    def requires(self):
        return GenerateData(self.out_prefix, self.numbers_amount, self.generate_sleep)

    def output(self):
        out_file = f'{self.out_prefix}.txt'
        return luigi.LocalTarget(out_file)

    def run(self):
        with self.input().open('r') as infile:
            data = infile.read().splitlines()
        total_sum = sum(map(int, data))
        time.sleep(self.calc_sleep)
        with self.output().open('w') as outfile:
            outfile.write(f'total_sum {total_sum}')


class CalcAmount(CalcSum):
    def run(self):
        with self.input().open('r') as infile:
            data = infile.read().splitlines()
        total_amount = len(data)
        time.sleep(self.calc_sleep)
        with self.output().open('w') as outfile:
            outfile.write(f'total_amount {total_amount}')


class SleepTask(luigi.Task):
    task_complete = False
    sleep_time = luigi.FloatParameter(default=0)  # type: float

    def run(self):
        time.sleep(self.sleep_time)
        self.task_complete = True

    def complete(self):
        return self.task_complete


if __name__ == '__main__':
    FIRST_TASK_EXECUTION = 2
    SECOND_TASK_EXECUTION = 3
    ITERATION_AMOUNT = 2
    for _ in range(ITERATION_AMOUNT):
        cur_ts = time.strftime("%Y-%m-%d_%H-%M-%S")
        start = time.time()
        luigi.build(
            [
                CalcSum(f'{cur_ts}_calc_sum', 100, FIRST_TASK_EXECUTION, SECOND_TASK_EXECUTION),
                CalcAmount(f'{cur_ts}_calc_amount', 200, FIRST_TASK_EXECUTION, SECOND_TASK_EXECUTION),
                SleepTask(FIRST_TASK_EXECUTION)
            ],
            local_scheduler=True,
            workers=3
        )
        logger.debug(f"Execution time: {time.time() - start}")

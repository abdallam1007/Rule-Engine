import time
from apscheduler.schedulers.background import BackgroundScheduler

import rule_engine
import database_engine


def schedule_rule(scheduler, mongo_db, rule):

    # Parse the cron to extract hours, minutes and seconds
    cron = rule["cron"]
    hours, minutes, seconds = map(int, cron.split())

    # Create a new job that will run every h:m:s starting from the current time
    scheduler.add_job(rule_engine.apply_rule, 'interval',
                      hours=hours,
                      minutes=minutes,
                      seconds=seconds,
                      args=[mongo_db, rule])


def init_scheduler(mongo_db, rules):

    # get a scheduler object from the apscheduler library
    scheduler = BackgroundScheduler()

    for rule in rules:
        # * since this is just a dummy database and not a fully
        # * integrated one, we have to put some updates in the database
        # * before actually running the rule
        database_engine.insert_new_updates(rule)

        # Create a job on the scheduler for the rule
        schedule_rule(scheduler, mongo_db, rule)

    # start running the scheduler
    scheduler.start()

    try:
        # Keep the scheduler thread alive by sleeping
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('The Scheduler is Shutting Down!!!')
        scheduler.shutdown()
import time
from apscheduler.schedulers.background import BackgroundScheduler

import rule_engine

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
    scheduler = BackgroundScheduler()

    # Create a job for each rule
    for rule in rules:
        schedule_rule(scheduler, mongo_db, rule)

    scheduler.start()

    try:
        # Keep the scheduler thread alive by sleeping
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('The Scheduler is Shutting Down!!!')
        scheduler.shutdown()
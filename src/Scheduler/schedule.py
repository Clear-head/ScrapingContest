import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR



class SchedulerService:
    def __init__(self, conn, func):
        self.conn = conn.get_cursor()
        self.scheduler = AsyncIOScheduler()
        self.func = func

        self.scheduler.add_listener(self.task_finish_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


    def schedule_task(self):

        try:
            time = datetime.datetime.now() + datetime.timedelta(seconds=10)
            if not self.check_last_schedule():
                time = datetime.datetime.now() + datetime.timedelta(days=1)

            time = time.strftime("%Y-%m-%d 04:04:04")


            job = self.scheduler.add_job(
                func=self.func,
                next_run_time=  datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S"),
                id="0",
                name="Scrap contest per a day",
                timezone="Asia/Seoul",
                replace_existing=True
            )
            print(f"[Schedule] next scraping time: {job.next_run_time.isoformat()}")

        except Exception as e:
            print(f"[Schedule] Error checking existing jobs: {e}")
            raise e

        print(f"[Schedule] scheduled job")

    def run(self):
        try:
            self.scheduler.start()
        except Exception as e:
            print(f"[Schedule] Error checking existing scheduled jobs: {e}")
        print(f"[Schedule] Scheduler successfully started")



    def task_finish_listener(self, event):
        if event.exception:
            print(f"[Schedule] Schedule Error: {event.exception}")

        else:
            print(f"[Schedule] Schedule task successfully scheduled: {event.job_id}")
            self.schedule_task()


    def check_last_schedule(self):
        print(f"[Schedule] recent schedule check")
        try:
            last_task = self.conn.get('last_scheduled')

            if not last_task:
                print(f"[Schedule] no last_scheduled task, scrap now!")
                return True

            now = datetime.datetime.now()
            recent_task = datetime.datetime.strptime(last_task.decode("utf-8"), '%Y-%m-%d')
            if now > recent_task + datetime.timedelta(days=1):
                print(f"[Schedule] recent task {recent_task}, scrap now!")
                return True
            else:
                print(f"[Schedule] recent task {recent_task}, scrap later~!")
                return False
        except Exception as e:
            print(f"[Schedule] Error checking recent task: {e}")
            return True

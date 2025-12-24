import datetime


class Scheduler:
    @staticmethod
    def get_scheduled_time(minute_to_start: int) -> str:
        minute_to_start = datetime.timedelta(minutes=minute_to_start)

        now = datetime.datetime.now()
        scheduled_time = now + minute_to_start
        
        at = str(scheduled_time.astimezone().isoformat())[:-6] + "Z"

        return at
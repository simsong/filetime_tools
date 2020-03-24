from crontab import CronTab
cron = CronTab(user='root')
job = cron.new(command='python3 ftt_live_update.py --config dbroot.bash')
job.minute.every(5)
job = cron.new(command='python3 delete_file_reports.py --config dbroot.bash')
job.minute.every(5)
cron.write()
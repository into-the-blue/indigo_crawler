import os
from pathlib import Path
from dotenv import load_dotenv
env_pth = Path(os.path.abspath(os.path.dirname(__file__)))/'..'/'dev.env'
load_dotenv(env_pth)
from job import testing_job, testing_job2
from mq import q, q2
from mw import start_worker
from multiprocessing import Pool


print(Path(os.path.abspath(os.path.dirname(__file__)))/'..'/'dev.env')
for i in range(4):
    q.enqueue(testing_job, args=(4,))
    q2.enqueue(testing_job2, args=(5,))

if __name__ == '__main__':
    try:
        p = Pool(4)
        for i in range(4):
            if i == 0:
                p.apply_async(start_worker, args=[['test2','test']])
            else:
                p.apply_async(start_worker, args=[['test','test2']])
        p.close()
        p.join()
    except:
        print('quit')

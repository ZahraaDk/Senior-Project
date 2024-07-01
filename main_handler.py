from prehook import execute_prehook
from hook import execute_hook
from posthookk import execute_posthook
import logging 

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

execute_prehook()
execute_hook()
execute_posthook()
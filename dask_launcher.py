import os
import subprocess
import ipywidgets as widgets
from IPython.lib import backgroundjobs as bg
from IPython.display import display, clear_output, HTML
import dask
from dask.distributed import Client


from datetime import datetime
def now():
    now = datetime.now()
    return now.strftime("%H:%M:%S")


class DaskLauncher():
    def __init__(self):
        image = time = ntasks = cpus = ''

        self.title = HTML('<h3>Dask Launcher Config</h3>')
        self.image = widgets.Text(description='Shifter Img', placeholder='None', value=image)
        self.time = widgets.Text(description='Time', placeholder='30', value=time)
        self.ntasks = widgets.Text(description='NTasks', placeholder='32', value=ntasks)
        self.cpus = widgets.Text(description='CPUs/Task', placeholder='2', value=cpus)
        self.button = widgets.Button(description="Launch Dask")
        self.button.on_click(self.handle_submit)
        self.output = widgets.Output()

        self.client = None
        self._button_clicked = False
    
    def display(self):
        display(self.title, self.image, self.time, self.ntasks, self.cpus, self.button, self.output)

    def handle_submit(self, b):
        if self._button_clicked:
            err = ('Error: you have already clicked Launch Dask. '
                   'You can reload the widget to try to relaunch, but check my.nersc.gov '
                   'to ensure you don\'t have unknown jobs running.')
            with self.output:
                raise Exception(err)
            raise Exception(err)
        
        self.validate_input()
        
        cmd = self.format_command()

        cmd_prefix = f'cd {os.environ["SCRATCH"]} && module load nersc-dask && '
        test_cmd = f'bash -c "{cmd_prefix + cmd} --test"'
        p = subprocess.Popen(test_cmd, shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()

        bash_cmd = f'bash -c "{cmd_prefix + cmd}"'
        jobs = bg.BackgroundJobManager()
        jobs.new('subprocess.Popen(bash_cmd, shell=True)')
        self._button_clicked = True

        self.print_display(out)
        
    def validate_input(self):
        if self.time.value != '' and (not self.time.value.isnumeric() or float(self.time.value) < 0):
            raise Exception('ValueError: invalid time value')
        if not self.ntasks.value.isdigit() and self.ntasks.value != '':
            raise Exception('ValueError: invalid ntasks value')
        if not self.cpus.value.isdigit() and self.cpus.value != '':
            raise Exception('ValueError: invalid cpus per task value')

    def print_display(self, out):
        clear_output()
        with self.output:
            clear_output()
            print(f'({now()}) Launching Dask using the following salloc call:')
            print(out.decode('utf-8').replace('Test only, exiting', ''))

            print('Run the cell below to connect your Dask client. This may take a couple minutes.')
            print('You can view the status of your Dask job at my.nersc.gov')

    def get_client(self):
        if self._button_clicked:
            scheduler_file = os.path.join(os.environ["SCRATCH"], 'scheduler.json')
            url = '{JUPYTERHUB_SERVICE_PREFIX}proxy/{host}:{port}/status'
            dask.config.config["distributed"]["dashboard"]["link"] = url

            self.client = Client(scheduler_file=scheduler_file)
            return self.client
        else:
            raise Exception('Error: client is not initalized. Did you click Launch Dask?')

    def shutdown(self):
        if self.client:
            self.client.shutdown()
            self.client.close()
            self.client = None
            self._button_clicked = False

    def format_command(self):
        command = 'start-dask-mpi'
        if self.image.value != '':
            command += f' --image={self.image.value}'
        if self.time.value != '':
            command += f' --time={self.time.value}'
        if self.ntasks.value != '':
            command += f' --ntasks={self.ntasks.value}'
        if self.cpus.value != '':
            command += f' --cpus-per-task={self.cpus.value}'

        return command

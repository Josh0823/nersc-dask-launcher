import ipywidgets as widgets
from IPython.display import display, clear_output, HTML

class dask_launcher():
    def __init__(self, 
                 image = '', 
                 nodes = '', 
                 time = '',
                 ntasks = '',
                 cpus = ''
                ):
        self.title = HTML('<h3>Dask Launcher Config</h3>')
        self.image = widgets.Text(description = 'Image', placeholder='None', value = image)
        self.nodes = widgets.Text(description = 'Nodes', placeholder = '8', value = nodes)
        self.time = widgets.Text(description = 'Time', placeholder = '30', value = time)
        self.ntasks = widgets.Text(description = 'NTasks', placeholder = '256', value = ntasks)
        self.cpus = widgets.Text(description = 'CPUs/Task', placeholder = '2', value= cpus)
        self.button = widgets.Button(description="Generate Command")
        self.button.on_click(self.handle_submit)
        self.output = widgets.Output()

        display(self.title, self.image, self.nodes, self.time, self.ntasks, self.cpus, self.button, self.output)


    def handle_submit(self, b):
        with self.output:
            clear_output()
            command = 'start-dask-mpi'
            get_ipython().set_next_input(self.format_command(), replace=False)
            print('Rerun the above cell to format your new cell')
    
    def format_command(self):
        command = 'start-dask-mpi'
        if self.image.value != '':
            command += f' --image={self.image.value}'
        if self.nodes.value != '':
            command += f' --nodes={self.nodes.value}'
        if self.time.value != '':
            command += f' --time={self.time.value}'
        if self.ntasks.value != '':
            command += f' --ntasks={self.ntasks.value}'
        if self.cpus.value != '':
            command += f' --cpus-per-task={self.cpus.value}'
            
        full_command = '# Run the following in a terminal and wait for the program to start\n'
        full_command += f'\'\'\'\ncd $SCRATCH\nmodule load nersc-dask\n{command}\n\'\'\'\n'
        full_command += '# Then execute this cell \n\n'
        full_command += 'import os\nimport dask\nfrom dask.distributed import Client\n\n'
        full_command += 'scheduler_file = os.path.join(os.environ["SCRATCH"], "scheduler.json")\n'
        full_command += 'dask.config.config["distributed"]["dashboard"]["link"] = "{JUPYTERHUB_SERVICE_PREFIX}proxy/{host}:{port}/status"\n\n'
        full_command += 'client = Client(scheduler_file=scheduler_file)'
        return full_command

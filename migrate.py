import click, importlib
from app import create_app

app = create_app()


@click.command()
@click.option('-s', '--sprint', help="指定迭代号，即/migrates目录下的那个子目录将被执行")
@click.option('-m', '--module', help="模块", default="explorer")
def pre_process(sprint, module):
    """Loads a set of Slices and Dashboards and a supporting dataset """
    upgrade_service = importlib.import_module(f'migrates.{sprint}.pre_{module}')
    print(f'Preprocessing of {sprint}.{module}!')

    upgrade_service.PreProcess(app)

    print(f'Finished preprocessing of {sprint}.{module}!')


if __name__ == '__main__':
    pre_process()

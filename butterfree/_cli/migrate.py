import typer

app = typer.Typer()


@app.command()
def migrate(path, diff_only: bool = True):
    # navigate trough path looking for feature set pipelines
    # if diff_only, navigate only through the files with diff
    # instantiate feature set, call migrate with object

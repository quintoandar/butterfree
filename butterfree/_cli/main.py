import typer

from butterfree._cli import migrate

app = typer.Typer()
app.add_typer(migrate.app, name="migrate")

if __name__ == "__main__":
    app()

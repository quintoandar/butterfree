import typer

from butterfree._cli import migrate

app = typer.Typer()
app.add_typer(migrate.app)

if __name__ == "__main__":
    app()

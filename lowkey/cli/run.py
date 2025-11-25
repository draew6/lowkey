import typer
from fastlet.cli.run import export_file, start as fastlet_start

app = typer.Typer()


@app.command()
def start():
    fastlet_start()
    export_file("requirements-base.txt", src_dir=__file__, mode="a")
    export_file("Dockerfile", src_dir=__file__, mode="w")

def main():
    app()

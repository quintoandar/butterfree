import os
from pathlib import Path
from subprocess import Popen, PIPE

if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    example_notebook_paths = [
        str(path)
        for path in list(Path(dir_path).rglob("*.ipynb"))
        if ".ipynb_checkpoints" not in str(path)
    ]

    print("\n>>> Notebook Examples Tests")
    errors = []
    for path in example_notebook_paths:
        print(f"    >>> Running {path}")

        p = Popen(
            [
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--inplace",
                "--no-prompt",
                "--execute",
                "--log-level='ERROR'",
                path,
            ],
            stdout=PIPE,
            stderr=PIPE,
        )

        _, error = p.communicate()
        if p.returncode != 0:
            errors.append({"notebook": path, "error": error})
            print(f"    >>> Error in execution!\n")
        else:
            print(f"    >>> Successful execution\n")

    if errors:
        print(">>> Errors in the following notebooks:")
        for run in errors:
            print("\n    >>>", run["notebook"])
            print(run["error"].decode("utf-8"))

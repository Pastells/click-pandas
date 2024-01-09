# View data as pandas DataFrame in terminal

Works for .csv, .tsv and excel files.

## Credits

The code and folder structure are taken from :

- https://click.palletsprojects.com/en/7.x/#documentation
- https://github.com/pallets/click/tree/master/examples/imagepipe

## Requirements

- Python 3.7 or greater

## Installation

1. Get the source with git clone or download the repo
2. With a terminal/IDE inside the root project directory run :
   `pip install --editable .`

## Execution

- `pd read file head`
- `pd read file tail`
- `pd read file filter "metric > 0.8" head`

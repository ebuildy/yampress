# yampress

Reduce your YAML file size! yampress is a small python script to compress YAML file by using anchor.

yampress detect duplicated content inside YAML file, and create anchors.

## Usage

```python
Usage: index.py [OPTIONS]

Options:
  --file TEXT
  --candidates         Just print candidates
  --min-count INTEGER  Occurences count filter
  --min-size INTEGER   Content size filter
  --help               Show this message and exit.

# Example
python index.py --file samples/sample.yaml --min-size 100
```

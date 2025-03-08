# h5pack
Utility to create and expand `.h5` audio datasets.

## Example `.yaml` file
```yaml
datasets:
  first_example_dataset:
    attrs:
      author: &author_name "Esteban Gómez"
      description: "First dataset description"
      version: &version "0.0.1"

    data:
      file: "first_dataset.csv"
      fields:
        audio:
          column: "file"
          parser: "as_audioint16"
  
  second_example_dataset:
    attrs:
      author: *author_name
      description: "Second dataset description"
      version: *version
    
    data:
      file: "second_dataset.csv"
      fields:
        audio:
          column: "file"
          parser: "as_audioint16"
```

- `attrs` can have any string attributes.
- `data` needs a `.csv` in the `file` key and every field in `fields` needs a `column` and a `parser` description.
- Multiple datasets can be added in a single file under the `datasets` key.

# Cite
If this package contributed to your work, please consider citing it:

```
@misc{moduleprofiler,
  author = {Esteban Gómez},
  title  = {h5pack},
  year   = 2024,
  url    = {https://github.com/eagomez2/h5pack}
}
```

This package was developed by <a href="https://estebangomez.me/" target="_blank">Esteban Gómez</a>, member of the <a href="https://www.aalto.fi/en/department-of-information-and-communications-engineering/speech-interaction-technology" target="_blank">Speech Interaction Technology group from Aalto University</a>.

# License
For further details about the license of this package, please see [LICENSE](LICENSE).

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

## Copyright
Created by Esteban Gómez (esteban.gomezmellado@aalto.fi).

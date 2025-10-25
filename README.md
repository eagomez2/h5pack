# h5pack
`h5pack` is a utility made to pack, analyze and unpack HDF5 audio datasets using data and annotations of various sources. HDF5 is an open-source file format for storing large, complex data. It uses a directory-like structure to organize data within the file. Click <a href="https://www.hdfgroup.org/solutions/hdf5/" target="_blank">here</a> to learn more about HDF5. Among other, HDF5 provides:

- Efficient storage: Handles large datasets with chunking and compression, reducing disk space usage.
- Fast I/O: Optimized for rapid reading/writing of multi-dimensional data, improving data loading speeds.
- Hierarhical structures: Organizes metadata, features, and audio data efficiently within a single file.
- Concurrent access: Supports concurrent access, enabling efficient multi-threaded data loading.
- Scalability: Handles massive datasets without performance degradation, making it ideal for large-scale machine learning tasks.
- Cross-platform: Works across different OS and programming languages, ensuring flexibility.

`h5pack` was made to go from raw files to HDF5 files and back in a robust, consistent and simple way. It provides a collection of tools to facilitate all the necessary tasks to make it possible:

- `h5pack pack`: Converts raw data and/or annotation files into an HDF5 file.
- `h5pack unpack`: Extracts raw data from an HDF5 file, allowing regeneration of the original input data.
- `h5pack virtual`: Creates a virtual dataset by combining multiple datasets into a single logical dataset without duplication, enabling seamless access to fragmented or distributed data.
- `h5pack info`: Displays the contents of an HDF5 file generated with `h5pack`, providing a quick overview of its structure.
- `h5pack checksum`: Verifies the integrity of an HDF5 file by checking its checksum to detect potential corruption.

# Table of contents
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Documentation](#documentation)
- [Cite](#cite)
- [License](#license)

# Installation
See how to install `h5pack` <a href="https://eagomez2.github.io/h5pack/install/" target="_blank">here</a>.

# Quickstart
Explore the <a href="https://eagomez2.github.io/h5pack/quickstart/" target="_blank">Quickstart Guide</a> to start using `h5pack` right away. It offers step-by-step instructions to get you set up quickly and effortlessly.

# Documentation
Access the <a href="https://eagomez2.github.io/h5pack/" target="_blank">Documentation</a> to explore all the tools included in this package.

# Cite
`h5pack` is free and open source. If this package contributed to your work, please consider citing it:

```
@misc{h5pack,
  author = {Esteban Gómez},
  title  = {h5pack},
  year   = 2025,
  url    = {https://github.com/eagomez2/h5pack}
}
```

This package was developed by <a href="https://estebangomez.me/" target="_blank">Esteban Gómez</a>, member of the <a href="https://www.aalto.fi/en/department-of-information-and-communications-engineering/speech-interaction-technology" target="_blank">Speech Interaction Technology group from Aalto University</a>.

# License
For further details about the license of this package, please see [LICENSE](LICENSE).

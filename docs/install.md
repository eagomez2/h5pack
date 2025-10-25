# Installation

## Install using pip
The easiest way to install `h5pack` is through `pip` by running:

```bash
pip install h5pack
```

After installing `h5pack` you can verify the installation by running:

```bash
h5pack --version
```

This should output:

```bash
h5pack version x.y.z yyyy-zzzz developed by Esteban Gómez (Speech Interaction Technology, Aalto University)
```

Where:

- `x.y.z` represents the major, minor, and patch version.
- `yyyy-zzzz` indicates the development start year and the current year.

## Install using uv 

`uv` is a modern python package manager. You can see more details about `uv` in [the official documentation](https://docs.astral.sh/uv/).

First, you need to install `uv` and `uvx` following the instructions for your operating system in <a href="https://docs.astral.sh/uv/getting-started/installation/" target="_blank">`uv` website</a>.

Then run:

```bash
uv tool install h5pack
```

You can verify the installation running:

```bash
uv tool run h5pack --version
```

or you can use the shortcut version `uvx`:

```bash
uvx h5pack --version
```

This should output:

```bash
h5pack version x.y.z yyyy-zzzz developed by Esteban Gómez (Speech Interaction Technology, Aalto University)
```

Where:

- `x.y.z` represents the major, minor, and patch version.
- `yyyy-zzzz` indicates the development start year and the current year.

Now that you have installed `h5pack`, check the [Quickstart](quickstart.md) section to begin using it.

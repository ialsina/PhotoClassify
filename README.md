# PhotoClassify

A powerful Python tool for managing and organizing your photo collections.

## Features

- **Smart Photo Organization**: Automatically organize photos based on metadata
- **Duplicate Detection**: Find and manage duplicate photos with advanced comparison
- **Safe File Operations**: Secure file transfers with integrity verification
- **Parallel Processing**: Fast processing of large photo collections
- **Flexible Configuration**: Customize behavior through YAML config and CLI options

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/PhotoClassify.git
cd PhotoClassify

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

1. Copy photos from source to destination:
```bash
photoclassify copy /path/to/source /path/to/destination
```

2. Find duplicate photos:
```bash
photoclassify diff /path/to/source /path/to/destination
```

3. Generate photo collection statistics:
```bash
photoclassify hist /path/to/photos
```

## Documentation

For detailed documentation, including API reference and advanced usage, see our [documentation](docs/index.html).

## Contributing

We welcome contributions! Please see our [Contributing Guide](docs/contributing.rst) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

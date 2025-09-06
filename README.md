# AWS Textract Invoice Processor

A Python application that uses AWS Textract to extract and analyze data from invoice PDF files. The application automatically uploads invoices to S3, processes them using AWS Textract's AnalyzeExpense API, and provides a fallback text detection method for enhanced payment terms extraction.

## Prerequisites

- Python 3.10 or higher
- AWS Account with appropriate permissions
- AWS CLI configured with credentials
- Access to AWS Textract and S3 services

## Installation

### Installing uv (Recommended Package Manager)

This project uses [uv](https://docs.astral.sh/uv/) for fast and reliable Python package management.

#### On macOS and Linux:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### On Windows:
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Alternative installation methods:
```bash
# Using pip
pip install uv

# Using pipx
pipx install uv

```

### Project Setup

1. **Clone or download the project**:
   ```bash
   git clone https://github.com/TharangaSG/AWS-Textract-Invoice-Processor.git
   cd aws-textract
   ```

2. **Install dependencies using uv**:
   ```bash
   uv sync
   ```

3. **Configure AWS credentials** (if not already done):
   ```bash
   aws configure
   ```
   Or set environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

## Configuration

### S3 Bucket Configuration

The application uses a predefined S3 bucket. To use your own bucket, modify the `S3_BUCKET_NAME` variable in `main.py`:

```python
S3_BUCKET_NAME = "your-s3bucket-name"
```

## Usage

### Basic Usage

Process a single invoice:
```bash
uv run main.py invoices/1.pdf
```

Process multiple invoices:
```bash
uv run main.py invoices/1.pdf invoices/2.pdf
uv run main.py invoices/*
```

### Alternative Running Methods

If you prefer to activate the virtual environment:
```bash
# Activate the virtual environment
source .venv/bin/activate  # On Linux/macOS
# or
.venv\Scripts\activate     # On Windows

# Run the script
python main.py path/to/invoice.pdf
```






# Tests for orca-hls-utils

This directory contains tests for the HLS utilities.

## Running Tests

To run all tests:

```bash
python tests/test_hlsstream.py
```

## Test Structure

- `test_hlsstream.py` - Tests for the HLSStream class, including:
  - Initialization tests
  - `get_next_clip()` behavior tests
  - `is_stream_over()` tests

## CI Testing

Tests are automatically run via GitHub Actions on pushes and pull requests to the main/master branches.

The workflow file is located at `.github/workflows/test_hlsstream.yml`.

## Adding New Tests

See the suggestions at the end of `test_hlsstream.py` for additional test cases that could be implemented.

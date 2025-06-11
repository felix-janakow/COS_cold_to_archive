# This repository contains three Python scripts with different methods for moving files into archive mode within a COS bucket.

All methods operate by performing an in-place copy and updating the metadata (such as timestamps). This triggers the archiving process once the archiving rule in the COS bucket is activated. Important: the file itself within the bucket remains unchanged

### [Prerequisites to run the script](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/Instructions/Prerequisites.md)


### Option 1: archive.py

- Traverses the entire bucket
- Options to process only specific folders or subfolders
- **Use Case:** archive smaller buckets or single folders within buckets quickly
- [How to use](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/Instructions/archive.py-INSTRUCTION.md)


### Option 2: archive_fbf.py (fbf = folder by folder)

- First accesses the bucket and retrieves the names of all folders and subfolders
- Pastes the names into the program and processes each folder one by one
- **Use case:** archiving large buckets
- [How to use](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/Instructions/archive_fbf.py-INSTRUCTION.md)


### Option 3: archive_fbf_non_interactive.py

- Works like archive_fbf.py
- Adds a feature that allows the terminal to be closed to prevent timeout issues
- Use case: archiving large buckets where the script is intended to run over an extended period (e.g., over the weekend)
- [How to use](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/Instructions/archive_fbf_non_interactive.py-INSTRUCTION.md)
# This repository contains three Python scripts with different methods for moving files into an archive within a COS bucket.

All methods operate by performing an in-place copy and updating the metadata (such as timestamps). This triggers the archiving process once the archiving rule in the COS bucket is activated. Important: the file itself within the bucket remains unchanged

### [Prerequisites to run the script]()


### Option 1: archive.py

- Traverses the entire bucket
- Options to process only specific folders or subfolders
- [How to use](https://github.com/felix-janakow/COS_cold_to_archive/blob/main/Instructions/archive.py-INSTRUCTION.md)
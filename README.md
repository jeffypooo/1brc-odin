# 1 Billion Row Challenge - Odin
In order to lean Odin, I decided to implement a solution to the [1brc](https://github.com/gunnarmorling/1brc).

## Building and running

1. Generate a `measurements.txt` file by cloning the challenge repository and following the instructions.
2. Build `odin build . -o:speed`
3. Run: `./1brc-odin <path-to-measurements.txt> <thread-count>`. Thread count defaults to your number of logical CPUs if not provided.

The results are written to `results.txt`.

### Other Configuration

By default the build is configured to use a memory mapped file to read in data, but the program only has support for memory mapped files on MacOS and Linux. If running on Windows or something else, you can disable this using `odin build . -o:speed -define:USE_MMAP=false`.

## Performance

On an M4 Macbook Pro with 14 cores (10/4):
```
> ./1brc-odin ../1brc/measurements_1b.txt
1BR Challenge
USE_MMAP: true
Job Count: 14
File size 13795416316
Finished in 2.97698175s
```
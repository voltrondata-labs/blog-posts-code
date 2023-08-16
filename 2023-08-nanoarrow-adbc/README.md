
# nanoarrow/ADBC example

This repository contains a bare-bones ADBC driver implemented in C++ using
[nanoarrow](https://arrow.apache.org/nanoarrow) to generate the required
[Arrow C Data](https://arrow.apache.org/docs/format/CDataInterface.html) and
[Arrow C Stream](https://arrow.apache.org/docs/format/CStreamInterface.html)
structures. Note that this example is designed to illustrate the glue code
required to expose a data source as an ADBC driver rather than implement a
fully-featured driver or CSV reader.

## Building

This example uses [CMake](https://cmake.org/) to coordinate the build.
You can build the project using:

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

This will build a shared library called `libadbc_simple_csv_driver`. The
extension will vary based on platform (.dylib on MacOS, .so on Linux,
and .dll on Windows).

```
build
├── CMakeCache.txt
├── CMakeFiles
├── Makefile
├── cmake_install.cmake
└── libadbc_simple_csv_driver.dylib
```

## Usage

The ADBC driver manager (including its bindings in
[R](https://github.com/apache/arrow-adbc/tree/main/r/adbcdrivermanager) and [Python](https://arrow.apache.org/adbc/main/python/driver_manager.html))
can load a driver from a shared object like the one we just built. Note that
the extension of the shared library will change based on the platform
(see above).

In Python:

```python
# pip install adbc_driver_manager pyarrow
import adbc_driver_manager
import pyarrow as pa

db = adbc_driver_manager.AdbcDatabase(
    driver="build/libadbc_simple_csv_driver.dylib",
    entrypoint="SimpleCsvDriverInit"
)

conn = adbc_driver_manager.AdbcConnection(db)
stmt = adbc_driver_manager.AdbcStatement(conn)
stmt.set_sql_query("test.csv")
array_stream, rows_affected = stmt.execute_query()
reader = pa.RecordBatchReader._import_from_c(array_stream.address)
reader.read_all()
#> pyarrow.Table
#> col1: string
#> col2: string
#> col3: string
#> ----
#> col1: [["val1"]]
#> col2: [["val2"]]
#> col3: [["val3"]]
```

In R:

```r
# install.packages("adbcdrivermanager")
library(adbcdrivermanager)

simple_csv_drv <- adbc_driver(
  "build/libadbc_simple_csv_driver.dylib",
  "SimpleCsvDriverInit"
)

adbc_database_init(simple_csv_drv) |>
  read_adbc("test.csv") |>
  as.data.frame()
#>   col1 col2 col3
#> 1 val1 val2 val3
```

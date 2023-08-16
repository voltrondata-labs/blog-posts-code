
curl -L https://github.com/apache/arrow-adbc/raw/main/adbc.h -o adbc.h

for f in nanoarrow.h nanoarrow.hpp nanoarrow.c; do
  curl -L \
    https://raw.githubusercontent.com/apache/arrow-nanoarrow/apache-arrow-nanoarrow-0.2.0/dist/$f \
    -o $f
done

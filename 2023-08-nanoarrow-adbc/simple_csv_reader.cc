
#include <cerrno>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "nanoarrow.hpp"
#include "simple_csv_reader.h"

enum class ScanResult { UNINITIALIZED, FIELD_SEP, LINE_SEP, DONE };

class SimpleCsvScanner {
 public:
  SimpleCsvScanner(const std::string& filename) : input_(filename, std::ios::binary) {}

  std::pair<ScanResult, std::string> ReadField() {
    std::stringstream stream;

    while (true) {
      int item = input_.get();
      switch (item) {
        case ',':
          return {ScanResult::FIELD_SEP, stream.str()};
        case '\n':
          return {ScanResult::LINE_SEP, stream.str()};
        case EOF:
          return {ScanResult::DONE, stream.str()};
        default:
          stream << static_cast<char>(item);
          break;
      }
    }
  }

  ScanResult ReadLine(std::vector<std::string>* values) {
    std::pair<ScanResult, std::string> result;

    do {
      result = ReadField();
      values->push_back(result.second);
    } while (result.first == ScanResult::FIELD_SEP);

    return result.first;
  }

 private:
  std::ifstream input_;
};

class SimpleCsvArrayBuilder {
 public:
  SimpleCsvArrayBuilder(const std::string& filename) : scanner_(filename) {
    ArrowErrorSet(&last_error_, "Internal error");
  }

  int GetSchema(ArrowSchema* out) {
    NANOARROW_RETURN_NOT_OK(ReadSchemaIfNeeded());
    NANOARROW_RETURN_NOT_OK(ArrowSchemaDeepCopy(schema_.get(), out));
    return NANOARROW_OK;
  }

  int GetArray(ArrowArray* out) {
    if (status_ == ScanResult::DONE) {
      out->release = nullptr;
      return NANOARROW_OK;
    }

    NANOARROW_RETURN_NOT_OK(ReadSchemaIfNeeded());
    NANOARROW_RETURN_NOT_OK(InitArrayIfNeeded());

    while (status_ != ScanResult::DONE) {
      NANOARROW_RETURN_NOT_OK(ReadLine());
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(array_.get(), &last_error_));
    ArrowArrayMove(array_.get(), out);
    return NANOARROW_OK;
  }

  const char* GetLastError() { return last_error_.message; }

 private:
  ScanResult status_;
  SimpleCsvScanner scanner_;
  std::vector<std::string> fields_;
  ArrowError last_error_;
  nanoarrow::UniqueSchema schema_;
  nanoarrow::UniqueArray array_;

  int ReadSchemaIfNeeded() {
    if (schema_->release != nullptr) {
      return NANOARROW_OK;
    }

    fields_.clear();
    status_ = scanner_.ReadLine(&fields_);

    ArrowSchemaInit(schema_.get());
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema_.get(), fields_.size()));
    for (int64_t i = 0; i < schema_->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(
          ArrowSchemaSetType(schema_->children[i], NANOARROW_TYPE_STRING));
      NANOARROW_RETURN_NOT_OK(
          ArrowSchemaSetName(schema_->children[i], fields_[i].c_str()));
    }

    return NANOARROW_OK;
  }

  int InitArrayIfNeeded() {
    if (array_->release != nullptr) {
      return NANOARROW_OK;
    }

    NANOARROW_RETURN_NOT_OK(
        ArrowArrayInitFromSchema(array_.get(), schema_.get(), &last_error_));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array_.get()));
    return NANOARROW_OK;
  }

  int ReadLine() {
    fields_.clear();
    status_ = scanner_.ReadLine(&fields_);

    // Skip blank line
    if (fields_.size() == 1 && fields_[0] == "") {
      return NANOARROW_OK;
    }

    if (schema_->n_children != fields_.size()) {
      ArrowErrorSet(&last_error_, "Expected %ld fields but found %ld fields",
                    (long)schema_->n_children, (long)fields_.size());
      return EINVAL;
    }

    ArrowStringView view;
    for (int64_t i = 0; i < schema_->n_children; i++) {
      view.data = fields_[i].data();
      view.size_bytes = fields_[i].size();
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(array_->children[i], view));
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array_.get()));
    return NANOARROW_OK;
  }
};

static int SimpleCsvArrayStreamGetSchema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto private_data = reinterpret_cast<SimpleCsvArrayBuilder*>(stream->private_data);
  return private_data->GetSchema(out);
}

static int SimpleCsvArrayStreamGetNext(ArrowArrayStream* stream, ArrowArray* out) {
  auto private_data = reinterpret_cast<SimpleCsvArrayBuilder*>(stream->private_data);
  return private_data->GetArray(out);
}

static const char* SimpleCsvArrayStreamGetLastError(ArrowArrayStream* stream) {
  auto private_data = reinterpret_cast<SimpleCsvArrayBuilder*>(stream->private_data);
  return private_data->GetLastError();
}

static void SimpleCsvArrayStreamRelease(ArrowArrayStream* stream) {
  auto private_data = reinterpret_cast<SimpleCsvArrayBuilder*>(stream->private_data);
  delete private_data;
  stream->release = nullptr;
}

void InitSimpleCsvArrayStream(const char* filename, ArrowArrayStream* out) {
  out->get_schema = &SimpleCsvArrayStreamGetSchema;
  out->get_next = &SimpleCsvArrayStreamGetNext;
  out->get_last_error = &SimpleCsvArrayStreamGetLastError;
  out->release = &SimpleCsvArrayStreamRelease;
  out->private_data = new SimpleCsvArrayBuilder(filename);
}

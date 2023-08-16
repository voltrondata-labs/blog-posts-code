
#include <cstdlib>
#include <cstring>
#include <string>

#include "adbc.h"
#include "simple_csv_reader.h"

// A little bit of hack, but we really do need placeholders for the private
// data for driver/database/connection/statement even though we don't use them.
// A real driver *would* use them, but also, the way to mark AdbcDriver and
// friends as released is to set the private_data to nullptr. Therefore, we need
// something that is *not* null to put there at the very least.
struct SimpleCsvDriverPrivate {
  int not_empty;
};

struct SimpleCsvDatabasePrivate {
  int not_empty;
};

struct SimpleCsvConnectionPrivate {
  int not_empty;
};

struct SimpleCsvStatementPrivate {
  std::string filename;
};

static AdbcStatusCode SimpleCsvDriverRelease(struct AdbcDriver* driver,
                                             struct AdbcError* error) {
  if (driver->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto driver_private = reinterpret_cast<SimpleCsvDriverPrivate*>(driver->private_data);
  delete driver_private;
  driver->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseNew(struct AdbcDatabase* database,
                                           struct AdbcError* error) {
  database->private_data = new SimpleCsvDatabasePrivate();
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseSetOption(struct AdbcDatabase* database,
                                                 const char* key, const char* value,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_INVALID_ARGUMENT;
}

static AdbcStatusCode SimpleCsvDatabaseInit(struct AdbcDatabase* database,
                                            struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvDatabaseRelease(struct AdbcDatabase* database,
                                               struct AdbcError* error) {
  if (database->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto database_private =
      reinterpret_cast<SimpleCsvDatabasePrivate*>(database->private_data);
  delete database_private;
  database->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionNew(struct AdbcConnection* connection,
                                             struct AdbcError* error) {
  connection->private_data = new SimpleCsvConnectionPrivate();
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionInit(struct AdbcConnection* connection,
                                              struct AdbcDatabase* database,
                                              struct AdbcError* error) {
  auto connection_private =
      reinterpret_cast<SimpleCsvConnectionPrivate*>(connection->private_data);
  auto database_private =
      reinterpret_cast<SimpleCsvDatabasePrivate*>(database->private_data);
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvConnectionRelease(struct AdbcConnection* connection,
                                                 struct AdbcError* error) {
  if (connection->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto connection_private =
      reinterpret_cast<SimpleCsvConnectionPrivate*>(connection->private_data);
  delete connection_private;
  connection->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementNew(struct AdbcConnection* connection,
                                            struct AdbcStatement* statement,
                                            struct AdbcError* error) {
  auto statement_private = new SimpleCsvStatementPrivate();
  auto connection_private =
      reinterpret_cast<SimpleCsvConnectionPrivate*>(connection->private_data);
  statement->private_data = statement_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementRelease(struct AdbcStatement* statement,
                                                struct AdbcError* error) {
  if (statement->private_data == nullptr) {
    return ADBC_STATUS_OK;
  }

  auto statement_private =
      reinterpret_cast<SimpleCsvStatementPrivate*>(statement->private_data);
  delete statement_private;
  statement->private_data = nullptr;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementSetSqlQuery(struct AdbcStatement* statement,
                                                    const char* query,
                                                    struct AdbcError* error) {
  auto statement_private =
      reinterpret_cast<SimpleCsvStatementPrivate*>(statement->private_data);
  statement_private->filename = query;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode SimpleCsvStatementExecuteQuery(struct AdbcStatement* statement,
                                                     struct ArrowArrayStream* out,
                                                     int64_t* rows_affected,
                                                     struct AdbcError* error) {
  auto statement_private =
      reinterpret_cast<SimpleCsvStatementPrivate*>(statement->private_data);
  InitSimpleCsvArrayStream(statement_private->filename.c_str(), out);
  *rows_affected = -1;
  return ADBC_STATUS_OK;
}

extern "C" AdbcStatusCode SimpleCsvDriverInit(int version, void* raw_driver,
                                              struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriver));
  driver->private_data = new SimpleCsvDriverPrivate();

  driver->DatabaseNew = SimpleCsvDatabaseNew;
  driver->DatabaseSetOption = SimpleCsvDatabaseSetOption;
  driver->DatabaseInit = &SimpleCsvDatabaseInit;
  driver->DatabaseRelease = SimpleCsvDatabaseRelease;

  driver->ConnectionNew = SimpleCsvConnectionNew;
  driver->ConnectionInit = SimpleCsvConnectionInit;
  driver->ConnectionRelease = SimpleCsvConnectionRelease;

  driver->StatementNew = SimpleCsvStatementNew;
  driver->StatementSetSqlQuery = SimpleCsvStatementSetSqlQuery;
  driver->StatementExecuteQuery = SimpleCsvStatementExecuteQuery;
  driver->StatementRelease = SimpleCsvStatementRelease;

  driver->release = SimpleCsvDriverRelease;

  return ADBC_STATUS_OK;
}

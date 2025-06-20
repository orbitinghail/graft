#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <sqlite3.h>

/// Initializes the statically linked graft extension.
/// Must be called with a valid `sqlite3*` database handle.
/// Returns 0 on success, non-zero on error.
int graft_static_init(sqlite3 *db);

#ifdef __cplusplus
}
#endif

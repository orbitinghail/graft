#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/// Initializes the statically linked graft extension.
/// Must be called with a valid `sqlite3*` database handle.
/// Returns 0 on success, non-zero on error.
int graft_static_init(void *db);

#ifdef __cplusplus
}
#endif

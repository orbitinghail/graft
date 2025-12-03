#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/// Initializes the statically linked graft extension.
/// Registers graft directly with statically linked SQLite3 symbols.
/// Returns 0 on success, non-zero on error.
int graft_static_init();

#ifdef __cplusplus
}
#endif

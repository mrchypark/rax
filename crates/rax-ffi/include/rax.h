#ifndef RAX_FFI_H
#define RAX_FFI_H

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RAX_STATUS_OK 0
#define RAX_STATUS_ERROR 1
#define RAX_STATUS_INVALID_ARGUMENT 2
#define RAX_STATUS_PANIC 3

/*
 * ABI contract:
 * - All input strings, including paths, must be non-null UTF-8 unless the
 *   function documents them as optional.
 * - Functions that accept char **out_json set it to NULL before doing work.
 * - On RAX_STATUS_OK, out_json points to an owned NUL-terminated JSON string.
 *   Release that string with rax_string_free().
 * - Do not free the pointers returned by rax_version() or rax_last_error().
 * - rax_last_error() is thread-local and remains valid only until the next
 *   rax FFI call on the same thread.
 */

const char *rax_version(void);
int rax_create(const char *store);
int rax_ingest_docs(const char *store, const char *input, char **out_json);
int rax_ingest_vectors(const char *store, const char *input, char **out_json);
int rax_remember(const char *store, const char *text, char **out_json);
int rax_recall(
    const char *store,
    const char *query,
    int top_k,
    bool preview,
    char **out_json);
int rax_search(
    const char *store,
    const char *mode,
    const char *text,
    const char *vector_input,
    int top_k,
    bool preview,
    char **out_json);
int rax_search_doc_ids(
    const char *store,
    const char *mode,
    const char *text,
    const char *vector_input,
    int top_k,
    char **out_json);
/*
 * Persistent read handles search the snapshot opened by rax_open_read_only().
 * Reopen the handle after replacing or rebuilding the store.
 */
int rax_open_read_only(const char *store, void **out_handle);
int rax_handle_search_doc_ids(
    void *handle,
    const char *mode,
    const char *text,
    const char *vector_input,
    int top_k,
    char **out_json);
int rax_handle_search_doc_ids_profiled(
    void *handle,
    const char *mode,
    const char *text,
    const char *vector_input,
    int top_k,
    char **out_json);
void rax_handle_close(void *handle);
void rax_string_free(char *value);
const char *rax_last_error(void);

#ifdef __cplusplus
}
#endif

#endif

// Package lib holds the service's long-running runtime machinery — the parts that are neither a
// request handler nor a data-access object. Today that is the reaper: the background loop that
// recovers jobs a dead worker stranded.
package lib

#ifndef PERF_CONFIG_H
#define PERF_CONFIG_H

// Profiling helpers
#define PROFILE_SURFACES 0
#define PROFILE_TEXTURES 0
#define PROFILE_FLIP 0

// Try to avoid re-uploading textures again and again by keeping them in GPU
// memory and identifying them using a fast hashing method.
#define USE_TEXTURE_CACHE 1
#define USE_TEXTURE_LOCATION_CACHE 1 // FIXME: Need to debug issue with double free
#define TRACK_LOCATION_CACHE_STATS 0

// Similar to the texture cache, try to avoid uploading geometry when possible
#define USE_GEOMETRY_CACHE 1
#define TRACK_GEOMETRY_CACHE_STATS 1

// Experimental stuff to get rid of the many (many!) uniform setting
#define USE_UBO 0
#define USE_UBO_CACHE 0

// Instead of using two threads for puller and pusher, use one thread and
// co-routines to switch between the tasks when blocked.
#define USE_COROUTINES 1

// Enable 4x surface rendering
#define RES_SCALE_4X 1

// Instead of writing surfaces out to memory...
// - Hold on to them in a cache as they are likely to be re-used (don't re-upload)
// - Use the cached surface when the frame should be swapped
#define USE_SHARED_CONTEXT 1

// Support loading texture data from a cached surface
#define RENDER_TO_TEXTURE 1

// Use glCopyImageSubData to copy surfaces to textures in upside-down.
//
// If not set, a different (likely much faster but need to profile) approach
// is used where a new framebuffer is bound with the destination texture as
// the color attachment and the surface is rendered into the framebuffer with
// a shader that flips it upside down.
#define RENDER_TO_TEXTURE_COPY 0

// Bypass parts of the FIFO state machine to faster bulk uploads
// Massive performance improvement in some cases.
#define FIFO_SHORTCUT 1

#endif

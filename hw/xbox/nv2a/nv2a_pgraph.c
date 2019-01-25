/*
 * QEMU Geforce NV2A implementation
 *
 * Copyright (c) 2012 espes
 * Copyright (c) 2015 Jannik Vogel
 * Copyright (c) 2018 Matt Borgerson
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include "xxhash.h"

#if PROFILE_METHODS
#include <signal.h>
int method_track[0x2000];
void track_pgraph_method(unsigned int subchannel,
                   unsigned int method,
                   uint32_t parameter)
{
    assert(method < 0x2000);
    method_track[method]++;
}

void dump_stats(int signum) {
    if (signum != SIGUSR2) return;

    printf("DUMPING STATS\n");

    FILE *fd = fopen("stats.txt", "w");
    for (int i = 0; i < 0x2000; i++) {
        if (method_track[i] > 0) {
            fprintf(fd, "%04x: %d\n", i, method_track[i]);
        }
    }
    fclose(fd);
}
#endif

static void pgraph_render_surface_to_texture(
    NV2AState *d, GLsync fence,
    GLuint src, GLenum src_format, GLenum src_target,
    GLuint dst, GLenum dst_format, GLenum dst_target,
    int width, int height, int src_zeta, int flip
    );

#if PROFILE_SURFACES
#define SDPRINTF printf
#else
#define SDPRINTF(...)
#endif

#if PROFILE_TEXTURES
#define TDPRINTF printf
#else
#define TDPRINTF(...)
#endif


#if PROFILE_TIME

struct timeval tv_start;
int tv_start_valid = 0;

static void print_timestamp(void);
static void start_frame_timer(void);
static void stop_frame_timer(void);

static void print_timestamp(void)
{
    struct timeval tv_now, tv_since_start;

    gettimeofday(&tv_now, NULL);

    if (!tv_start_valid) {
        tv_start = tv_now;
        tv_start_valid = 1;
    }

    timersub(&tv_now, &tv_start, &tv_since_start);

    printf("[%4ld.%06ld] ", tv_since_start.tv_sec, tv_since_start.tv_usec);

}

struct timeval frame_timer_start;
int frame_timer_started = 0;

static void start_frame_timer(void)
{
    if (frame_timer_started) return;
    gettimeofday(&frame_timer_start, NULL);
    frame_timer_started = 1;
}

static void stop_frame_timer(void)
{
    static int i = 0;

    if (!frame_timer_started) return;
    struct timeval tv_now, tv_since_start;
    gettimeofday(&tv_now, NULL);
    timersub(&tv_now, &frame_timer_start, &tv_since_start);

    double s_per_frame = tv_since_start.tv_usec/1000000.0 + tv_since_start.tv_sec;
    double ms_per_frame = s_per_frame * 1000;

    if (i++ > 10) {
        printf("--- [ms %4f, rfps = %4f]\n", (float)ms_per_frame, s_per_frame > 0.0 ? 1.0/(float)s_per_frame : 0.0);
        i = 0;
    }
    frame_timer_started = 0;
}

struct timeval timer_start, timer_stop;

static void time_this(int start);

static void time_this(int start)
{
    struct timeval tv_now, tv_since_start;

    gettimeofday(&tv_now, NULL);

    if (start) {
        timer_start = tv_now;
    } else {
        timersub(&tv_now, &timer_start, &tv_since_start);
        printf("[%4ld.%06ld]\n", tv_since_start.tv_sec, tv_since_start.tv_usec);
    }
}

#else
#define start_frame_timer() do {} while (0)
#define stop_frame_timer() do {} while (0)
#endif

volatile int available = 0;
volatile GLuint fb_tex = 0;
volatile GLsync fb_sync = 0;

extern QemuSpin avail_spinner;

extern volatile int fifo_access_cond;
volatile int flip_3d;


struct lru_node *tce_init(struct lru_node *obj, void *key);
struct lru_node *tce_deinit(struct lru_node *obj);
int tce_key_compare(struct lru_node *obj, void *key);


struct lru_node *gce_init(struct lru_node *obj, void *key);
struct lru_node *gce_deinit(struct lru_node *obj);
int gce_key_compare(struct lru_node *obj, void *key);

struct lru_node *uboce_init(struct lru_node *obj, void *key);
struct lru_node *uboce_deinit(struct lru_node *obj);
int uboce_key_compare(struct lru_node *obj, void *key);

/*
 *
 * A very dumb surface cache. Surfaces are identified by their
 * offset and shape.
 *
 */

int surface_cache_find(hwaddr addr, int color);
int surface_cache_retire(int index);
int surface_cache_store(hwaddr addr);

#define SURFACE_CACHE_SLOTS 128

struct surface_cache_slot {
    int valid;
    hwaddr addr;
    SurfaceShape shape;
    GLuint buf_id;
    GLsync fence;
    int color;
} surface_cache[SURFACE_CACHE_SLOTS];

int surface_cache_find(hwaddr addr, int color)
{
    int i;
    for (i = 0; i < SURFACE_CACHE_SLOTS; i++) {
        if (surface_cache[i].valid &&
            surface_cache[i].addr == addr &&
            surface_cache[i].color == color) {
            return i;
        }
    }

    return -1;
}

int surface_cache_retire(int index)
{
    surface_cache[index].valid = 0;
    return 0;
}

int surface_cache_store(hwaddr addr)
{
    int i = surface_cache_find(addr, 1);

    if (i < 0) {
        // Find a free slot
        int j;
        for (j = 0; j < SURFACE_CACHE_SLOTS; j++) {
            if (!surface_cache[j].valid) {
                i = j;
                break;
            }
        }
    }

    assert(i >= 0);

    surface_cache[i].addr = addr;
    surface_cache[i].fence = 0;
    surface_cache[i].valid = 1;

    return i;
}




static const GLenum pgraph_texture_min_filter_map[] = {
    0,
    GL_NEAREST,
    GL_LINEAR,
    GL_NEAREST_MIPMAP_NEAREST,
    GL_LINEAR_MIPMAP_NEAREST,
    GL_NEAREST_MIPMAP_LINEAR,
    GL_LINEAR_MIPMAP_LINEAR,
    GL_LINEAR, /* TODO: Convolution filter... */
};

static const GLenum pgraph_texture_mag_filter_map[] = {
    0,
    GL_NEAREST,
    GL_LINEAR,
    0,
    GL_LINEAR /* TODO: Convolution filter... */
};

static const GLenum pgraph_texture_addr_map[] = {
    0,
    GL_REPEAT,
    GL_MIRRORED_REPEAT,
    GL_CLAMP_TO_EDGE,
    GL_CLAMP_TO_BORDER,
    GL_CLAMP
};

static const GLenum pgraph_blend_factor_map[] = {
    GL_ZERO,
    GL_ONE,
    GL_SRC_COLOR,
    GL_ONE_MINUS_SRC_COLOR,
    GL_SRC_ALPHA,
    GL_ONE_MINUS_SRC_ALPHA,
    GL_DST_ALPHA,
    GL_ONE_MINUS_DST_ALPHA,
    GL_DST_COLOR,
    GL_ONE_MINUS_DST_COLOR,
    GL_SRC_ALPHA_SATURATE,
    0,
    GL_CONSTANT_COLOR,
    GL_ONE_MINUS_CONSTANT_COLOR,
    GL_CONSTANT_ALPHA,
    GL_ONE_MINUS_CONSTANT_ALPHA,
};

static const GLenum pgraph_blend_equation_map[] = {
    GL_FUNC_SUBTRACT,
    GL_FUNC_REVERSE_SUBTRACT,
    GL_FUNC_ADD,
    GL_MIN,
    GL_MAX,
    GL_FUNC_REVERSE_SUBTRACT,
    GL_FUNC_ADD,
};

static const GLenum pgraph_blend_logicop_map[] = {
    GL_CLEAR,
    GL_AND,
    GL_AND_REVERSE,
    GL_COPY,
    GL_AND_INVERTED,
    GL_NOOP,
    GL_XOR,
    GL_OR,
    GL_NOR,
    GL_EQUIV,
    GL_INVERT,
    GL_OR_REVERSE,
    GL_COPY_INVERTED,
    GL_OR_INVERTED,
    GL_NAND,
    GL_SET,
};

static const GLenum pgraph_cull_face_map[] = {
    0,
    GL_FRONT,
    GL_BACK,
    GL_FRONT_AND_BACK
};

static const GLenum pgraph_depth_func_map[] = {
    GL_NEVER,
    GL_LESS,
    GL_EQUAL,
    GL_LEQUAL,
    GL_GREATER,
    GL_NOTEQUAL,
    GL_GEQUAL,
    GL_ALWAYS,
};

static const GLenum pgraph_stencil_func_map[] = {
    GL_NEVER,
    GL_LESS,
    GL_EQUAL,
    GL_LEQUAL,
    GL_GREATER,
    GL_NOTEQUAL,
    GL_GEQUAL,
    GL_ALWAYS,
};

static const GLenum pgraph_stencil_op_map[] = {
    0,
    GL_KEEP,
    GL_ZERO,
    GL_REPLACE,
    GL_INCR,
    GL_DECR,
    GL_INVERT,
    GL_INCR_WRAP,
    GL_DECR_WRAP,
};

typedef struct ColorFormatInfo {
    unsigned int bytes_per_pixel;
    bool linear;
    GLint gl_internal_format;
    GLenum gl_format;
    GLenum gl_type;
    GLenum gl_swizzle_mask[4];
} ColorFormatInfo;

static const ColorFormatInfo kelvin_color_format_map[66] = {
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_Y8] =
        {1, false, GL_R8, GL_RED, GL_UNSIGNED_BYTE,
         {GL_RED, GL_RED, GL_RED, GL_ONE}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_AY8] =
        {1, false, GL_R8, GL_RED, GL_UNSIGNED_BYTE,
         {GL_RED, GL_RED, GL_RED, GL_RED}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A1R5G5B5] =
        {2, false, GL_RGB5_A1, GL_BGRA, GL_UNSIGNED_SHORT_1_5_5_5_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_X1R5G5B5] =
        {2, false, GL_RGB5, GL_BGRA, GL_UNSIGNED_SHORT_1_5_5_5_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A4R4G4B4] =
        {2, false, GL_RGBA4, GL_BGRA, GL_UNSIGNED_SHORT_4_4_4_4_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_R5G6B5] =
        {2, false, GL_RGB565, GL_RGB, GL_UNSIGNED_SHORT_5_6_5},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A8R8G8B8] =
        {4, false, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_X8R8G8B8] =
        {4, false, GL_RGB8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},

    /* paletted texture */
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_I8_A8R8G8B8] =
        {1, false, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},

    [NV097_SET_TEXTURE_FORMAT_COLOR_L_DXT1_A1R5G5B5] =
        {4, false, GL_COMPRESSED_RGBA_S3TC_DXT1_EXT, 0, GL_RGBA},
    [NV097_SET_TEXTURE_FORMAT_COLOR_L_DXT23_A8R8G8B8] =
        {4, false, GL_COMPRESSED_RGBA_S3TC_DXT3_EXT, 0, GL_RGBA},
    [NV097_SET_TEXTURE_FORMAT_COLOR_L_DXT45_A8R8G8B8] =
        {4, false, GL_COMPRESSED_RGBA_S3TC_DXT5_EXT, 0, GL_RGBA},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A1R5G5B5] =
        {2, true, GL_RGB5_A1, GL_BGRA, GL_UNSIGNED_SHORT_1_5_5_5_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_R5G6B5] =
        {2, true, GL_RGB565, GL_RGB, GL_UNSIGNED_SHORT_5_6_5},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A8R8G8B8] =
        {4, true, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_Y8] =
        {1, true, GL_R8, GL_RED, GL_UNSIGNED_BYTE,
         {GL_RED, GL_RED, GL_RED, GL_ONE}},

    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A8] =
        {1, false, GL_R8, GL_RED, GL_UNSIGNED_BYTE,
         {GL_ONE, GL_ONE, GL_ONE, GL_RED}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A8Y8] = // this one
        {2, false, GL_RG8, GL_RG, GL_UNSIGNED_BYTE,
         {GL_GREEN, GL_GREEN, GL_RED, GL_RED}}, // Blue as alpha
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_AY8] =
        {1, true, GL_R8, GL_RED, GL_UNSIGNED_BYTE,
         {GL_RED, GL_RED, GL_RED, GL_RED}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_X1R5G5B5] =
        {2, true, GL_RGB5, GL_BGRA, GL_UNSIGNED_SHORT_1_5_5_5_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A4R4G4B4] =
        {2, false, GL_RGBA4, GL_BGRA, GL_UNSIGNED_SHORT_4_4_4_4_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_X8R8G8B8] =
        {4, true, GL_RGB8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A8] =
        {1, true, GL_R8, GL_RED, GL_UNSIGNED_BYTE,
         {GL_ONE, GL_ONE, GL_ONE, GL_RED}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A8Y8] =
        {2, true, GL_RG8, GL_RG, GL_UNSIGNED_BYTE,
         {GL_GREEN, GL_GREEN, GL_GREEN, GL_RED}},

    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_R6G5B5] =
        {2, false, GL_RGB8_SNORM, GL_RGB, GL_BYTE}, /* FIXME: This might be signed */
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_G8B8] =
        {2, false, GL_RG8_SNORM, GL_RG, GL_BYTE, /* FIXME: This might be signed */
         {GL_ZERO, GL_RED, GL_GREEN, GL_ONE}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_R8B8] =
        {2, false, GL_RG8_SNORM, GL_RG, GL_BYTE, /* FIXME: This might be signed */
         {GL_RED, GL_ZERO, GL_GREEN, GL_ONE}},


    /* TODO: format conversion */
    [NV097_SET_TEXTURE_FORMAT_COLOR_LC_IMAGE_CR8YB8CB8YA8] =
        {2, true, GL_RGBA8,  GL_RGBA, GL_UNSIGNED_INT_8_8_8_8_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_DEPTH_X8_Y24_FIXED] =
        {4, true, GL_DEPTH24_STENCIL8, GL_DEPTH_STENCIL, GL_UNSIGNED_INT_24_8},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_DEPTH_Y16_FIXED] =
        {2, true, GL_DEPTH_COMPONENT16, GL_DEPTH_COMPONENT, GL_UNSIGNED_SHORT},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_Y16] =
        {2, true, GL_R16, GL_RED, GL_UNSIGNED_SHORT,
         {GL_RED, GL_RED, GL_RED, GL_ONE}},
    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A8B8G8R8] =
        {4, false, GL_RGBA8, GL_RGBA, GL_UNSIGNED_INT_8_8_8_8_REV},

    [NV097_SET_TEXTURE_FORMAT_COLOR_SZ_R8G8B8A8] =
        {4, false, GL_RGBA8, GL_RGBA, GL_UNSIGNED_INT_8_8_8_8},

    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A8B8G8R8] =
        {4, true, GL_RGBA8, GL_RGBA, GL_UNSIGNED_INT_8_8_8_8_REV},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_B8G8R8A8] =
        {4, true, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8},
    [NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_R8G8B8A8] =
        {4, true, GL_RGBA8, GL_RGBA, GL_UNSIGNED_INT_8_8_8_8}
};

typedef struct SurfaceColorFormatInfo {
    unsigned int bytes_per_pixel;
    GLint gl_internal_format;
    GLenum gl_format;
    GLenum gl_type;
} SurfaceColorFormatInfo;

static const SurfaceColorFormatInfo kelvin_surface_color_format_map[] = {
    [NV097_SET_SURFACE_FORMAT_COLOR_LE_X1R5G5B5_Z1R5G5B5] =
        {2, GL_RGB5_A1, GL_BGRA, GL_UNSIGNED_SHORT_1_5_5_5_REV},
    [NV097_SET_SURFACE_FORMAT_COLOR_LE_R5G6B5] =
        {2, GL_RGB565, GL_RGB, GL_UNSIGNED_SHORT_5_6_5},
    [NV097_SET_SURFACE_FORMAT_COLOR_LE_X8R8G8B8_Z8R8G8B8] =
        {4, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},
    [NV097_SET_SURFACE_FORMAT_COLOR_LE_A8R8G8B8] =
        {4, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},

    [NV097_SET_SURFACE_FORMAT_COLOR_LE_X1R5G5B5_O1R5G5B5] =
        {0},

    [NV097_SET_SURFACE_FORMAT_COLOR_LE_X8R8G8B8_O8R8G8B8] =
        {0},

    [NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_Z1A7R8G8B8] =
        {0},

    [NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_O1A7R8G8B8] =
        {0},

    [NV097_SET_SURFACE_FORMAT_COLOR_LE_B8] =
        {4, GL_RGBA8, GL_BGRA, GL_UNSIGNED_INT_8_8_8_8_REV},

    [NV097_SET_SURFACE_FORMAT_COLOR_LE_G8B8] =
        {0},


};

static bool check_surface_to_texture_compatibility(int surface_fmt, int texture_fmt)
{
    switch (surface_fmt) {
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1R5G5B5_Z1R5G5B5: goto err;
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1R5G5B5_O1R5G5B5: goto err;
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_R5G6B5: switch (texture_fmt) {
        case NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_R5G6B5: return true;
        case NV097_SET_TEXTURE_FORMAT_COLOR_SZ_R5G6B5: return true;
        default: goto err;
        }
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_X8R8G8B8_Z8R8G8B8: switch(texture_fmt) {
        case NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_X8R8G8B8: return true;
        case NV097_SET_TEXTURE_FORMAT_COLOR_SZ_X8R8G8B8: return true;
        default: goto err;
    }
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_X8R8G8B8_O8R8G8B8: goto err;
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_Z1A7R8G8B8: goto err;
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_O1A7R8G8B8: goto err;
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_A8R8G8B8: switch (texture_fmt) {
        case NV097_SET_TEXTURE_FORMAT_COLOR_LU_IMAGE_A8R8G8B8: return true;
        case NV097_SET_TEXTURE_FORMAT_COLOR_SZ_A8R8G8B8: return true;
        default: goto err;
    }
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_B8: goto err;
    case NV097_SET_SURFACE_FORMAT_COLOR_LE_G8B8: goto err;
    default: goto err;
    }

err:
    SDPRINTF("surface to texture compat failed: %d to %d\n", surface_fmt, texture_fmt);
    return false;
}

// static void pgraph_set_context_user(NV2AState *d, uint32_t val);
static void pgraph_method_log(unsigned int subchannel, unsigned int graphics_class, unsigned int method, uint32_t parameter);
static void pgraph_allocate_inline_buffer_vertices(PGRAPHState *pg, unsigned int attr);
static void pgraph_finish_inline_buffer_vertex(PGRAPHState *pg);
static void pgraph_vert_shader_update_constants(PGRAPHState *pg, VertexShaderBinding *binding, bool binding_changed, bool vertex_program, bool fixed_function);
static void pgraph_frag_shader_update_constants(PGRAPHState *pg, FragmentShaderBinding *binding, bool binding_changed);
static void pgraph_bind_shaders(PGRAPHState *pg);
static bool pgraph_framebuffer_dirty(PGRAPHState *pg);
static bool pgraph_color_write_enabled(PGRAPHState *pg);
static bool pgraph_zeta_write_enabled(PGRAPHState *pg);
static void pgraph_set_surface_dirty(PGRAPHState *pg, bool color, bool zeta);
static void pgraph_update_surface_part(NV2AState *d, bool upload, bool color);
static void pgraph_update_surface(NV2AState *d, bool upload, bool color_write, bool zeta_write);
static void pgraph_bind_textures(NV2AState *d);
static void pgraph_apply_anti_aliasing_factor(PGRAPHState *pg, unsigned int *width, unsigned int *height);
static void pgraph_get_surface_dimensions(PGRAPHState *pg, unsigned int *width, unsigned int *height);
static void pgraph_update_memory_buffer(NV2AState *d, hwaddr addr, hwaddr size, bool f);
static void pgraph_bind_vertex_attributes(NV2AState *d, unsigned int num_elements, bool inline_data, unsigned int inline_stride);
static unsigned int pgraph_bind_inline_array(NV2AState *d);
static float convert_f16_to_float(uint16_t f16);
static float convert_f24_to_float(uint32_t f24);
static uint8_t cliptobyte(int x);
static void convert_yuy2_to_rgb(const uint8_t *line, unsigned int ix, uint8_t *r, uint8_t *g, uint8_t* b);
static uint8_t* convert_texture_data(const TextureShape s, const uint8_t *data, const uint8_t *palette_data, unsigned int width, unsigned int height, unsigned int depth, unsigned int row_pitch, unsigned int slice_pitch);
static void upload_gl_texture(GLenum gl_target, const TextureShape s, const uint8_t *texture_data, const uint8_t *palette_data);
static TextureBinding* generate_texture(const TextureShape s, const uint8_t *texture_data, const uint8_t *palette_data);
static void texture_binding_destroy(gpointer data);

#if USE_TEXTURE_LOCATION_CACHE
static struct lru_node *texture_location_cache_entry_init(struct lru_node *obj, void *key);
static struct lru_node *texture_location_cache_entry_deinit(struct lru_node *obj);
static int texture_location_cache_entry_compare(struct lru_node *obj, void *key);
#endif

static struct lru_node *texture_cache_entry_init(struct lru_node *obj, void *key);
static struct lru_node *texture_cache_entry_deinit(struct lru_node *obj);
static int texture_cache_entry_compare(struct lru_node *obj, void *key);
static guint vertex_shader_hash(gconstpointer key);
static gboolean vertex_shader_equal(gconstpointer a, gconstpointer b);
static guint fragment_shader_hash(gconstpointer key);
static gboolean fragment_shader_equal(gconstpointer a, gconstpointer b);
static unsigned int kelvin_map_stencil_op(uint32_t parameter);
static unsigned int kelvin_map_polygon_mode(uint32_t parameter);
static unsigned int kelvin_map_texgen(uint32_t parameter, unsigned int channel);
static uint64_t fnv_hash(const uint8_t *data, size_t len);
static uint64_t fast_hash(const uint8_t *data, size_t len, unsigned int samples);


NV2AState *fuck_fuck_fixme;

/* PGRAPH - accelerated 2d/3d drawing engine */
uint64_t pgraph_read(void *opaque, hwaddr addr, unsigned int size)
{
    NV2AState *d = (NV2AState *)opaque;

#if !USE_COROUTINES
    qemu_mutex_lock(&d->pgraph.lock);
#endif

    uint64_t r = 0;
    switch (addr) {
    case NV_PGRAPH_INTR:
        r = d->pgraph.pending_interrupts;
        break;
    case NV_PGRAPH_INTR_EN:
        r = d->pgraph.enabled_interrupts;
        break;
    default:
        r = d->pgraph.regs[addr];
        break;
    }

#if !USE_COROUTINES
    qemu_mutex_unlock(&d->pgraph.lock);
#endif

    reg_log_read(NV_PGRAPH, addr, r);
    return r;
}

void pgraph_write(void *opaque, hwaddr addr, uint64_t val, unsigned int size)
{
    NV2AState *d = (NV2AState *)opaque;

    reg_log_write(NV_PGRAPH, addr, val);

#if !USE_COROUTINES
    qemu_mutex_lock(&d->pgraph.lock);
#endif

    switch (addr) {
    case NV_PGRAPH_INTR:
        d->pgraph.pending_interrupts &= ~val;
        CRPRINTF("pgraph_intr set!\n");
#if !USE_COROUTINES
        qemu_cond_broadcast(&d->pgraph.interrupt_cond);
#endif
        break;
    case NV_PGRAPH_INTR_EN:
        d->pgraph.enabled_interrupts = val;
        break;
    case NV_PGRAPH_INCREMENT:
        if (val & NV_PGRAPH_INCREMENT_READ_3D) {
            SET_MASK(d->pgraph.regs[NV_PGRAPH_SURFACE],
                     NV_PGRAPH_SURFACE_READ_3D,
                     (GET_MASK(d->pgraph.regs[NV_PGRAPH_SURFACE],
                              NV_PGRAPH_SURFACE_READ_3D)+1)
                        % GET_MASK(d->pgraph.regs[NV_PGRAPH_SURFACE],
                                   NV_PGRAPH_SURFACE_MODULO_3D) );
#if USE_COROUTINES
            qemu_spin_lock(&d->pgraph.lock);
            flip_3d = 1;
            qemu_spin_unlock(&d->pgraph.lock);
#else
            qemu_cond_broadcast(&d->pgraph.flip_3d);
#endif
        }
        break;
    case NV_PGRAPH_CHANNEL_CTX_TRIGGER: {
        hwaddr context_address =
            GET_MASK(d->pgraph.regs[NV_PGRAPH_CHANNEL_CTX_POINTER], NV_PGRAPH_CHANNEL_CTX_POINTER_INST) << 4;

        if (val & NV_PGRAPH_CHANNEL_CTX_TRIGGER_READ_IN) {
            unsigned pgraph_channel_id =
                GET_MASK(d->pgraph.regs[NV_PGRAPH_CTX_USER], NV_PGRAPH_CTX_USER_CHID);

            NV2A_DPRINTF("PGRAPH: read channel %d context from %" HWADDR_PRIx "\n",
                         pgraph_channel_id, context_address);

            assert(context_address < memory_region_size(&d->ramin));

            uint8_t *context_ptr = d->ramin_ptr + context_address;
            uint32_t context_user = ldl_le_p((uint32_t*)context_ptr);

            NV2A_DPRINTF("    - CTX_USER = 0x%x\n", context_user);

            d->pgraph.regs[NV_PGRAPH_CTX_USER] = context_user;
            // pgraph_set_context_user(d, context_user);
        }
        if (val & NV_PGRAPH_CHANNEL_CTX_TRIGGER_WRITE_OUT) {
            /* do stuff ... */
        }

        break;
    }
    default:
        d->pgraph.regs[addr] = val;
        break;
    }

    // events
    switch (addr) {
    case NV_PGRAPH_FIFO:
#if USE_COROUTINES
        CRPRINTF("fifo_access_cond = 1\n");
        fifo_access_cond = 1;
#else
        qemu_cond_broadcast(&d->pgraph.fifo_access_cond);
#endif
        break;
    }

#if !USE_COROUTINES
    qemu_mutex_unlock(&d->pgraph.lock);
#endif
}


struct blit_texture {
    uint32_t vaddr; // Destination address
    GLuint   tex;   // Destination texture
} blit_textures[20];

int num_methods_executed = 0;
int num_obj_methods_executed = 0;
int num_nv097_methods_executed = 0;

static void pgraph_method(NV2AState *d,
                   unsigned int subchannel,
                   unsigned int method,
                   uint32_t parameter)
{
    int i;
    unsigned int slot;

    PGRAPHState *pg = &d->pgraph;

    num_methods_executed++;

    bool channel_valid =
        d->pgraph.regs[NV_PGRAPH_CTX_CONTROL] & NV_PGRAPH_CTX_CONTROL_CHID;
    assert(channel_valid);

    unsigned channel_id = GET_MASK(pg->regs[NV_PGRAPH_CTX_USER], NV_PGRAPH_CTX_USER_CHID);

    ContextSurfaces2DState *context_surfaces_2d = &pg->context_surfaces_2d;
    ImageBlitState *image_blit = &pg->image_blit;
    KelvinState *kelvin = &pg->kelvin;

    assert(subchannel < 8);

    if (method == NV_SET_OBJECT) {
        assert(parameter < memory_region_size(&d->ramin));
        uint8_t *obj_ptr = d->ramin_ptr + parameter;

        uint32_t ctx_1 = ldl_le_p((uint32_t*)obj_ptr);
        uint32_t ctx_2 = ldl_le_p((uint32_t*)(obj_ptr+4));
        uint32_t ctx_3 = ldl_le_p((uint32_t*)(obj_ptr+8));
        uint32_t ctx_4 = ldl_le_p((uint32_t*)(obj_ptr+12));
        uint32_t ctx_5 = parameter;

        pg->regs[NV_PGRAPH_CTX_CACHE1 + subchannel * 4] = ctx_1;
        pg->regs[NV_PGRAPH_CTX_CACHE2 + subchannel * 4] = ctx_2;
        pg->regs[NV_PGRAPH_CTX_CACHE3 + subchannel * 4] = ctx_3;
        pg->regs[NV_PGRAPH_CTX_CACHE4 + subchannel * 4] = ctx_4;
        pg->regs[NV_PGRAPH_CTX_CACHE5 + subchannel * 4] = ctx_5;
    }

    // is this right?
    pg->regs[NV_PGRAPH_CTX_SWITCH1] = pg->regs[NV_PGRAPH_CTX_CACHE1 + subchannel * 4];
    pg->regs[NV_PGRAPH_CTX_SWITCH2] = pg->regs[NV_PGRAPH_CTX_CACHE2 + subchannel * 4];
    pg->regs[NV_PGRAPH_CTX_SWITCH3] = pg->regs[NV_PGRAPH_CTX_CACHE3 + subchannel * 4];
    pg->regs[NV_PGRAPH_CTX_SWITCH4] = pg->regs[NV_PGRAPH_CTX_CACHE4 + subchannel * 4];
    pg->regs[NV_PGRAPH_CTX_SWITCH5] = pg->regs[NV_PGRAPH_CTX_CACHE5 + subchannel * 4];

    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);

    // NV2A_DPRINTF("graphics_class %d 0x%x\n", subchannel, graphics_class);
    pgraph_method_log(subchannel, graphics_class, method, parameter);

#if PROFILE_METHODS
    if (graphics_class == NV_KELVIN_PRIMITIVE) {
        track_pgraph_method(subchannel, method, parameter);
    }
#endif

    if (subchannel != 0) {
        // catches context switching issues on xbox d3d
        assert(graphics_class != 0x97);
    }

    /* ugly switch for now */
    switch (graphics_class) {

    case NV_CONTEXT_PATTERN: { switch (method) {
    case NV044_SET_MONOCHROME_COLOR0:
        pg->regs[NV_PGRAPH_PATT_COLOR0] = parameter;
        break;
    } break; }

    case NV_CONTEXT_SURFACES_2D: { switch (method) {
    case NV062_SET_OBJECT:
        context_surfaces_2d->object_instance = parameter;
        break;

    case NV062_SET_CONTEXT_DMA_IMAGE_SOURCE:
        context_surfaces_2d->dma_image_source = parameter;
        break;
    case NV062_SET_CONTEXT_DMA_IMAGE_DESTIN:
        context_surfaces_2d->dma_image_dest = parameter;
        break;
    case NV062_SET_COLOR_FORMAT:
        context_surfaces_2d->color_format = parameter;
        break;
    case NV062_SET_PITCH:
        context_surfaces_2d->source_pitch = parameter & 0xFFFF;
        context_surfaces_2d->dest_pitch = parameter >> 16;
        break;
    case NV062_SET_OFFSET_SOURCE:
        context_surfaces_2d->source_offset = parameter & 0x07FFFFFF;
        break;
    case NV062_SET_OFFSET_DESTIN:
        context_surfaces_2d->dest_offset = parameter & 0x07FFFFFF;
        break;
    } break; }

    case NV_IMAGE_BLIT: { switch (method) {
    case NV09F_SET_OBJECT:
        image_blit->object_instance = parameter;
        break;

    case NV09F_SET_CONTEXT_SURFACES:
        image_blit->context_surfaces = parameter;
        break;
    case NV09F_SET_OPERATION:
        image_blit->operation = parameter;
        break;
    case NV09F_CONTROL_POINT_IN:
        image_blit->in_x = parameter & 0xFFFF;
        image_blit->in_y = parameter >> 16;
        break;
    case NV09F_CONTROL_POINT_OUT:
        image_blit->out_x = parameter & 0xFFFF;
        image_blit->out_y = parameter >> 16;
        break;
    case NV09F_SIZE:
        image_blit->width = parameter & 0xFFFF;
        image_blit->height = parameter >> 16;

        /* I guess this kicks it off? */
        if (image_blit->operation == NV09F_SET_OPERATION_SRCCOPY) {

            // Force write-back to mem
            pgraph_update_surface(d, false, true, true);


            NV2A_GL_DPRINTF(true, "NV09F_SET_OPERATION_SRCCOPY");

            ContextSurfaces2DState *context_surfaces = context_surfaces_2d;
            assert(context_surfaces->object_instance
                    == image_blit->context_surfaces);

            unsigned int bytes_per_pixel;
            switch (context_surfaces->color_format) {
            case NV062_SET_COLOR_FORMAT_LE_Y8:
                bytes_per_pixel = 1;
                break;
            case NV062_SET_COLOR_FORMAT_LE_R5G6B5:
                bytes_per_pixel = 2;
                break;
            case NV062_SET_COLOR_FORMAT_LE_A8R8G8B8:
                bytes_per_pixel = 4;
                break;
            default:
                fprintf(stderr, "Unknown blit surface format: 0x%x\n", context_surfaces->color_format);
                assert(false);
                break;
            }

            hwaddr source_dma_len, dest_dma_len;
            uint8_t *source, *dest;

            source = (uint8_t*)nv_dma_map(d, context_surfaces->dma_image_source,
                                          &source_dma_len);
            assert(context_surfaces->source_offset < source_dma_len);
            source += context_surfaces->source_offset;

            dest = (uint8_t*)nv_dma_map(d, context_surfaces->dma_image_dest,
                                        &dest_dma_len);
            assert(context_surfaces->dest_offset < dest_dma_len);
            dest += context_surfaces->dest_offset;

            NV2A_DPRINTF("  - 0x%tx -> 0x%tx\n", source - d->vram_ptr,
                                                 dest - d->vram_ptr);

#if RENDER_TO_TEXTURE

            // Create new surface at dest and put it in surface cache
            if (source - d->vram_ptr == pg->gl_color_buffer_offset) {
                printf("BLITTING FROM CURRENT COLOR BUFFER\n");
                glFinish();

                GLuint gl_buf;

                // Create a new texture to copy the surface into
                glGenTextures(1, &gl_buf);
                glBindTexture(GL_TEXTURE_2D, gl_buf);
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_BASE_LEVEL, 0);
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAX_LEVEL, 0);
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);

                SurfaceColorFormatInfo f =
                    kelvin_surface_color_format_map[pg->surface_shape.color_format];

                GLenum gl_internal_format = f.gl_internal_format;
                GLenum gl_format = f.gl_format;
                GLenum gl_type = f.gl_type;

                int width = image_blit->width;
                int height = image_blit->height;
#if RES_SCALE_FACTOR != 1
                width *= RES_SCALE_FACTOR;
                height *= RES_SCALE_FACTOR;
#endif

                glTexImage2D(GL_TEXTURE_2D, 0, gl_internal_format,
                     width, height,
                     0, gl_format, gl_type,
                     NULL); // skipping upload
#if RENDER_TO_TEXTURE_COPY
                for (int i = 0; i < height; i++) {
                    glCopyImageSubData(
    #if 0
                        pg->gl_color_buffer, GL_TEXTURE_2D, 0, 0, height-i-1, 0,
    #else
                        pg->gl_color_buffer, GL_TEXTURE_2D, 0, 0, i, 0,
    #endif
                        gl_buf,              GL_TEXTURE_2D, 0, 0, i, 0,
                        width/4, 1, 1);
                }
#else
                // pgraph_render_surface_to_texture(
                //     NV2AState *d, GLsync fence,
                //     GLuint src, GLenum src_format, GLenum src_target,
                //     GLuint dst, GLenum dst_format, GLenum dst_target,
                //     int width, int height, int src_zeta
                pgraph_render_surface_to_texture(
                    d, 0,//glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 ),
                    pg->gl_color_buffer, gl_format, GL_TEXTURE_2D,
                    gl_buf, gl_format, GL_TEXTURE_2D,
                    width/4, height, 0, 0
                    );
#endif

                int index = surface_cache_store(dest - d->vram_ptr);
                surface_cache[index].buf_id = gl_buf;
                // surface_cache[index].fence = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 ); // Should probably be moved below
                surface_cache[index].color = 1; //fixme
                memcpy(&surface_cache[index].shape, &pg->surface_shape, sizeof(SurfaceShape));

                printf("CREATED NEW ENTRY IN SURFACE CACHE\n");
            }  else {
                // assert(false);
#if 0
                int index = surface_cache_find(texture_vram_offset);
                if (index >= 0) {
                    printf("FOUND BLIT SRC IN SURFACE CACHE\n");
#endif
            }

#endif


        // printf("~~~~~~~ performing copy! (%d,%d) @ %08x -> (%d,%d) @ %08x, size = %dx%d\n",
        //     image_blit->in_x,
        //     image_blit->in_y,
        //     source - d->vram_ptr,
        //     image_blit->out_x,
        //     image_blit->out_y,
        //     dest - d->vram_ptr,
        //     image_blit->width, image_blit->height
        //     );

            int y;
            for (y=0; y<image_blit->height; y++) {
                uint8_t *source_row = source
                    + (image_blit->in_y + y) * context_surfaces->source_pitch
                    + image_blit->in_x * bytes_per_pixel;

                uint8_t *dest_row = dest
                    + (image_blit->out_y + y) * context_surfaces->dest_pitch
                    + image_blit->out_x * bytes_per_pixel;

                memmove(dest_row, source_row,
                        image_blit->width * bytes_per_pixel);
            }
        } else {
            assert(false);
        }

        break;
    } break; }


    case NV_KELVIN_PRIMITIVE: { switch (method) {

    case NV097_SET_OBJECT:
        kelvin->object_instance = parameter;
        break;

    case NV097_NO_OPERATION:
        /* The bios uses nop as a software method call -
         * it seems to expect a notify interrupt if the parameter isn't 0.
         * According to a nouveau guy it should still be a nop regardless
         * of the parameter. It's possible a debug register enables this,
         * but nothing obvious sticks out. Weird.
         */
        if (parameter != 0) {
            assert(!(pg->pending_interrupts & NV_PGRAPH_INTR_ERROR));

            SET_MASK(pg->regs[NV_PGRAPH_TRAPPED_ADDR],
                NV_PGRAPH_TRAPPED_ADDR_CHID, channel_id);
            SET_MASK(pg->regs[NV_PGRAPH_TRAPPED_ADDR],
                NV_PGRAPH_TRAPPED_ADDR_SUBCH, subchannel);
            SET_MASK(pg->regs[NV_PGRAPH_TRAPPED_ADDR],
                NV_PGRAPH_TRAPPED_ADDR_MTHD, method);
            pg->regs[NV_PGRAPH_TRAPPED_DATA_LOW] = parameter;
            pg->regs[NV_PGRAPH_NSOURCE] = NV_PGRAPH_NSOURCE_NOTIFICATION; /* TODO: check this */
            pg->pending_interrupts |= NV_PGRAPH_INTR_ERROR;

#if !USE_COROUTINES
            qemu_mutex_unlock(&pg->lock);
#endif
            // printf("TIME TO UPDATE IRQ: ");
            // time_this(1);

            qemu_mutex_lock_iothread();
            CRPRINTF("updating IRQ\n");
            update_irq(d);
#if !USE_COROUTINES
            qemu_mutex_lock(&pg->lock);
#endif
            qemu_mutex_unlock_iothread();

            // time_this(0);

            while (pg->pending_interrupts & NV_PGRAPH_INTR_ERROR) {
#if USE_COROUTINES
                CRPRINTF("pgraph waiting for error to clear\n");
                qemu_coroutine_yield();
#else
                qemu_cond_wait(&pg->interrupt_cond, &pg->lock);
#endif
            }
        }
        break;

    case NV097_WAIT_FOR_IDLE:
        SDPRINTF("NV097_WAIT_FOR_IDLE\n");
        NV2A_GL_DPRINTF(true, "NV097_WAIT_FOR_IDLE -- crt = %08x",  d->pcrtc.start);

        pgraph_update_surface(d, false, true, true);
        break;


    case NV097_SET_FLIP_READ:
        SET_MASK(pg->regs[NV_PGRAPH_SURFACE], NV_PGRAPH_SURFACE_READ_3D,
                 parameter);
        break;
    case NV097_SET_FLIP_WRITE:
        SDPRINTF("NV097_SET_FLIP_WRITE -- crt = %08x\n",  d->pcrtc.start);
        NV2A_GL_DPRINTF(true, "NV097_SET_FLIP_WRITE -- crt = %08x",  d->pcrtc.start);

        SET_MASK(pg->regs[NV_PGRAPH_SURFACE], NV_PGRAPH_SURFACE_WRITE_3D,
                 parameter);
        break;
    case NV097_SET_FLIP_MODULO:
        SET_MASK(pg->regs[NV_PGRAPH_SURFACE], NV_PGRAPH_SURFACE_MODULO_3D,
                 parameter);
        break;
    case NV097_FLIP_INCREMENT_WRITE: {

        SDPRINTF("NV097_FLIP_INCREMENT_WRITE\n");
                // pgraph_update_surface(d, false, true, true);


        NV2A_DPRINTF("flip increment write %d -> ",
            GET_MASK(pg->regs[NV_PGRAPH_SURFACE],
                          NV_PGRAPH_SURFACE_WRITE_3D));
        SET_MASK(pg->regs[NV_PGRAPH_SURFACE],
                 NV_PGRAPH_SURFACE_WRITE_3D,
                 (GET_MASK(pg->regs[NV_PGRAPH_SURFACE],
                          NV_PGRAPH_SURFACE_WRITE_3D)+1)
                    % GET_MASK(pg->regs[NV_PGRAPH_SURFACE],
                               NV_PGRAPH_SURFACE_MODULO_3D) );
        NV2A_DPRINTF("%d\n",
            GET_MASK(pg->regs[NV_PGRAPH_SURFACE],
                          NV_PGRAPH_SURFACE_WRITE_3D));



        NV2A_GL_DPRINTF(true, "NV097_FLIP_INCREMENT_WRITE -- crt = %08x",  d->pcrtc.start);













#if USE_SHARED_CONTEXT
        //---------------------------------------------------------------------------
        GLsync fence;
        GLuint fb_tex_tmp;
        SDPRINTF("frame: crt = %08lx\n", d->pcrtc.start);
        SDPRINTF("       color offset = %08lx\n", pg->gl_color_buffer_offset);
        int index = surface_cache_find(d->pcrtc.start, 1);

        if (index > 0) {
            NV2A_GL_DPRINTF(true, "Found GL buf! Making frame available (%d)", surface_cache[index].buf_id);
            // printf("GL buf found in cache! %d\n", surface_cache[index].buf_id);
            fb_tex_tmp = surface_cache[index].buf_id;
            // fence = surface_cache[index].fence;
        } else {
            // printf("GL buf not found :(\n");
            fb_tex_tmp = 0;
            // fence = 0;
        }


        if (d->pcrtc.start == pg->gl_color_buffer_offset) {
            // printf("Single Buffered!\n");
            fb_tex_tmp = pg->gl_color_buffer;
        }


        fence = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 );
#if 1
        while(1)
        {
            int result = glClientWaitSync(fence, GL_SYNC_FLUSH_COMMANDS_BIT, (GLuint64)(5000000000)); //5 Second timeout
            if(result != GL_TIMEOUT_EXPIRED) {
                glDeleteSync(fence);
                break; //we ignore timeouts and wait until all OpenGL commands are processed!
            }
        }
#endif


        // print_timestamp();
        // printf("FRAME COMPLETE\n");

        stop_frame_timer();

#if 0
        if (fence == 0) {
            SDPRINTF("Sync point not found... forcing sync!\n");
            fence = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 );
        }

        glFinish();
#endif
#if 0
        // Make frame available
        while (available) {
            printf("waiting for previous frame to be consumed\n");
            qemu_coroutine_yield();
        }
#endif
        qemu_spin_lock(&avail_spinner);
        available = 1;
        fb_tex = fb_tex_tmp;
        fb_sync = fence;
        qemu_spin_unlock(&avail_spinner);
#if 0
        while(1)
        {
            int result = glClientWaitSync(fence, GL_SYNC_FLUSH_COMMANDS_BIT, (GLuint64)(5000000000)); //5 Second timeout
            if(result != GL_TIMEOUT_EXPIRED) break; //we ignore timeouts and wait until all OpenGL commands are processed!
        }
#endif
#endif

#if 0
        int reps = 0;
        while (available) {
            /// kills perf...
            // if (++reps > 750) {
            //     printf("waiting for frame to be consumed\n");
            //     reps = 0;
            // }
            qemu_coroutine_yield();
        }
#endif


glo_set_current(pg->gl_context);

        NV2A_GL_DFRAME_TERMINATOR();

        //-----------------------------------------------------------------------------------

        break;
    }
    case NV097_FLIP_STALL:
        SDPRINTF("NV097_FLIP_STALL\n");
        pgraph_update_surface(d, false, true, true);

        NV2A_GL_DPRINTF(true, "NV097_FLIP_STALL -- crt = %08x",  d->pcrtc.start);


        // if (flip_sync) {
        //     glWaitSync(flip_sync, 0, GL_TIMEOUT_IGNORED);
        // }

        // time_this(1);

#if 1
{

#if 1
        if (fb_sync) {
#if PROFILE_FLIP
            printf("WAITING FOR FRAME TO FINISH: ");
            time_this(1);
#endif
            glClientWaitSync(fb_sync, GL_SYNC_FLUSH_COMMANDS_BIT, (GLuint64)(5000000000)); //5 Second timeout

#if PROFILE_FLIP
            time_this(0);
#endif
        }
#endif
#if 0
        int reps = 0;
        while (available) {
            if (++reps > 750) {
                SDPRINTF("waiting for frame to be consumed\n");
                reps = 0;
            }
            qemu_coroutine_yield();
        }
#endif
}
#endif

        // time_this(0);

#if PROFILE_FLIP
        printf("WAITING FOR FLIP: ");
        time_this(1);
#endif

        while (true) {
            NV2A_DPRINTF("flip stall read: %d, write: %d, modulo: %d\n",
                GET_MASK(pg->regs[NV_PGRAPH_SURFACE], NV_PGRAPH_SURFACE_READ_3D),
                GET_MASK(pg->regs[NV_PGRAPH_SURFACE], NV_PGRAPH_SURFACE_WRITE_3D),
                GET_MASK(pg->regs[NV_PGRAPH_SURFACE], NV_PGRAPH_SURFACE_MODULO_3D));

            uint32_t s = pg->regs[NV_PGRAPH_SURFACE];
            if (GET_MASK(s, NV_PGRAPH_SURFACE_READ_3D)
                != GET_MASK(s, NV_PGRAPH_SURFACE_WRITE_3D)) {
                break;
            }

#if USE_COROUTINES
            while (1) {
                int should_break = 0;
                qemu_spin_lock(&pg->lock);
                if (flip_3d) {
                    should_break = 1;
                    flip_3d = 0;
                }
                qemu_spin_unlock(&pg->lock);

                if (should_break) break;
                else qemu_coroutine_yield();
            }
#else
            qemu_cond_wait(&pg->flip_3d, &pg->lock);
#endif
        }


#if PROFILE_FLIP
            time_this(0);
#endif

        NV2A_GL_DPRINTF(true, "NV097_FLIP_STALL DONE -- crt = %08x",  d->pcrtc.start);

        NV2A_DPRINTF("flip stall done\n");
        break;

    // TODO: these should be loading the dma objects from ramin here?
    case NV097_SET_CONTEXT_DMA_NOTIFIES:
        pg->dma_notifies = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_A:
        pg->dma_a = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_B:
        pg->dma_b = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_STATE:
        pg->dma_state = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_COLOR:
        /* try to get any straggling draws in before the surface's changed :/ */
        SDPRINTF("NV097_SET_CONTEXT_DMA_COLOR\n");
        pgraph_update_surface(d, false, true, true);

        pg->dma_color = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_ZETA:
        pg->dma_zeta = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_VERTEX_A:
        pg->dma_vertex_a = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_VERTEX_B:
        pg->dma_vertex_b = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_SEMAPHORE:
        pg->dma_semaphore = parameter;
        break;
    case NV097_SET_CONTEXT_DMA_REPORT:
        pg->dma_report = parameter;
        break;

    case NV097_SET_SURFACE_CLIP_HORIZONTAL:
        SDPRINTF("NV097_SET_SURFACE_CLIP_HORIZONTAL\n");
        pgraph_update_surface(d, false, true, true);

        pg->surface_shape.clip_x =
            GET_MASK(parameter, NV097_SET_SURFACE_CLIP_HORIZONTAL_X);
        pg->surface_shape.clip_width =
            GET_MASK(parameter, NV097_SET_SURFACE_CLIP_HORIZONTAL_WIDTH);
        break;
    case NV097_SET_SURFACE_CLIP_VERTICAL:
        SDPRINTF("NV097_SET_SURFACE_CLIP_VERTICAL\n");
        pgraph_update_surface(d, false, true, true);

        pg->surface_shape.clip_y =
            GET_MASK(parameter, NV097_SET_SURFACE_CLIP_VERTICAL_Y);
        pg->surface_shape.clip_height =
            GET_MASK(parameter, NV097_SET_SURFACE_CLIP_VERTICAL_HEIGHT);
        break;
    case NV097_SET_SURFACE_FORMAT:
        SDPRINTF("NV097_SET_SURFACE_FORMAT\n");
        pgraph_update_surface(d, false, true, true);

        pg->surface_shape.color_format =
            GET_MASK(parameter, NV097_SET_SURFACE_FORMAT_COLOR);
        pg->surface_shape.zeta_format =
            GET_MASK(parameter, NV097_SET_SURFACE_FORMAT_ZETA);
        pg->surface_type =
            GET_MASK(parameter, NV097_SET_SURFACE_FORMAT_TYPE);
        pg->surface_shape.anti_aliasing =
            GET_MASK(parameter, NV097_SET_SURFACE_FORMAT_ANTI_ALIASING);
        pg->surface_shape.log_width =
            GET_MASK(parameter, NV097_SET_SURFACE_FORMAT_WIDTH);
        pg->surface_shape.log_height =
            GET_MASK(parameter, NV097_SET_SURFACE_FORMAT_HEIGHT);
        break;
    case NV097_SET_SURFACE_PITCH:
        SDPRINTF("NV097_SET_SURFACE_PITCH\n");
        pgraph_update_surface(d, false, true, true);

        pg->surface_color.pitch =
            GET_MASK(parameter, NV097_SET_SURFACE_PITCH_COLOR);
        pg->surface_zeta.pitch =
            GET_MASK(parameter, NV097_SET_SURFACE_PITCH_ZETA);

        pg->surface_color.buffer_dirty = true;
        pg->surface_zeta.buffer_dirty = true;
        break;
    case NV097_SET_SURFACE_COLOR_OFFSET:
        SDPRINTF("NV097_SET_SURFACE_COLOR_OFFSET\n");
        pgraph_update_surface(d, false, true, true);

        pg->surface_color.offset = parameter;
        pg->surface_color.buffer_dirty = true;
        break;
    case NV097_SET_SURFACE_ZETA_OFFSET:
        SDPRINTF("NV097_SET_SURFACE_ZETA_OFFSET\n");
        pgraph_update_surface(d, false, true, true);

        pg->surface_zeta.offset = parameter;
        pg->surface_zeta.buffer_dirty = true;
        break;

    case NV097_SET_COMBINER_ALPHA_ICW ...
            NV097_SET_COMBINER_ALPHA_ICW + 28:
        slot = (method - NV097_SET_COMBINER_ALPHA_ICW) / 4;
        pg->regs[NV_PGRAPH_COMBINEALPHAI0 + slot*4] = parameter;
        break;

    case NV097_SET_COMBINER_SPECULAR_FOG_CW0:
        pg->regs[NV_PGRAPH_COMBINESPECFOG0] = parameter;
        break;

    case NV097_SET_COMBINER_SPECULAR_FOG_CW1:
        pg->regs[NV_PGRAPH_COMBINESPECFOG1] = parameter;
        break;

    CASE_4(NV097_SET_TEXTURE_ADDRESS, 64):
        slot = (method - NV097_SET_TEXTURE_ADDRESS) / 64;
        pg->regs[NV_PGRAPH_TEXADDRESS0 + slot * 4] = parameter;
        break;
    case NV097_SET_CONTROL0: {
        SDPRINTF("NV097_SET_CONTROL0\n");
        pgraph_update_surface(d, false, true, true);

        bool stencil_write_enable =
            parameter & NV097_SET_CONTROL0_STENCIL_WRITE_ENABLE;
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_STENCIL_WRITE_ENABLE,
                 stencil_write_enable);

        uint32_t z_format = GET_MASK(parameter, NV097_SET_CONTROL0_Z_FORMAT);
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_Z_FORMAT, z_format);

        bool z_perspective =
            parameter & NV097_SET_CONTROL0_Z_PERSPECTIVE_ENABLE;
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_Z_PERSPECTIVE_ENABLE,
                 z_perspective);
        break;
    }

    case NV097_SET_FOG_MODE: {
        /* FIXME: There is also NV_PGRAPH_CSV0_D_FOG_MODE */
        unsigned int mode;
        switch (parameter) {
        case NV097_SET_FOG_MODE_V_LINEAR:
            mode = NV_PGRAPH_CONTROL_3_FOG_MODE_LINEAR; break;
        case NV097_SET_FOG_MODE_V_EXP:
            mode = NV_PGRAPH_CONTROL_3_FOG_MODE_EXP; break;
        case NV097_SET_FOG_MODE_V_EXP2:
            mode = NV_PGRAPH_CONTROL_3_FOG_MODE_EXP2; break;
        case NV097_SET_FOG_MODE_V_EXP_ABS:
            mode = NV_PGRAPH_CONTROL_3_FOG_MODE_EXP_ABS; break;
        case NV097_SET_FOG_MODE_V_EXP2_ABS:
            mode = NV_PGRAPH_CONTROL_3_FOG_MODE_EXP2_ABS; break;
        case NV097_SET_FOG_MODE_V_LINEAR_ABS:
            mode = NV_PGRAPH_CONTROL_3_FOG_MODE_LINEAR_ABS; break;
        default:
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_3], NV_PGRAPH_CONTROL_3_FOG_MODE,
                 mode);
        break;
    }
    case NV097_SET_FOG_GEN_MODE: {
        unsigned int mode;
        switch (parameter) {
        case NV097_SET_FOG_GEN_MODE_V_SPEC_ALPHA:
            mode = NV_PGRAPH_CSV0_D_FOGGENMODE_SPEC_ALPHA; break;
        case NV097_SET_FOG_GEN_MODE_V_RADIAL:
            mode = NV_PGRAPH_CSV0_D_FOGGENMODE_RADIAL; break;
        case NV097_SET_FOG_GEN_MODE_V_PLANAR:
            mode = NV_PGRAPH_CSV0_D_FOGGENMODE_PLANAR; break;
        case NV097_SET_FOG_GEN_MODE_V_ABS_PLANAR:
            mode = NV_PGRAPH_CSV0_D_FOGGENMODE_ABS_PLANAR; break;
        case NV097_SET_FOG_GEN_MODE_V_FOG_X:
            mode = NV_PGRAPH_CSV0_D_FOGGENMODE_FOG_X; break;
        default:
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_D], NV_PGRAPH_CSV0_D_FOGGENMODE, mode);
        break;
    }
    case NV097_SET_FOG_ENABLE:
/*
      FIXME: There is also:
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_D], NV_PGRAPH_CSV0_D_FOGENABLE,
             parameter);
*/
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_3], NV_PGRAPH_CONTROL_3_FOGENABLE,
             parameter);
        break;
    case NV097_SET_FOG_COLOR: {
        /* PGRAPH channels are ARGB, parameter channels are ABGR */
        uint8_t red = GET_MASK(parameter, NV097_SET_FOG_COLOR_RED);
        uint8_t green = GET_MASK(parameter, NV097_SET_FOG_COLOR_GREEN);
        uint8_t blue = GET_MASK(parameter, NV097_SET_FOG_COLOR_BLUE);
        uint8_t alpha = GET_MASK(parameter, NV097_SET_FOG_COLOR_ALPHA);
        SET_MASK(pg->regs[NV_PGRAPH_FOGCOLOR], NV_PGRAPH_FOGCOLOR_RED, red);
        SET_MASK(pg->regs[NV_PGRAPH_FOGCOLOR], NV_PGRAPH_FOGCOLOR_GREEN, green);
        SET_MASK(pg->regs[NV_PGRAPH_FOGCOLOR], NV_PGRAPH_FOGCOLOR_BLUE, blue);
        SET_MASK(pg->regs[NV_PGRAPH_FOGCOLOR], NV_PGRAPH_FOGCOLOR_ALPHA, alpha);
        break;
    }
    case NV097_SET_WINDOW_CLIP_TYPE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_WINDOWCLIPTYPE, parameter);
        break;
    case NV097_SET_WINDOW_CLIP_HORIZONTAL ...
            NV097_SET_WINDOW_CLIP_HORIZONTAL + 0x1c:
        slot = (method - NV097_SET_WINDOW_CLIP_HORIZONTAL) / 4;
        pg->regs[NV_PGRAPH_WINDOWCLIPX0 + slot * 4] = parameter;
        break;
    case NV097_SET_WINDOW_CLIP_VERTICAL ...
            NV097_SET_WINDOW_CLIP_VERTICAL + 0x1c:
        slot = (method - NV097_SET_WINDOW_CLIP_VERTICAL) / 4;
        pg->regs[NV_PGRAPH_WINDOWCLIPY0 + slot * 4] = parameter;
        break;
    case NV097_SET_ALPHA_TEST_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_ALPHATESTENABLE, parameter);
        break;
    case NV097_SET_BLEND_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_BLEND], NV_PGRAPH_BLEND_EN, parameter);
        break;
    case NV097_SET_CULL_FACE_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_CULLENABLE,
                 parameter);
        break;
    case NV097_SET_DEPTH_TEST_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0], NV_PGRAPH_CONTROL_0_ZENABLE,
                 parameter);
        break;
    case NV097_SET_DITHER_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_DITHERENABLE, parameter);
        break;
    case NV097_SET_LIGHTING_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_C], NV_PGRAPH_CSV0_C_LIGHTING,
                 parameter);
        break;
    case NV097_SET_SKIN_MODE:
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_D], NV_PGRAPH_CSV0_D_SKIN,
                 parameter);
        break;
    case NV097_SET_STENCIL_TEST_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                 NV_PGRAPH_CONTROL_1_STENCIL_TEST_ENABLE, parameter);
        break;
    case NV097_SET_POLY_OFFSET_POINT_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_POFFSETPOINTENABLE, parameter);
        break;
    case NV097_SET_POLY_OFFSET_LINE_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_POFFSETLINEENABLE, parameter);
        break;
    case NV097_SET_POLY_OFFSET_FILL_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_POFFSETFILLENABLE, parameter);
        break;
    case NV097_SET_ALPHA_FUNC:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_ALPHAFUNC, parameter & 0xF);
        break;
    case NV097_SET_ALPHA_REF:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_ALPHAREF, parameter);
        break;
    case NV097_SET_BLEND_FUNC_SFACTOR: {
        unsigned int factor;
        switch (parameter) {
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ZERO:
            factor = NV_PGRAPH_BLEND_SFACTOR_ZERO; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_SRC_COLOR:
            factor = NV_PGRAPH_BLEND_SFACTOR_SRC_COLOR; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE_MINUS_SRC_COLOR:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE_MINUS_SRC_COLOR; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_SRC_ALPHA:
            factor = NV_PGRAPH_BLEND_SFACTOR_SRC_ALPHA; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE_MINUS_SRC_ALPHA:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE_MINUS_SRC_ALPHA; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_DST_ALPHA:
            factor = NV_PGRAPH_BLEND_SFACTOR_DST_ALPHA; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE_MINUS_DST_ALPHA:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE_MINUS_DST_ALPHA; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_DST_COLOR:
            factor = NV_PGRAPH_BLEND_SFACTOR_DST_COLOR; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE_MINUS_DST_COLOR:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE_MINUS_DST_COLOR; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_SRC_ALPHA_SATURATE:
            factor = NV_PGRAPH_BLEND_SFACTOR_SRC_ALPHA_SATURATE; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_CONSTANT_COLOR:
            factor = NV_PGRAPH_BLEND_SFACTOR_CONSTANT_COLOR; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE_MINUS_CONSTANT_COLOR:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE_MINUS_CONSTANT_COLOR; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_CONSTANT_ALPHA:
            factor = NV_PGRAPH_BLEND_SFACTOR_CONSTANT_ALPHA; break;
        case NV097_SET_BLEND_FUNC_SFACTOR_V_ONE_MINUS_CONSTANT_ALPHA:
            factor = NV_PGRAPH_BLEND_SFACTOR_ONE_MINUS_CONSTANT_ALPHA; break;
        default:
            fprintf(stderr, "Unknown blend source factor: 0x%x\n", parameter);
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_BLEND], NV_PGRAPH_BLEND_SFACTOR, factor);

        break;
    }

    case NV097_SET_BLEND_FUNC_DFACTOR: {
        unsigned int factor;
        switch (parameter) {
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ZERO:
            factor = NV_PGRAPH_BLEND_DFACTOR_ZERO; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_SRC_COLOR:
            factor = NV_PGRAPH_BLEND_DFACTOR_SRC_COLOR; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE_MINUS_SRC_COLOR:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE_MINUS_SRC_COLOR; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_SRC_ALPHA:
            factor = NV_PGRAPH_BLEND_DFACTOR_SRC_ALPHA; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE_MINUS_SRC_ALPHA:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE_MINUS_SRC_ALPHA; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_DST_ALPHA:
            factor = NV_PGRAPH_BLEND_DFACTOR_DST_ALPHA; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE_MINUS_DST_ALPHA:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE_MINUS_DST_ALPHA; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_DST_COLOR:
            factor = NV_PGRAPH_BLEND_DFACTOR_DST_COLOR; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE_MINUS_DST_COLOR:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE_MINUS_DST_COLOR; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_SRC_ALPHA_SATURATE:
            factor = NV_PGRAPH_BLEND_DFACTOR_SRC_ALPHA_SATURATE; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_CONSTANT_COLOR:
            factor = NV_PGRAPH_BLEND_DFACTOR_CONSTANT_COLOR; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE_MINUS_CONSTANT_COLOR:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE_MINUS_CONSTANT_COLOR; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_CONSTANT_ALPHA:
            factor = NV_PGRAPH_BLEND_DFACTOR_CONSTANT_ALPHA; break;
        case NV097_SET_BLEND_FUNC_DFACTOR_V_ONE_MINUS_CONSTANT_ALPHA:
            factor = NV_PGRAPH_BLEND_DFACTOR_ONE_MINUS_CONSTANT_ALPHA; break;
        default:
            fprintf(stderr, "Unknown blend destination factor: 0x%x\n", parameter);
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_BLEND], NV_PGRAPH_BLEND_DFACTOR, factor);

        break;
    }

    case NV097_SET_BLEND_COLOR:
        pg->regs[NV_PGRAPH_BLENDCOLOR] = parameter;
        break;

    case NV097_SET_BLEND_EQUATION: {
        unsigned int equation;
        switch (parameter) {
        case NV097_SET_BLEND_EQUATION_V_FUNC_SUBTRACT:
            equation = 0; break;
        case NV097_SET_BLEND_EQUATION_V_FUNC_REVERSE_SUBTRACT:
            equation = 1; break;
        case NV097_SET_BLEND_EQUATION_V_FUNC_ADD:
            equation = 2; break;
        case NV097_SET_BLEND_EQUATION_V_MIN:
            equation = 3; break;
        case NV097_SET_BLEND_EQUATION_V_MAX:
            equation = 4; break;
        case NV097_SET_BLEND_EQUATION_V_FUNC_REVERSE_SUBTRACT_SIGNED:
            equation = 5; break;
        case NV097_SET_BLEND_EQUATION_V_FUNC_ADD_SIGNED:
            equation = 6; break;
        default:
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_BLEND], NV_PGRAPH_BLEND_EQN, equation);

        break;
    }

    case NV097_SET_DEPTH_FUNC:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0], NV_PGRAPH_CONTROL_0_ZFUNC,
                 parameter & 0xF);
        break;

    case NV097_SET_COLOR_MASK: {
        pg->surface_color.write_enabled_cache |= pgraph_color_write_enabled(pg);

        bool alpha = parameter & NV097_SET_COLOR_MASK_ALPHA_WRITE_ENABLE;
        bool red = parameter & NV097_SET_COLOR_MASK_RED_WRITE_ENABLE;
        bool green = parameter & NV097_SET_COLOR_MASK_GREEN_WRITE_ENABLE;
        bool blue = parameter & NV097_SET_COLOR_MASK_BLUE_WRITE_ENABLE;
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_ALPHA_WRITE_ENABLE, alpha);
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_RED_WRITE_ENABLE, red);
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_GREEN_WRITE_ENABLE, green);
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_BLUE_WRITE_ENABLE, blue);
        break;
    }
    case NV097_SET_DEPTH_MASK:
        pg->surface_zeta.write_enabled_cache |= pgraph_zeta_write_enabled(pg);

        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                 NV_PGRAPH_CONTROL_0_ZWRITEENABLE, parameter);
        break;
    case NV097_SET_STENCIL_MASK:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                 NV_PGRAPH_CONTROL_1_STENCIL_MASK_WRITE, parameter);
        break;
    case NV097_SET_STENCIL_FUNC:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                 NV_PGRAPH_CONTROL_1_STENCIL_FUNC, parameter & 0xF);
        break;
    case NV097_SET_STENCIL_FUNC_REF:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                 NV_PGRAPH_CONTROL_1_STENCIL_REF, parameter);
        break;
    case NV097_SET_STENCIL_FUNC_MASK:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                 NV_PGRAPH_CONTROL_1_STENCIL_MASK_READ, parameter);
        break;
    case NV097_SET_STENCIL_OP_FAIL:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_2],
                 NV_PGRAPH_CONTROL_2_STENCIL_OP_FAIL,
                 kelvin_map_stencil_op(parameter));
        break;
    case NV097_SET_STENCIL_OP_ZFAIL:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_2],
                 NV_PGRAPH_CONTROL_2_STENCIL_OP_ZFAIL,
                 kelvin_map_stencil_op(parameter));
        break;
    case NV097_SET_STENCIL_OP_ZPASS:
        SET_MASK(pg->regs[NV_PGRAPH_CONTROL_2],
                 NV_PGRAPH_CONTROL_2_STENCIL_OP_ZPASS,
                 kelvin_map_stencil_op(parameter));
        break;

    case NV097_SET_POLYGON_OFFSET_SCALE_FACTOR:
        pg->regs[NV_PGRAPH_ZOFFSETFACTOR] = parameter;
        break;
    case NV097_SET_POLYGON_OFFSET_BIAS:
        pg->regs[NV_PGRAPH_ZOFFSETBIAS] = parameter;
        break;
    case NV097_SET_FRONT_POLYGON_MODE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_FRONTFACEMODE,
                 kelvin_map_polygon_mode(parameter));
        break;
    case NV097_SET_BACK_POLYGON_MODE:
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_BACKFACEMODE,
                 kelvin_map_polygon_mode(parameter));
        break;
    case NV097_SET_CLIP_MIN:
        pg->regs[NV_PGRAPH_ZCLIPMIN] = parameter;
        break;
    case NV097_SET_CLIP_MAX:
        pg->regs[NV_PGRAPH_ZCLIPMAX] = parameter;
        break;
    case NV097_SET_CULL_FACE: {
        unsigned int face;
        switch (parameter) {
        case NV097_SET_CULL_FACE_V_FRONT:
            face = NV_PGRAPH_SETUPRASTER_CULLCTRL_FRONT; break;
        case NV097_SET_CULL_FACE_V_BACK:
            face = NV_PGRAPH_SETUPRASTER_CULLCTRL_BACK; break;
        case NV097_SET_CULL_FACE_V_FRONT_AND_BACK:
            face = NV_PGRAPH_SETUPRASTER_CULLCTRL_FRONT_AND_BACK; break;
        default:
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_CULLCTRL,
                 face);
        break;
    }
    case NV097_SET_FRONT_FACE: {
        bool ccw;
        switch (parameter) {
        case NV097_SET_FRONT_FACE_V_CW:
            ccw = false; break;
        case NV097_SET_FRONT_FACE_V_CCW:
            ccw = true; break;
        default:
            fprintf(stderr, "Unknown front face: 0x%x\n", parameter);
            assert(false);
            break;
        }
        SET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                 NV_PGRAPH_SETUPRASTER_FRONTFACE,
                 ccw ? 1 : 0);
        break;
    }
    case NV097_SET_NORMALIZATION_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_C],
                 NV_PGRAPH_CSV0_C_NORMALIZATION_ENABLE,
                 parameter);
        break;

    case NV097_SET_LIGHT_ENABLE_MASK:
        SET_MASK(d->pgraph.regs[NV_PGRAPH_CSV0_D],
                 NV_PGRAPH_CSV0_D_LIGHTS,
                 parameter);
        break;

    CASE_4(NV097_SET_TEXGEN_S, 16): {
        slot = (method - NV097_SET_TEXGEN_S) / 16;
        unsigned int reg = (slot < 2) ? NV_PGRAPH_CSV1_A
                                      : NV_PGRAPH_CSV1_B;
        unsigned int mask = (slot % 2) ? NV_PGRAPH_CSV1_A_T1_S
                                       : NV_PGRAPH_CSV1_A_T0_S;
        SET_MASK_SLOW(pg->regs[reg], mask, kelvin_map_texgen(parameter, 0));
        break;
    }
    CASE_4(NV097_SET_TEXGEN_T, 16): {
        slot = (method - NV097_SET_TEXGEN_T) / 16;
        unsigned int reg = (slot < 2) ? NV_PGRAPH_CSV1_A
                                      : NV_PGRAPH_CSV1_B;
        unsigned int mask = (slot % 2) ? NV_PGRAPH_CSV1_A_T1_T
                                       : NV_PGRAPH_CSV1_A_T0_T;
        SET_MASK_SLOW(pg->regs[reg], mask, kelvin_map_texgen(parameter, 1));
        break;
    }
    CASE_4(NV097_SET_TEXGEN_R, 16): {
        slot = (method - NV097_SET_TEXGEN_R) / 16;
        unsigned int reg = (slot < 2) ? NV_PGRAPH_CSV1_A
                                      : NV_PGRAPH_CSV1_B;
        unsigned int mask = (slot % 2) ? NV_PGRAPH_CSV1_A_T1_R
                                       : NV_PGRAPH_CSV1_A_T0_R;
        SET_MASK_SLOW(pg->regs[reg], mask, kelvin_map_texgen(parameter, 2));
        break;
    }
    CASE_4(NV097_SET_TEXGEN_Q, 16): {
        slot = (method - NV097_SET_TEXGEN_Q) / 16;
        unsigned int reg = (slot < 2) ? NV_PGRAPH_CSV1_A
                                      : NV_PGRAPH_CSV1_B;
        unsigned int mask = (slot % 2) ? NV_PGRAPH_CSV1_A_T1_Q
                                       : NV_PGRAPH_CSV1_A_T0_Q;
        SET_MASK_SLOW(pg->regs[reg], mask, kelvin_map_texgen(parameter, 3));
        break;
    }
    CASE_4(NV097_SET_TEXTURE_MATRIX_ENABLE,4):
        slot = (method - NV097_SET_TEXTURE_MATRIX_ENABLE) / 4;
        pg->texture_matrix_enable[slot] = parameter;
        break;

    case NV097_SET_PROJECTION_MATRIX ...
            NV097_SET_PROJECTION_MATRIX + 0x3c: {
        slot = (method - NV097_SET_PROJECTION_MATRIX) / 4;
        // pg->projection_matrix[slot] = *(float*)&parameter;
        unsigned int row = NV_IGRAPH_XF_XFCTX_PMAT0 + slot/4;
        pg->vsh_constants[row][slot%4] = parameter;
        pg->vsh_constants_dirty[row] = true;
        break;
    }

    case NV097_SET_MODEL_VIEW_MATRIX ...
            NV097_SET_MODEL_VIEW_MATRIX + 0xfc: {
        slot = (method - NV097_SET_MODEL_VIEW_MATRIX) / 4;
        unsigned int matnum = slot / 16;
        unsigned int entry = slot % 16;
        unsigned int row = NV_IGRAPH_XF_XFCTX_MMAT0 + matnum*8 + entry/4;
        pg->vsh_constants[row][entry % 4] = parameter;
        pg->vsh_constants_dirty[row] = true;
        break;
    }

    case NV097_SET_INVERSE_MODEL_VIEW_MATRIX ...
            NV097_SET_INVERSE_MODEL_VIEW_MATRIX + 0xfc: {
        slot = (method - NV097_SET_INVERSE_MODEL_VIEW_MATRIX) / 4;
        unsigned int matnum = slot / 16;
        unsigned int entry = slot % 16;
        unsigned int row = NV_IGRAPH_XF_XFCTX_IMMAT0 + matnum*8 + entry/4;
        pg->vsh_constants[row][entry % 4] = parameter;
        pg->vsh_constants_dirty[row] = true;
        break;
    }

    case NV097_SET_COMPOSITE_MATRIX ...
            NV097_SET_COMPOSITE_MATRIX + 0x3c: {
        slot = (method - NV097_SET_COMPOSITE_MATRIX) / 4;
        unsigned int row = NV_IGRAPH_XF_XFCTX_CMAT0 + slot/4;
        pg->vsh_constants[row][slot%4] = parameter;
        pg->vsh_constants_dirty[row] = true;
        break;
    }

    case NV097_SET_TEXTURE_MATRIX ...
            NV097_SET_TEXTURE_MATRIX + 0xfc: {
        slot = (method - NV097_SET_TEXTURE_MATRIX) / 4;
        unsigned int tex = slot / 16;
        unsigned int entry = slot % 16;
        unsigned int row = NV_IGRAPH_XF_XFCTX_T0MAT + tex*8 + entry/4;
        pg->vsh_constants[row][entry%4] = parameter;
        pg->vsh_constants_dirty[row] = true;
        break;
    }

    case NV097_SET_FOG_PARAMS ...
            NV097_SET_FOG_PARAMS + 8:
        slot = (method - NV097_SET_FOG_PARAMS) / 4;
        if (slot < 2) {
            pg->regs[NV_PGRAPH_FOGPARAM0 + slot*4] = parameter;
        } else {
            /* FIXME: No idea where slot = 2 is */
        }

        pg->ltctxa[NV_IGRAPH_XF_LTCTXA_FOG_K][slot] = parameter;
        pg->ltctxa_dirty[NV_IGRAPH_XF_LTCTXA_FOG_K] = true;
        break;

    /* Handles NV097_SET_TEXGEN_PLANE_S,T,R,Q */
    case NV097_SET_TEXGEN_PLANE_S ...
            NV097_SET_TEXGEN_PLANE_S + 0xfc: {
        slot = (method - NV097_SET_TEXGEN_PLANE_S) / 4;
        unsigned int tex = slot / 16;
        unsigned int entry = slot % 16;
        unsigned int row = NV_IGRAPH_XF_XFCTX_TG0MAT + tex*8 + entry/4;
        pg->vsh_constants[row][entry%4] = parameter;
        pg->vsh_constants_dirty[row] = true;
        break;
    }

    case NV097_SET_TEXGEN_VIEW_MODEL:
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_D], NV_PGRAPH_CSV0_D_TEXGEN_REF,
                 parameter);
        break;

    case NV097_SET_FOG_PLANE ...
            NV097_SET_FOG_PLANE + 12:
        slot = (method - NV097_SET_FOG_PLANE) / 4;
        pg->vsh_constants[NV_IGRAPH_XF_XFCTX_FOG][slot] = parameter;
        pg->vsh_constants_dirty[NV_IGRAPH_XF_XFCTX_FOG] = true;
        break;

    case NV097_SET_SCENE_AMBIENT_COLOR ...
            NV097_SET_SCENE_AMBIENT_COLOR + 8:
        slot = (method - NV097_SET_SCENE_AMBIENT_COLOR) / 4;
        // ??
        pg->ltctxa[NV_IGRAPH_XF_LTCTXA_FR_AMB][slot] = parameter;
        pg->ltctxa_dirty[NV_IGRAPH_XF_LTCTXA_FR_AMB] = true;
        break;

    case NV097_SET_VIEWPORT_OFFSET ...
            NV097_SET_VIEWPORT_OFFSET + 12:
        slot = (method - NV097_SET_VIEWPORT_OFFSET) / 4;
        pg->vsh_constants[NV_IGRAPH_XF_XFCTX_VPOFF][slot] = parameter;
        pg->vsh_constants_dirty[NV_IGRAPH_XF_XFCTX_VPOFF] = true;
        break;

    case NV097_SET_EYE_POSITION ...
            NV097_SET_EYE_POSITION + 12:
        slot = (method - NV097_SET_EYE_POSITION) / 4;
        pg->vsh_constants[NV_IGRAPH_XF_XFCTX_EYEP][slot] = parameter;
        pg->vsh_constants_dirty[NV_IGRAPH_XF_XFCTX_EYEP] = true;
        break;
    case NV097_SET_COMBINER_FACTOR0 ...
            NV097_SET_COMBINER_FACTOR0 + 28:
        slot = (method - NV097_SET_COMBINER_FACTOR0) / 4;
        pg->regs[NV_PGRAPH_COMBINEFACTOR0 + slot*4] = parameter;
        break;

    case NV097_SET_COMBINER_FACTOR1 ...
            NV097_SET_COMBINER_FACTOR1 + 28:
        slot = (method - NV097_SET_COMBINER_FACTOR1) / 4;
        pg->regs[NV_PGRAPH_COMBINEFACTOR1 + slot*4] = parameter;
        break;

    case NV097_SET_COMBINER_ALPHA_OCW ...
            NV097_SET_COMBINER_ALPHA_OCW + 28:
        slot = (method - NV097_SET_COMBINER_ALPHA_OCW) / 4;
        pg->regs[NV_PGRAPH_COMBINEALPHAO0 + slot*4] = parameter;
        break;

    case NV097_SET_COMBINER_COLOR_ICW ...
            NV097_SET_COMBINER_COLOR_ICW + 28:
        slot = (method - NV097_SET_COMBINER_COLOR_ICW) / 4;
        pg->regs[NV_PGRAPH_COMBINECOLORI0 + slot*4] = parameter;
        break;

    case NV097_SET_VIEWPORT_SCALE ...
            NV097_SET_VIEWPORT_SCALE + 12:
        slot = (method - NV097_SET_VIEWPORT_SCALE) / 4;
        pg->vsh_constants[NV_IGRAPH_XF_XFCTX_VPSCL][slot] = parameter;
        pg->vsh_constants_dirty[NV_IGRAPH_XF_XFCTX_VPSCL] = true;
        break;

    case NV097_SET_TRANSFORM_PROGRAM ...
            NV097_SET_TRANSFORM_PROGRAM + 0x7c: {

        slot = (method - NV097_SET_TRANSFORM_PROGRAM) / 4;

        int program_load = GET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                                    NV_PGRAPH_CHEOPS_OFFSET_PROG_LD_PTR);

        assert(program_load < NV2A_MAX_TRANSFORM_PROGRAM_LENGTH);
        pg->program_data[program_load][slot%4] = parameter;

        if (slot % 4 == 3) {
            SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                     NV_PGRAPH_CHEOPS_OFFSET_PROG_LD_PTR, program_load+1);
        }

        break;
    }

    case NV097_SET_TRANSFORM_CONSTANT ...
            NV097_SET_TRANSFORM_CONSTANT + 0x7c: {

        slot = (method - NV097_SET_TRANSFORM_CONSTANT) / 4;

        int const_load = GET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                                  NV_PGRAPH_CHEOPS_OFFSET_CONST_LD_PTR);

        assert(const_load < NV2A_VERTEXSHADER_CONSTANTS);
        // VertexShaderConstant *constant = &pg->constants[const_load];
        pg->vsh_constants_dirty[const_load] |=
            (parameter != pg->vsh_constants[const_load][slot%4]);
        pg->vsh_constants[const_load][slot%4] = parameter;

        if (slot % 4 == 3) {
            SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                     NV_PGRAPH_CHEOPS_OFFSET_CONST_LD_PTR, const_load+1);
        }
        break;
    }

    case NV097_SET_VERTEX3F ...
            NV097_SET_VERTEX3F + 8: {
        slot = (method - NV097_SET_VERTEX3F) / 4;
        VertexAttribute *attribute =
            &pg->vertex_attributes[NV2A_VERTEX_ATTR_POSITION];
        pgraph_allocate_inline_buffer_vertices(pg, NV2A_VERTEX_ATTR_POSITION);
        attribute->inline_value[slot] = *(float*)&parameter;
        attribute->inline_value[3] = 1.0f;
        if (slot == 2) {
            pgraph_finish_inline_buffer_vertex(pg);
        }
        break;
    }

    /* Handles NV097_SET_BACK_LIGHT_* */
    case NV097_SET_BACK_LIGHT_AMBIENT_COLOR ...
            NV097_SET_BACK_LIGHT_SPECULAR_COLOR + 0x1C8: {
        slot = (method - NV097_SET_BACK_LIGHT_AMBIENT_COLOR) / 4;
        unsigned int part = NV097_SET_BACK_LIGHT_AMBIENT_COLOR / 4 + slot % 16;
        slot /= 16; /* [Light index] */
        assert(slot < 8);
        switch(part * 4) {
        case NV097_SET_BACK_LIGHT_AMBIENT_COLOR ...
                NV097_SET_BACK_LIGHT_AMBIENT_COLOR + 8:
            part -= NV097_SET_BACK_LIGHT_AMBIENT_COLOR / 4;
            pg->ltctxb[NV_IGRAPH_XF_LTCTXB_L0_BAMB + slot*6][part] = parameter;
            pg->ltctxb_dirty[NV_IGRAPH_XF_LTCTXB_L0_BAMB + slot*6] = true;
            break;
        case NV097_SET_BACK_LIGHT_DIFFUSE_COLOR ...
                NV097_SET_BACK_LIGHT_DIFFUSE_COLOR + 8:
            part -= NV097_SET_BACK_LIGHT_DIFFUSE_COLOR / 4;
            pg->ltctxb[NV_IGRAPH_XF_LTCTXB_L0_BDIF + slot*6][part] = parameter;
            pg->ltctxb_dirty[NV_IGRAPH_XF_LTCTXB_L0_BDIF + slot*6] = true;
            break;
        case NV097_SET_BACK_LIGHT_SPECULAR_COLOR ...
                NV097_SET_BACK_LIGHT_SPECULAR_COLOR + 8:
            part -= NV097_SET_BACK_LIGHT_SPECULAR_COLOR / 4;
            pg->ltctxb[NV_IGRAPH_XF_LTCTXB_L0_BSPC + slot*6][part] = parameter;
            pg->ltctxb_dirty[NV_IGRAPH_XF_LTCTXB_L0_BSPC + slot*6] = true;
            break;
        default:
            assert(false);
            break;
        }
        break;
    }
    /* Handles all the light source props except for NV097_SET_BACK_LIGHT_* */
    case NV097_SET_LIGHT_AMBIENT_COLOR ...
            NV097_SET_LIGHT_LOCAL_ATTENUATION + 0x38C: {
        slot = (method - NV097_SET_LIGHT_AMBIENT_COLOR) / 4;
        unsigned int part = NV097_SET_LIGHT_AMBIENT_COLOR / 4 + slot % 32;
        slot /= 32; /* [Light index] */
        assert(slot < 8);
        switch(part * 4) {
        case NV097_SET_LIGHT_AMBIENT_COLOR ...
                NV097_SET_LIGHT_AMBIENT_COLOR + 8:
            part -= NV097_SET_LIGHT_AMBIENT_COLOR / 4;
            pg->ltctxb[NV_IGRAPH_XF_LTCTXB_L0_AMB + slot*6][part] = parameter;
            pg->ltctxb_dirty[NV_IGRAPH_XF_LTCTXB_L0_AMB + slot*6] = true;
            break;
        case NV097_SET_LIGHT_DIFFUSE_COLOR ...
               NV097_SET_LIGHT_DIFFUSE_COLOR + 8:
            part -= NV097_SET_LIGHT_DIFFUSE_COLOR / 4;
            pg->ltctxb[NV_IGRAPH_XF_LTCTXB_L0_DIF + slot*6][part] = parameter;
            pg->ltctxb_dirty[NV_IGRAPH_XF_LTCTXB_L0_DIF + slot*6] = true;
            break;
        case NV097_SET_LIGHT_SPECULAR_COLOR ...
                NV097_SET_LIGHT_SPECULAR_COLOR + 8:
            part -= NV097_SET_LIGHT_SPECULAR_COLOR / 4;
            pg->ltctxb[NV_IGRAPH_XF_LTCTXB_L0_SPC + slot*6][part] = parameter;
            pg->ltctxb_dirty[NV_IGRAPH_XF_LTCTXB_L0_SPC + slot*6] = true;
            break;
        case NV097_SET_LIGHT_LOCAL_RANGE:
            pg->ltc1[NV_IGRAPH_XF_LTC1_r0 + slot][0] = parameter;
            pg->ltc1_dirty[NV_IGRAPH_XF_LTC1_r0 + slot] = true;
            break;
        case NV097_SET_LIGHT_INFINITE_HALF_VECTOR ...
                NV097_SET_LIGHT_INFINITE_HALF_VECTOR + 8:
            part -= NV097_SET_LIGHT_INFINITE_HALF_VECTOR / 4;
            pg->light_infinite_half_vector[slot][part] = *(float*)&parameter;
            break;
        case NV097_SET_LIGHT_INFINITE_DIRECTION ...
                NV097_SET_LIGHT_INFINITE_DIRECTION + 8:
            part -= NV097_SET_LIGHT_INFINITE_DIRECTION / 4;
            pg->light_infinite_direction[slot][part] = *(float*)&parameter;
            break;
        case NV097_SET_LIGHT_SPOT_FALLOFF ...
                NV097_SET_LIGHT_SPOT_FALLOFF + 8:
            part -= NV097_SET_LIGHT_SPOT_FALLOFF / 4;
            pg->ltctxa[NV_IGRAPH_XF_LTCTXA_L0_K + slot*2][part] = parameter;
            pg->ltctxa_dirty[NV_IGRAPH_XF_LTCTXA_L0_K + slot*2] = true;
            break;
        case NV097_SET_LIGHT_SPOT_DIRECTION ...
                NV097_SET_LIGHT_SPOT_DIRECTION + 12:
            part -= NV097_SET_LIGHT_SPOT_DIRECTION / 4;
            pg->ltctxa[NV_IGRAPH_XF_LTCTXA_L0_SPT + slot*2][part] = parameter;
            pg->ltctxa_dirty[NV_IGRAPH_XF_LTCTXA_L0_SPT + slot*2] = true;
            break;
        case NV097_SET_LIGHT_LOCAL_POSITION ...
                NV097_SET_LIGHT_LOCAL_POSITION + 8:
            part -= NV097_SET_LIGHT_LOCAL_POSITION / 4;
            pg->light_local_position[slot][part] = *(float*)&parameter;
            break;
        case NV097_SET_LIGHT_LOCAL_ATTENUATION ...
                NV097_SET_LIGHT_LOCAL_ATTENUATION + 8:
            part -= NV097_SET_LIGHT_LOCAL_ATTENUATION / 4;
            pg->light_local_attenuation[slot][part] = *(float*)&parameter;
            break;
        default:
            assert(false);
            break;
        }
        break;
    }

    case NV097_SET_VERTEX4F ...
            NV097_SET_VERTEX4F + 12: {
        slot = (method - NV097_SET_VERTEX4F) / 4;
        VertexAttribute *attribute =
            &pg->vertex_attributes[NV2A_VERTEX_ATTR_POSITION];
        pgraph_allocate_inline_buffer_vertices(pg, NV2A_VERTEX_ATTR_POSITION);
        attribute->inline_value[slot] = *(float*)&parameter;
        if (slot == 3) {
            pgraph_finish_inline_buffer_vertex(pg);
        }
        break;
    }

    case NV097_SET_VERTEX_DATA_ARRAY_FORMAT ...
            NV097_SET_VERTEX_DATA_ARRAY_FORMAT + 0x3c: {

        slot = (method - NV097_SET_VERTEX_DATA_ARRAY_FORMAT) / 4;
        VertexAttribute *vertex_attribute = &pg->vertex_attributes[slot];

        vertex_attribute->format =
            GET_MASK(parameter, NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE);
        vertex_attribute->count =
            GET_MASK(parameter, NV097_SET_VERTEX_DATA_ARRAY_FORMAT_SIZE);
        vertex_attribute->stride =
            GET_MASK(parameter, NV097_SET_VERTEX_DATA_ARRAY_FORMAT_STRIDE);

        NV2A_DPRINTF("vertex data array format=%d, count=%d, stride=%d\n",
            vertex_attribute->format,
            vertex_attribute->count,
            vertex_attribute->stride);

        vertex_attribute->gl_count = vertex_attribute->count;

        switch (vertex_attribute->format) {
        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_UB_D3D:
            vertex_attribute->gl_type = GL_UNSIGNED_BYTE;
            vertex_attribute->gl_normalize = GL_TRUE;
            vertex_attribute->size = 1;
            assert(vertex_attribute->count == 4);
            // http://www.opengl.org/registry/specs/ARB/vertex_array_bgra.txt
            vertex_attribute->gl_count = GL_BGRA;
            vertex_attribute->needs_conversion = false;
            break;
        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_UB_OGL:
            vertex_attribute->gl_type = GL_UNSIGNED_BYTE;
            vertex_attribute->gl_normalize = GL_TRUE;
            vertex_attribute->size = 1;
            vertex_attribute->needs_conversion = false;
            break;
        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_S1:
            vertex_attribute->gl_type = GL_SHORT;
            vertex_attribute->gl_normalize = GL_TRUE;
            vertex_attribute->size = 2;
            vertex_attribute->needs_conversion = false;
            break;
        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_F:
            vertex_attribute->gl_type = GL_FLOAT;
            vertex_attribute->gl_normalize = GL_FALSE;
            vertex_attribute->size = 4;
            vertex_attribute->needs_conversion = false;
            break;
        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_S32K:
            vertex_attribute->gl_type = GL_SHORT;
            vertex_attribute->gl_normalize = GL_FALSE;
            vertex_attribute->size = 2;
            vertex_attribute->needs_conversion = false;
            break;
        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_CMP:
            /* 3 signed, normalized components packed in 32-bits. (11,11,10) */
            vertex_attribute->size = 4;
            vertex_attribute->gl_type = GL_FLOAT;
            vertex_attribute->gl_normalize = GL_FALSE;
            vertex_attribute->needs_conversion = true;
            vertex_attribute->converted_size = sizeof(float);
            vertex_attribute->converted_count = 3 * vertex_attribute->count;
            break;
        default:
            fprintf(stderr, "Unknown vertex type: 0x%x\n", vertex_attribute->format);
            assert(false);
            break;
        }

        if (vertex_attribute->needs_conversion) {
            vertex_attribute->converted_elements = 0;
        } else {
            if (vertex_attribute->converted_buffer) {
                g_free(vertex_attribute->converted_buffer);
                vertex_attribute->converted_buffer = NULL;
            }
        }

        break;
    }

    case NV097_SET_VERTEX_DATA_ARRAY_OFFSET ...
            NV097_SET_VERTEX_DATA_ARRAY_OFFSET + 0x3c:

        slot = (method - NV097_SET_VERTEX_DATA_ARRAY_OFFSET) / 4;

        pg->vertex_attributes[slot].dma_select =
            parameter & 0x80000000;
        pg->vertex_attributes[slot].offset =
            parameter & 0x7fffffff;

        pg->vertex_attributes[slot].converted_elements = 0;

        break;

    case NV097_SET_LOGIC_OP_ENABLE:
        SET_MASK(pg->regs[NV_PGRAPH_BLEND],
                 NV_PGRAPH_BLEND_LOGICOP_ENABLE, parameter);
        break;

    case NV097_SET_LOGIC_OP:
        SET_MASK(pg->regs[NV_PGRAPH_BLEND],
                 NV_PGRAPH_BLEND_LOGICOP, parameter & 0xF);
        break;

    case NV097_CLEAR_REPORT_VALUE:
        /* FIXME: Does this have a value in parameter? Also does this (also?) modify
         *        the report memory block?
         */
        if (pg->gl_zpass_pixel_count_query_count) {
            // glDeleteQueries(pg->gl_zpass_pixel_count_query_count,
            //                 pg->gl_zpass_pixel_count_queries);
            pg->gl_zpass_pixel_count_query_count = 0;
        }
        pg->zpass_pixel_count_result = 0;
        break;

    case NV097_SET_ZPASS_PIXEL_COUNT_ENABLE:
        pg->zpass_pixel_count_enable = parameter;
        break;

    case NV097_GET_REPORT: {
        /* FIXME: This was first intended to be watchpoint-based. However,
         *        qemu / kvm only supports virtual-address watchpoints.
         *        This'll do for now, but accuracy and performance with other
         *        approaches could be better
         */
        uint8_t type = GET_MASK(parameter, NV097_GET_REPORT_TYPE);
        assert(type == NV097_GET_REPORT_TYPE_ZPASS_PIXEL_CNT);
        hwaddr offset = GET_MASK(parameter, NV097_GET_REPORT_OFFSET);

        uint64_t timestamp = 0x0011223344556677; /* FIXME: Update timestamp?! */
        uint32_t done = 0;

        /* FIXME: Multisampling affects this (both: OGL and Xbox GPU),
         *        not sure if CLEARs also count
         */
        /* FIXME: What about clipping regions etc? */
        for(i = 0; i < pg->gl_zpass_pixel_count_query_count; i++) {
            GLuint gl_query_result = 0;
            // glGetQueryObjectuiv(pg->gl_zpass_pixel_count_queries[i],
            //                     GL_QUERY_RESULT,
            //                     &gl_query_result);
            pg->zpass_pixel_count_result += gl_query_result;
        }
        if (pg->gl_zpass_pixel_count_query_count) {
            // glDeleteQueries(pg->gl_zpass_pixel_count_query_count,
            //                 pg->gl_zpass_pixel_count_queries);
        }
        pg->gl_zpass_pixel_count_query_count = 0;

        hwaddr report_dma_len;
        uint8_t *report_data = (uint8_t*)nv_dma_map(d, pg->dma_report,
                                                    &report_dma_len);
        assert(offset < report_dma_len);
        report_data += offset;

        stq_le_p((uint64_t*)&report_data[0], timestamp);
        stl_le_p((uint32_t*)&report_data[8], pg->zpass_pixel_count_result);
        stl_le_p((uint32_t*)&report_data[12], done);

        break;
    }

    case NV097_SET_EYE_DIRECTION ...
            NV097_SET_EYE_DIRECTION + 8:
        slot = (method - NV097_SET_EYE_DIRECTION) / 4;
        pg->ltctxa[NV_IGRAPH_XF_LTCTXA_EYED][slot] = parameter;
        pg->ltctxa_dirty[NV_IGRAPH_XF_LTCTXA_EYED] = true;
        break;

    case NV097_SET_BEGIN_END: {
        bool depth_test =
            pg->regs[NV_PGRAPH_CONTROL_0] & NV_PGRAPH_CONTROL_0_ZENABLE;
        bool stencil_test = pg->regs[NV_PGRAPH_CONTROL_1]
                                & NV_PGRAPH_CONTROL_1_STENCIL_TEST_ENABLE;

        if (parameter == NV097_SET_BEGIN_END_OP_END) {

            assert(pg->vertex_shader_binding);
            assert(pg->fragment_shader_binding);

            if (pg->draw_arrays_length) {

                NV2A_GL_DPRINTF(false, "Draw Arrays");

                assert(pg->inline_buffer_length == 0);
                assert(pg->inline_array_length == 0);
                assert(pg->inline_elements_length == 0);
                pgraph_bind_vertex_attributes(d, pg->draw_arrays_max_count,
                                              false, 0);
                glMultiDrawArrays(pg->vertex_shader_binding->gl_primitive_mode,
                                  pg->gl_draw_arrays_start,
                                  pg->gl_draw_arrays_count,
                                  pg->draw_arrays_length);
            } else if (pg->inline_buffer_length) {

                NV2A_GL_DPRINTF(false, "Inline Buffer");

                assert(pg->draw_arrays_length == 0);
                assert(pg->inline_array_length == 0);
                assert(pg->inline_elements_length == 0);

                for (i = 0; i < NV2A_VERTEXSHADER_ATTRIBUTES; i++) {
                    VertexAttribute *attribute = &pg->vertex_attributes[i];

                    if (attribute->inline_buffer) {

#if USE_GEOMETRY_CACHE
                        size_t len = pg->inline_buffer_length * sizeof(float) * 4;
                        uint64_t geom_hash = fast_hash((const unsigned char *)attribute->inline_buffer, len, 0);

                        GeometryKey key_in = {
                            .buffer_type = GL_ARRAY_BUFFER,
                            .buffer_length = len,
                            .populated = 0
                        };

                        struct lru_node *found = lru_lookup(&pg->inline_attribute_buffer_cache, geom_hash, &key_in);
                        GeometryKey *key_out = container_of(found, struct GeometryKey, node);
                        assert(key_out != NULL);
                        glBindBuffer(GL_ARRAY_BUFFER, key_out->buffer_id);
                        SDPRINTF("Uploading inline elements %zd, # %016lx ", pg->inline_buffer_length, geom_hash);
                        if (!key_out->populated) {
                            SDPRINTF("....uploading\n");
                            glBufferData(GL_ARRAY_BUFFER,
                                         pg->inline_buffer_length
                                            * sizeof(float) * 4,
                                         attribute->inline_buffer,
                                         GL_DYNAMIC_DRAW);
                              key_out->populated = 1;
                        } else {
                            SDPRINTF("Re-using buffer!\n");
                        }
#else
                        glBindBuffer(GL_ARRAY_BUFFER,
                                     attribute->gl_inline_buffer);
                        glBufferData(GL_ARRAY_BUFFER,
                                     pg->inline_buffer_length
                                        * sizeof(float) * 4,
                                     attribute->inline_buffer,
                                     GL_DYNAMIC_DRAW);
#endif

                        /* Clear buffer for next batch */
                        g_free(attribute->inline_buffer);
                        attribute->inline_buffer = NULL;

                        glVertexAttribPointer(i, 4, GL_FLOAT, GL_FALSE, 0, 0);
                        glEnableVertexAttribArray(i);
                    } else {
                        glDisableVertexAttribArray(i);

                        glVertexAttrib4fv(i, attribute->inline_value);
                    }

                }

                glDrawArrays(pg->vertex_shader_binding->gl_primitive_mode,
                             0, pg->inline_buffer_length);
            } else if (pg->inline_array_length) {

                NV2A_GL_DPRINTF(false, "Inline Array");

                assert(pg->draw_arrays_length == 0);
                assert(pg->inline_buffer_length == 0);
                assert(pg->inline_elements_length == 0);

                unsigned int index_count = pgraph_bind_inline_array(d);
                glDrawArrays(pg->vertex_shader_binding->gl_primitive_mode,
                             0, index_count);
            } else if (pg->inline_elements_length) {

                NV2A_GL_DPRINTF(false, "Inline Elements");

                assert(pg->draw_arrays_length == 0);
                assert(pg->inline_buffer_length == 0);
                assert(pg->inline_array_length == 0);

                uint32_t max_element = 0;
                uint32_t min_element = (uint32_t)-1;
                for (i=0; i<pg->inline_elements_length; i++) {
                    max_element = MAX(pg->inline_elements[i], max_element);
                    min_element = MIN(pg->inline_elements[i], min_element);
                }

                pgraph_bind_vertex_attributes(d, max_element+1, false, 0);

#if USE_GEOMETRY_CACHE
                uint64_t geom_hash = fast_hash((const unsigned char *)pg->inline_elements, pg->inline_elements_length*4, 0);

                GeometryKey key_in = {
                    .buffer_type = GL_ELEMENT_ARRAY_BUFFER,
                    .buffer_length = pg->inline_elements_length*4,
                    .populated = 0
                };

                struct lru_node *found = lru_lookup(&pg->inline_element_cache, geom_hash, &key_in);
                GeometryKey *key_out = container_of(found, struct GeometryKey, node);
                assert(key_out != NULL);
                glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, key_out->buffer_id);
                SDPRINTF("Uploading inline elements %zd, # %016lx ", pg->inline_elements_length, geom_hash);
                if (!key_out->populated) {
                    SDPRINTF("....uploading\n");
                      glBufferData(GL_ELEMENT_ARRAY_BUFFER,
                                     pg->inline_elements_length*4,
                                     pg->inline_elements,
                                     GL_DYNAMIC_DRAW);
                      key_out->populated = 1;
                } else {
                    SDPRINTF("Re-using buffer!\n");
                }
#else

                glBindBuffer(GL_ELEMENT_ARRAY_BUFFER, pg->gl_element_buffer);
                glBufferData(GL_ELEMENT_ARRAY_BUFFER,
                             pg->inline_elements_length*4,
                             pg->inline_elements,
                             GL_DYNAMIC_DRAW);
#endif
                // SDPRINTF("Uploading inline elements %zd, # %016lx ", pg->inline_elements_length, fast_hash(pg->inline_elements, pg->inline_elements_length*4, 0));

                glDrawRangeElements(pg->vertex_shader_binding->gl_primitive_mode,
                                    min_element, max_element,
                                    pg->inline_elements_length,
                                    GL_UNSIGNED_INT,
                                    (void*)0);

            } else {
                NV2A_GL_DPRINTF(true, "EMPTY NV097_SET_BEGIN_END");
                // assert(false);
            }

            /* End of visibility testing */
            if (pg->zpass_pixel_count_enable) {
                // glEndQuery(GL_SAMPLES_PASSED);
            }

            NV2A_GL_DGROUP_END();
        } else {
            NV2A_GL_DGROUP_BEGIN("NV097_SET_BEGIN_END: 0x%x", parameter);
            assert(parameter <= NV097_SET_BEGIN_END_OP_POLYGON);

            SDPRINTF("NV097_SET_BEGIN_END\n");
            pgraph_update_surface(d, true, true, depth_test || stencil_test);

            pg->primitive_mode = parameter;

            uint32_t control_0 = pg->regs[NV_PGRAPH_CONTROL_0];

            bool alpha = control_0 & NV_PGRAPH_CONTROL_0_ALPHA_WRITE_ENABLE;
            bool red = control_0 & NV_PGRAPH_CONTROL_0_RED_WRITE_ENABLE;
            bool green = control_0 & NV_PGRAPH_CONTROL_0_GREEN_WRITE_ENABLE;
            bool blue = control_0 & NV_PGRAPH_CONTROL_0_BLUE_WRITE_ENABLE;
            glColorMask(red, green, blue, alpha);
            glDepthMask(!!(control_0 & NV_PGRAPH_CONTROL_0_ZWRITEENABLE));
            glStencilMask(GET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                                   NV_PGRAPH_CONTROL_1_STENCIL_MASK_WRITE));

            if (pg->regs[NV_PGRAPH_BLEND] & NV_PGRAPH_BLEND_EN) {
                glEnable(GL_BLEND);
                uint32_t sfactor = GET_MASK(pg->regs[NV_PGRAPH_BLEND],
                                            NV_PGRAPH_BLEND_SFACTOR);
                uint32_t dfactor = GET_MASK(pg->regs[NV_PGRAPH_BLEND],
                                            NV_PGRAPH_BLEND_DFACTOR);
                assert(sfactor < ARRAY_SIZE(pgraph_blend_factor_map));
                assert(dfactor < ARRAY_SIZE(pgraph_blend_factor_map));
                glBlendFunc(pgraph_blend_factor_map[sfactor],
                            pgraph_blend_factor_map[dfactor]);

                uint32_t equation = GET_MASK(pg->regs[NV_PGRAPH_BLEND],
                                             NV_PGRAPH_BLEND_EQN);
                assert(equation < ARRAY_SIZE(pgraph_blend_equation_map));
                glBlendEquation(pgraph_blend_equation_map[equation]);

                uint32_t blend_color = pg->regs[NV_PGRAPH_BLENDCOLOR];
                glBlendColor( ((blend_color >> 16) & 0xFF) / 255.0f, /* red */
                              ((blend_color >> 8) & 0xFF) / 255.0f,  /* green */
                              (blend_color & 0xFF) / 255.0f,         /* blue */
                              ((blend_color >> 24) & 0xFF) / 255.0f);/* alpha */
            } else {
                glDisable(GL_BLEND);
            }

            /* Face culling */
            if (pg->regs[NV_PGRAPH_SETUPRASTER]
                    & NV_PGRAPH_SETUPRASTER_CULLENABLE) {
                uint32_t cull_face = GET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                                              NV_PGRAPH_SETUPRASTER_CULLCTRL);
                assert(cull_face < ARRAY_SIZE(pgraph_cull_face_map));
                glCullFace(pgraph_cull_face_map[cull_face]);
                glEnable(GL_CULL_FACE);
            } else {
                glDisable(GL_CULL_FACE);
            }

            /* Front-face select */
            glFrontFace(pg->regs[NV_PGRAPH_SETUPRASTER]
                            & NV_PGRAPH_SETUPRASTER_FRONTFACE
                                ? GL_CCW : GL_CW);

            /* Polygon offset */
            /* FIXME: GL implementation-specific, maybe do this in VS? */
            if (pg->regs[NV_PGRAPH_SETUPRASTER] &
                    NV_PGRAPH_SETUPRASTER_POFFSETFILLENABLE) {
                glEnable(GL_POLYGON_OFFSET_FILL);
            } else {
                glDisable(GL_POLYGON_OFFSET_FILL);
            }
            if (pg->regs[NV_PGRAPH_SETUPRASTER] &
                    NV_PGRAPH_SETUPRASTER_POFFSETLINEENABLE) {
                glEnable(GL_POLYGON_OFFSET_LINE);
            } else {
                glDisable(GL_POLYGON_OFFSET_LINE);
            }
            if (pg->regs[NV_PGRAPH_SETUPRASTER] &
                    NV_PGRAPH_SETUPRASTER_POFFSETPOINTENABLE) {
                glEnable(GL_POLYGON_OFFSET_POINT);
            } else {
                glDisable(GL_POLYGON_OFFSET_POINT);
            }
            if (pg->regs[NV_PGRAPH_SETUPRASTER] &
                    (NV_PGRAPH_SETUPRASTER_POFFSETFILLENABLE |
                     NV_PGRAPH_SETUPRASTER_POFFSETLINEENABLE |
                     NV_PGRAPH_SETUPRASTER_POFFSETPOINTENABLE)) {
                GLfloat zfactor = *(float*)&pg->regs[NV_PGRAPH_ZOFFSETFACTOR];
                GLfloat zbias = *(float*)&pg->regs[NV_PGRAPH_ZOFFSETBIAS];
                glPolygonOffset(zfactor, zbias);
            }

            /* Depth testing */
            if (depth_test) {
                glEnable(GL_DEPTH_TEST);

                uint32_t depth_func = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                                               NV_PGRAPH_CONTROL_0_ZFUNC);
                assert(depth_func < ARRAY_SIZE(pgraph_depth_func_map));
                glDepthFunc(pgraph_depth_func_map[depth_func]);
            } else {
                glDisable(GL_DEPTH_TEST);
            }

            if (stencil_test) {
                glEnable(GL_STENCIL_TEST);

                uint32_t stencil_func = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                                            NV_PGRAPH_CONTROL_1_STENCIL_FUNC);
                uint32_t stencil_ref = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                                            NV_PGRAPH_CONTROL_1_STENCIL_REF);
                uint32_t func_mask = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_1],
                                        NV_PGRAPH_CONTROL_1_STENCIL_MASK_READ);
                uint32_t op_fail = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_2],
                                        NV_PGRAPH_CONTROL_2_STENCIL_OP_FAIL);
                uint32_t op_zfail = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_2],
                                        NV_PGRAPH_CONTROL_2_STENCIL_OP_ZFAIL);
                uint32_t op_zpass = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_2],
                                        NV_PGRAPH_CONTROL_2_STENCIL_OP_ZPASS);

                assert(stencil_func < ARRAY_SIZE(pgraph_stencil_func_map));
                assert(op_fail < ARRAY_SIZE(pgraph_stencil_op_map));
                assert(op_zfail < ARRAY_SIZE(pgraph_stencil_op_map));
                assert(op_zpass < ARRAY_SIZE(pgraph_stencil_op_map));

                glStencilFunc(
                    pgraph_stencil_func_map[stencil_func],
                    stencil_ref,
                    func_mask);

                glStencilOp(
                    pgraph_stencil_op_map[op_fail],
                    pgraph_stencil_op_map[op_zfail],
                    pgraph_stencil_op_map[op_zpass]);

            } else {
                glDisable(GL_STENCIL_TEST);
            }

            /* Dither */
            /* FIXME: GL implementation dependent */
            if (pg->regs[NV_PGRAPH_CONTROL_0] &
                    NV_PGRAPH_CONTROL_0_DITHERENABLE) {
                glEnable(GL_DITHER);
            } else {
                glDisable(GL_DITHER);
            }

            pgraph_bind_shaders(pg);
            pgraph_bind_textures(d);

            //glDisableVertexAttribArray(NV2A_VERTEX_ATTR_DIFFUSE);
            //glVertexAttrib4f(NV2A_VERTEX_ATTR_DIFFUSE, 1.0, 1.0, 1.0, 1.0);


            unsigned int width, height;
            pgraph_get_surface_dimensions(pg, &width, &height);
            pgraph_apply_anti_aliasing_factor(pg, &width, &height);

#if RES_SCALE_FACTOR != 1
            glViewport(0, 0, width*RES_SCALE_FACTOR, height*RES_SCALE_FACTOR);
#else
            glViewport(0, 0, width, height);
#endif
            pg->inline_elements_length = 0;
            pg->inline_array_length = 0;
            pg->inline_buffer_length = 0;
            pg->draw_arrays_length = 0;
            pg->draw_arrays_max_count = 0;

            /* Visibility testing */
            if (pg->zpass_pixel_count_enable) {
                GLuint gl_query = 0;
                // glGenQueries(1, &gl_query);
                pg->gl_zpass_pixel_count_query_count++;
                pg->gl_zpass_pixel_count_queries = (GLuint*)g_realloc(
                    pg->gl_zpass_pixel_count_queries,
                    sizeof(GLuint) * pg->gl_zpass_pixel_count_query_count);
                pg->gl_zpass_pixel_count_queries[
                    pg->gl_zpass_pixel_count_query_count - 1] = gl_query;
                // glBeginQuery(GL_SAMPLES_PASSED, gl_query);
            }
        }

        pgraph_set_surface_dirty(pg, true, depth_test || stencil_test);
        break;
    }
    CASE_4(NV097_SET_TEXTURE_OFFSET, 64):
        slot = (method - NV097_SET_TEXTURE_OFFSET) / 64;
        pg->regs[NV_PGRAPH_TEXOFFSET0 + slot * 4] = parameter;
        pg->texture_dirty[slot] = true;
        break;
    CASE_4(NV097_SET_TEXTURE_FORMAT, 64): {
        slot = (method - NV097_SET_TEXTURE_FORMAT) / 64;

        bool dma_select =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_CONTEXT_DMA) == 2;
        bool cubemap =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_CUBEMAP_ENABLE);
        unsigned int border_source =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_BORDER_SOURCE);
        unsigned int dimensionality =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_DIMENSIONALITY);
        unsigned int color_format =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_COLOR);
        unsigned int levels =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_MIPMAP_LEVELS);
        unsigned int log_width =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_BASE_SIZE_U);
        unsigned int log_height =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_BASE_SIZE_V);
        unsigned int log_depth =
            GET_MASK(parameter, NV097_SET_TEXTURE_FORMAT_BASE_SIZE_P);

        uint32_t *reg = &pg->regs[NV_PGRAPH_TEXFMT0 + slot * 4];
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_CONTEXT_DMA, dma_select);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_CUBEMAPENABLE, cubemap);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_BORDER_SOURCE, border_source);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_DIMENSIONALITY, dimensionality);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_COLOR, color_format);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_MIPMAP_LEVELS, levels);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_BASE_SIZE_U, log_width);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_BASE_SIZE_V, log_height);
        SET_MASK(*reg, NV_PGRAPH_TEXFMT0_BASE_SIZE_P, log_depth);

        pg->texture_dirty[slot] = true;
        break;
    }
    CASE_4(NV097_SET_TEXTURE_CONTROL0, 64):
        slot = (method - NV097_SET_TEXTURE_CONTROL0) / 64;
        pg->regs[NV_PGRAPH_TEXCTL0_0 + slot*4] = parameter;
        break;
    CASE_4(NV097_SET_TEXTURE_CONTROL1, 64):
        slot = (method - NV097_SET_TEXTURE_CONTROL1) / 64;
        pg->regs[NV_PGRAPH_TEXCTL1_0 + slot*4] = parameter;
        break;
    CASE_4(NV097_SET_TEXTURE_FILTER, 64):
        slot = (method - NV097_SET_TEXTURE_FILTER) / 64;
        pg->regs[NV_PGRAPH_TEXFILTER0 + slot * 4] = parameter;
        break;
    CASE_4(NV097_SET_TEXTURE_IMAGE_RECT, 64):
        slot = (method - NV097_SET_TEXTURE_IMAGE_RECT) / 64;
        pg->regs[NV_PGRAPH_TEXIMAGERECT0 + slot * 4] = parameter;
        pg->texture_dirty[slot] = true;
        break;
    CASE_4(NV097_SET_TEXTURE_PALETTE, 64): {
        slot = (method - NV097_SET_TEXTURE_PALETTE) / 64;

        bool dma_select =
            GET_MASK(parameter, NV097_SET_TEXTURE_PALETTE_CONTEXT_DMA) == 1;
        unsigned int length =
            GET_MASK(parameter, NV097_SET_TEXTURE_PALETTE_LENGTH);
        unsigned int offset =
            GET_MASK(parameter, NV097_SET_TEXTURE_PALETTE_OFFSET);

        uint32_t *reg = &pg->regs[NV_PGRAPH_TEXPALETTE0 + slot * 4];
        SET_MASK(*reg, NV_PGRAPH_TEXPALETTE0_CONTEXT_DMA, dma_select);
        SET_MASK(*reg, NV_PGRAPH_TEXPALETTE0_LENGTH, length);
        SET_MASK(*reg, NV_PGRAPH_TEXPALETTE0_OFFSET, offset);

        pg->texture_dirty[slot] = true;
        break;
    }

    CASE_4(NV097_SET_TEXTURE_BORDER_COLOR, 64):
        slot = (method - NV097_SET_TEXTURE_BORDER_COLOR) / 64;
        pg->regs[NV_PGRAPH_BORDERCOLOR0 + slot * 4] = parameter;
        break;
    CASE_4(NV097_SET_TEXTURE_SET_BUMP_ENV_MAT + 0x0, 64):
    CASE_4(NV097_SET_TEXTURE_SET_BUMP_ENV_MAT + 0x4, 64):
    CASE_4(NV097_SET_TEXTURE_SET_BUMP_ENV_MAT + 0x8, 64):
    CASE_4(NV097_SET_TEXTURE_SET_BUMP_ENV_MAT + 0xc, 64):
        slot = (method - NV097_SET_TEXTURE_SET_BUMP_ENV_MAT) / 4;
        assert((slot / 16) > 0);
        slot -= 16;
        pg->bump_env_matrix[slot / 16][slot % 4] = *(float*)&parameter;
        break;

    CASE_4(NV097_SET_TEXTURE_SET_BUMP_ENV_SCALE, 64):
        slot = (method - NV097_SET_TEXTURE_SET_BUMP_ENV_SCALE) / 64;
        assert(slot > 0);
        slot--;
        pg->regs[NV_PGRAPH_BUMPSCALE1 + slot * 4] = parameter;
        break;
    CASE_4(NV097_SET_TEXTURE_SET_BUMP_ENV_OFFSET, 64):
        slot = (method - NV097_SET_TEXTURE_SET_BUMP_ENV_OFFSET) / 64;
        assert(slot > 0);
        slot--;
        pg->regs[NV_PGRAPH_BUMPOFFSET1 + slot * 4] = parameter;
        break;

    case NV097_ARRAY_ELEMENT16:
        assert(pg->inline_elements_length < NV2A_MAX_BATCH_LENGTH);
        pg->inline_elements[
            pg->inline_elements_length++] = parameter & 0xFFFF;
        pg->inline_elements[
            pg->inline_elements_length++] = parameter >> 16;
        break;
    case NV097_ARRAY_ELEMENT32:
        assert(pg->inline_elements_length < NV2A_MAX_BATCH_LENGTH);
        pg->inline_elements[
            pg->inline_elements_length++] = parameter;
        break;
    case NV097_DRAW_ARRAYS: {

        unsigned int start = GET_MASK(parameter, NV097_DRAW_ARRAYS_START_INDEX);
        unsigned int count = GET_MASK(parameter, NV097_DRAW_ARRAYS_COUNT)+1;

        pg->draw_arrays_max_count = MAX(pg->draw_arrays_max_count, start + count);

        assert(pg->draw_arrays_length < ARRAY_SIZE(pg->gl_draw_arrays_start));

        /* Attempt to connect primitives */
        if (pg->draw_arrays_length > 0) {
            unsigned int last_start =
                pg->gl_draw_arrays_start[pg->draw_arrays_length - 1];
            GLsizei* last_count =
                &pg->gl_draw_arrays_count[pg->draw_arrays_length - 1];
            if (start == (last_start + *last_count)) {
                *last_count += count;
                break;
            }
        }

        pg->gl_draw_arrays_start[pg->draw_arrays_length] = start;
        pg->gl_draw_arrays_count[pg->draw_arrays_length] = count;
        pg->draw_arrays_length++;
        break;
    }
    case NV097_INLINE_ARRAY:
        assert(pg->inline_array_length < NV2A_MAX_BATCH_LENGTH);
        pg->inline_array[
            pg->inline_array_length++] = parameter;
        break;
    case NV097_SET_EYE_VECTOR ...
            NV097_SET_EYE_VECTOR + 8:
        slot = (method - NV097_SET_EYE_VECTOR) / 4;
        pg->regs[NV_PGRAPH_EYEVEC0 + slot * 4] = parameter;
        break;

    case NV097_SET_VERTEX_DATA2F_M ...
            NV097_SET_VERTEX_DATA2F_M + 0x7c: {
        slot = (method - NV097_SET_VERTEX_DATA2F_M) / 4;
        unsigned int part = slot % 2;
        slot /= 2;
        VertexAttribute *attribute = &pg->vertex_attributes[slot];
        pgraph_allocate_inline_buffer_vertices(pg, slot);
        attribute->inline_value[part] = *(float*)&parameter;
        /* FIXME: Should these really be set to 0.0 and 1.0 ? Conditions? */
        attribute->inline_value[2] = 0.0;
        attribute->inline_value[3] = 1.0;
        if ((slot == 0) && (part == 1)) {
            pgraph_finish_inline_buffer_vertex(pg);
        }
        break;
    }
    case NV097_SET_VERTEX_DATA4F_M ...
            NV097_SET_VERTEX_DATA4F_M + 0xfc: {
        slot = (method - NV097_SET_VERTEX_DATA4F_M) / 4;
        unsigned int part = slot % 4;
        slot /= 4;
        VertexAttribute *attribute = &pg->vertex_attributes[slot];
        pgraph_allocate_inline_buffer_vertices(pg, slot);
        attribute->inline_value[part] = *(float*)&parameter;
        if ((slot == 0) && (part == 3)) {
            pgraph_finish_inline_buffer_vertex(pg);
        }
        break;
    }
    case NV097_SET_VERTEX_DATA2S ...
            NV097_SET_VERTEX_DATA2S + 0x3c: {
        slot = (method - NV097_SET_VERTEX_DATA2S) / 4;
        // assert(false); /* FIXME: Untested! */
        VertexAttribute *attribute = &pg->vertex_attributes[slot];
        pgraph_allocate_inline_buffer_vertices(pg, slot);
        /* FIXME: Is mapping to [-1,+1] correct? */
#if 0
        attribute->inline_value[0] = ((int16_t)(parameter & 0xFFFF) * 2.0 + 1)
                                         / 65535.0;
        attribute->inline_value[1] = ((int16_t)(parameter >> 16) * 2.0 + 1)
                                         / 65535.0;
#else
        // FIXME: Addresses https://github.com/xqemu/xqemu/issues/165, needs PR
        int16_t low = (int16_t)(parameter & 0xFFFF);
        int16_t high = (int16_t)(parameter >> 16);

        attribute->inline_value[0] = (float)low;
        attribute->inline_value[1] = (float)high;
#endif
        /* FIXME: Should these really be set to 0.0 and 1.0 ? Conditions? */
        attribute->inline_value[2] = 0.0;
        attribute->inline_value[3] = 1.0;
        if (slot == 0) {
            pgraph_finish_inline_buffer_vertex(pg);
            // assert(false); /* FIXME: Untested */
        }
        break;
    }
    case NV097_SET_VERTEX_DATA4UB ...
            NV097_SET_VERTEX_DATA4UB + 0x3c: {
        slot = (method - NV097_SET_VERTEX_DATA4UB) / 4;
        VertexAttribute *attribute = &pg->vertex_attributes[slot];
        pgraph_allocate_inline_buffer_vertices(pg, slot);
        attribute->inline_value[0] = (parameter & 0xFF) / 255.0;
        attribute->inline_value[1] = ((parameter >> 8) & 0xFF) / 255.0;
        attribute->inline_value[2] = ((parameter >> 16) & 0xFF) / 255.0;
        attribute->inline_value[3] = ((parameter >> 24) & 0xFF) / 255.0;
        if (slot == 0) {
            pgraph_finish_inline_buffer_vertex(pg);
            assert(false); /* FIXME: Untested */
        }
        break;
    }
    case NV097_SET_VERTEX_DATA4S_M ...
            NV097_SET_VERTEX_DATA4S_M + 0x7c: {
        slot = (method - NV097_SET_VERTEX_DATA4S_M) / 4;
        unsigned int part = slot % 2;
        slot /= 2;
        assert(false); /* FIXME: Untested! */
        VertexAttribute *attribute = &pg->vertex_attributes[slot];
        pgraph_allocate_inline_buffer_vertices(pg, slot);
        /* FIXME: Is mapping to [-1,+1] correct? */
        attribute->inline_value[part * 2 + 0] = ((int16_t)(parameter & 0xFFFF)
                                                     * 2.0 + 1) / 65535.0;
        attribute->inline_value[part * 2 + 1] = ((int16_t)(parameter >> 16)
                                                     * 2.0 + 1) / 65535.0;
        if ((slot == 0) && (part == 1)) {
            pgraph_finish_inline_buffer_vertex(pg);
            assert(false); /* FIXME: Untested */
        }
        break;
    }

    case NV097_SET_SEMAPHORE_OFFSET:
        pg->regs[NV_PGRAPH_SEMAPHOREOFFSET] = parameter;
        break;
    case NV097_BACK_END_WRITE_SEMAPHORE_RELEASE: {

        SDPRINTF("NV097_BACK_END_WRITE_SEMAPHORE_RELEASE\n");
        pgraph_update_surface(d, false, true, true);

        //qemu_mutex_unlock(&d->pgraph.lock);
        //qemu_mutex_lock_iothread();

        uint32_t semaphore_offset = pg->regs[NV_PGRAPH_SEMAPHOREOFFSET];

        hwaddr semaphore_dma_len;
        uint8_t *semaphore_data = (uint8_t*)nv_dma_map(d, pg->dma_semaphore,
                                                       &semaphore_dma_len);
        assert(semaphore_offset < semaphore_dma_len);
        semaphore_data += semaphore_offset;

        stl_le_p((uint32_t*)semaphore_data, parameter);

        //qemu_mutex_lock(&d->pgraph.lock);
        //qemu_mutex_unlock_iothread();

        break;
    }
    case NV097_SET_ZSTENCIL_CLEAR_VALUE:
        pg->regs[NV_PGRAPH_ZSTENCILCLEARVALUE] = parameter;
        break;

    case NV097_SET_COLOR_CLEAR_VALUE:
        pg->regs[NV_PGRAPH_COLORCLEARVALUE] = parameter;
        break;

    case NV097_CLEAR_SURFACE: {
        NV2A_DPRINTF("---------PRE CLEAR ------\n");
        GLbitfield gl_mask = 0;

        bool write_color = (parameter & NV097_CLEAR_SURFACE_COLOR);
        bool write_zeta =
            (parameter & (NV097_CLEAR_SURFACE_Z | NV097_CLEAR_SURFACE_STENCIL));

        if (write_zeta) {
            uint32_t clear_zstencil =
                d->pgraph.regs[NV_PGRAPH_ZSTENCILCLEARVALUE];
            GLint gl_clear_stencil;
            GLfloat gl_clear_depth;

            /* FIXME: Put these in some lookup table */
            const float f16_max = 511.9375f;
            /* FIXME: 7 bits of mantissa unused. maybe use full buffer? */
            const float f24_max = 3.4027977E38;

            switch(pg->surface_shape.zeta_format) {
            case NV097_SET_SURFACE_FORMAT_ZETA_Z16: {
                uint16_t z = clear_zstencil & 0xFFFF;
                /* FIXME: Remove bit for stencil clear? */
                if (pg->surface_shape.z_format) {
                    gl_clear_depth = convert_f16_to_float(z) / f16_max;
                    assert(false); /* FIXME: Untested */
                } else {
                    gl_clear_depth = z / (float)0xFFFF;
                }
                break;
            }
            case NV097_SET_SURFACE_FORMAT_ZETA_Z24S8: {
                gl_clear_stencil = clear_zstencil & 0xFF;
                uint32_t z = clear_zstencil >> 8;
                if (pg->surface_shape.z_format) {
                    gl_clear_depth = convert_f24_to_float(z) / f24_max;
                    assert(false); /* FIXME: Untested */
                } else {
                    gl_clear_depth = z / (float)0xFFFFFF;
                }
                break;
            }
            default:
                fprintf(stderr, "Unknown zeta surface format: 0x%x\n", pg->surface_shape.zeta_format);
                assert(false);
                break;
            }
            if (parameter & NV097_CLEAR_SURFACE_Z) {
                gl_mask |= GL_DEPTH_BUFFER_BIT;
                glDepthMask(GL_TRUE);
                glClearDepth(gl_clear_depth);


                // print_timestamp();
                // printf("HALO CLEAR\n");
                // start_frame_timer();

            }
            if (parameter & NV097_CLEAR_SURFACE_STENCIL) {
                gl_mask |= GL_STENCIL_BUFFER_BIT;
                glStencilMask(0xff);
                glClearStencil(gl_clear_stencil);
            }
        }
        if (write_color) {
            gl_mask |= GL_COLOR_BUFFER_BIT;
            glColorMask((parameter & NV097_CLEAR_SURFACE_R)
                             ? GL_TRUE : GL_FALSE,
                        (parameter & NV097_CLEAR_SURFACE_G)
                             ? GL_TRUE : GL_FALSE,
                        (parameter & NV097_CLEAR_SURFACE_B)
                             ? GL_TRUE : GL_FALSE,
                        (parameter & NV097_CLEAR_SURFACE_A)
                             ? GL_TRUE : GL_FALSE);
            uint32_t clear_color = d->pgraph.regs[NV_PGRAPH_COLORCLEARVALUE];

            /* Handle RGB */
            GLfloat red, green, blue;
            switch(pg->surface_shape.color_format) {
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1R5G5B5_Z1R5G5B5:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1R5G5B5_O1R5G5B5:
                red = ((clear_color >> 10) & 0x1F) / 31.0f;
                green = ((clear_color >> 5) & 0x1F) / 31.0f;
                blue = (clear_color & 0x1F) / 31.0f;
                // assert(false); /* Untested */
                break;
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_R5G6B5:
                red = ((clear_color >> 11) & 0x1F) / 31.0f;
                green = ((clear_color >> 5) & 0x3F) / 63.0f;
                blue = (clear_color & 0x1F) / 31.0f;
                break;
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X8R8G8B8_Z8R8G8B8:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X8R8G8B8_O8R8G8B8:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_Z1A7R8G8B8:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_O1A7R8G8B8:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_A8R8G8B8:
                red = ((clear_color >> 16) & 0xFF) / 255.0f;
                green = ((clear_color >> 8) & 0xFF) / 255.0f;
                blue = (clear_color & 0xFF) / 255.0f;
                break;
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_B8:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_G8B8:
                /* Xbox D3D doesn't support clearing those */
            default:
                red = 1.0f;
                green = 0.0f;
                blue = 1.0f;
                fprintf(stderr, "CLEAR_SURFACE for color_format 0x%x unsupported",
                        pg->surface_shape.color_format);
                assert(false);
                break;
            }

            /* Handle alpha */
            GLfloat alpha;
            switch(pg->surface_shape.color_format) {
            /* FIXME: CLEAR_SURFACE seems to work like memset, so maybe we
             *        also have to clear non-alpha bits with alpha value?
             *        As GL doesn't own those pixels we'd have to do this on
             *        our own in xbox memory.
             */
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_Z1A7R8G8B8:
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_X1A7R8G8B8_O1A7R8G8B8:
                alpha = ((clear_color >> 24) & 0x7F) / 127.0f;
                assert(false); /* Untested */
                break;
            case NV097_SET_SURFACE_FORMAT_COLOR_LE_A8R8G8B8:
                alpha = ((clear_color >> 24) & 0xFF) / 255.0f;
                break;
            default:
                alpha = 1.0f;
                break;
            }

            glClearColor(red, green, blue, alpha);
        }

        SDPRINTF("NV097_CLEAR_SURFACE\n");
        pgraph_update_surface(d, true, write_color, write_zeta);

        glEnable(GL_SCISSOR_TEST);

        unsigned int xmin = GET_MASK(pg->regs[NV_PGRAPH_CLEARRECTX],
                NV_PGRAPH_CLEARRECTX_XMIN);
        unsigned int xmax = GET_MASK(pg->regs[NV_PGRAPH_CLEARRECTX],
                NV_PGRAPH_CLEARRECTX_XMAX);
        unsigned int ymin = GET_MASK(pg->regs[NV_PGRAPH_CLEARRECTY],
                NV_PGRAPH_CLEARRECTY_YMIN);
        unsigned int ymax = GET_MASK(pg->regs[NV_PGRAPH_CLEARRECTY],
                NV_PGRAPH_CLEARRECTY_YMAX);

        unsigned int scissor_x = xmin;
        unsigned int scissor_y = pg->surface_shape.clip_height - ymax - 1;

        unsigned int scissor_width = xmax - xmin + 1;
        unsigned int scissor_height = ymax - ymin + 1;

        pgraph_apply_anti_aliasing_factor(pg, &scissor_x, &scissor_y);
        pgraph_apply_anti_aliasing_factor(pg, &scissor_width, &scissor_height);

        /* FIXME: Should this really be inverted instead of ymin? */
#if RES_SCALE_FACTOR != 1
        scissor_width *= RES_SCALE_FACTOR;
        scissor_height *= RES_SCALE_FACTOR;
        scissor_x *= RES_SCALE_FACTOR;
        scissor_y *= RES_SCALE_FACTOR;
#endif
        glScissor(scissor_x, scissor_y, scissor_width, scissor_height);

        /* FIXME: Respect window clip?!?! */

        NV2A_DPRINTF("------------------CLEAR 0x%x %d,%d - %d,%d  %x---------------\n",
            parameter, xmin, ymin, xmax, ymax, d->pgraph.regs[NV_PGRAPH_COLORCLEARVALUE]);

        // memory_global_dirty_log_sync();

        /* Dither */
        /* FIXME: Maybe also disable it here? + GL implementation dependent */
        if (pg->regs[NV_PGRAPH_CONTROL_0] &
                NV_PGRAPH_CONTROL_0_DITHERENABLE) {
            glEnable(GL_DITHER);
        } else {
            glDisable(GL_DITHER);
        }

        glClear(gl_mask);


            // Safe for dash?
                // printf("HALO CLEAR\n");
                start_frame_timer();


        glDisable(GL_SCISSOR_TEST);

        pgraph_set_surface_dirty(pg, write_color, write_zeta);
        break;
    }

    case NV097_SET_CLEAR_RECT_HORIZONTAL:
        pg->regs[NV_PGRAPH_CLEARRECTX] = parameter;
        break;
    case NV097_SET_CLEAR_RECT_VERTICAL:
        pg->regs[NV_PGRAPH_CLEARRECTY] = parameter;
        break;

    case NV097_SET_SPECULAR_FOG_FACTOR ...
            NV097_SET_SPECULAR_FOG_FACTOR + 4:
        slot = (method - NV097_SET_SPECULAR_FOG_FACTOR) / 4;
        pg->regs[NV_PGRAPH_SPECFOGFACTOR0 + slot*4] = parameter;
        break;

    case NV097_SET_SHADER_CLIP_PLANE_MODE:
        pg->regs[NV_PGRAPH_SHADERCLIPMODE] = parameter;
        break;

    case NV097_SET_COMBINER_COLOR_OCW ...
            NV097_SET_COMBINER_COLOR_OCW + 28:
        slot = (method - NV097_SET_COMBINER_COLOR_OCW) / 4;
        pg->regs[NV_PGRAPH_COMBINECOLORO0 + slot*4] = parameter;
        break;

    case NV097_SET_COMBINER_CONTROL:
        pg->regs[NV_PGRAPH_COMBINECTL] = parameter;
        break;

    case NV097_SET_SHADOW_ZSLOPE_THRESHOLD:
        pg->regs[NV_PGRAPH_SHADOWZSLOPETHRESHOLD] = parameter;
        assert(parameter == 0x7F800000); /* FIXME: Unimplemented */
        break;

    case NV097_SET_SHADER_STAGE_PROGRAM:
        pg->regs[NV_PGRAPH_SHADERPROG] = parameter;
        break;

    case NV097_SET_SHADER_OTHER_STAGE_INPUT:
        pg->regs[NV_PGRAPH_SHADERCTL] = parameter;
        break;

    case NV097_SET_TRANSFORM_EXECUTION_MODE:
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_D], NV_PGRAPH_CSV0_D_MODE,
                 GET_MASK(parameter,
                          NV097_SET_TRANSFORM_EXECUTION_MODE_MODE));
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_D], NV_PGRAPH_CSV0_D_RANGE_MODE,
                 GET_MASK(parameter,
                          NV097_SET_TRANSFORM_EXECUTION_MODE_RANGE_MODE));
        break;
    case NV097_SET_TRANSFORM_PROGRAM_CXT_WRITE_EN:
        pg->enable_vertex_program_write = parameter;
        break;
    case NV097_SET_TRANSFORM_PROGRAM_LOAD:
        assert(parameter < NV2A_MAX_TRANSFORM_PROGRAM_LENGTH);
        SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                 NV_PGRAPH_CHEOPS_OFFSET_PROG_LD_PTR, parameter);
        break;
    case NV097_SET_TRANSFORM_PROGRAM_START:
        assert(parameter < NV2A_MAX_TRANSFORM_PROGRAM_LENGTH);
        SET_MASK(pg->regs[NV_PGRAPH_CSV0_C],
                 NV_PGRAPH_CSV0_C_CHEOPS_PROGRAM_START, parameter);
        break;
    case NV097_SET_TRANSFORM_CONSTANT_LOAD:
        assert(parameter < NV2A_VERTEXSHADER_CONSTANTS);
        SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                 NV_PGRAPH_CHEOPS_OFFSET_CONST_LD_PTR, parameter);
        NV2A_DPRINTF("load to %d\n", parameter);
        break;

    default:
        NV2A_GL_DPRINTF(true, "    unhandled  (0x%02x 0x%08x)",
                        graphics_class, method);
        break;
    } break; }

    default:
        NV2A_GL_DPRINTF(true, "    unhandled  (0x%02x 0x%08x)",
                        graphics_class, method);
        break;

    }
}

static void pgraph_context_switch(NV2AState *d, unsigned int channel_id)
{
    bool channel_valid =
        d->pgraph.regs[NV_PGRAPH_CTX_CONTROL] & NV_PGRAPH_CTX_CONTROL_CHID;
    unsigned pgraph_channel_id = GET_MASK(d->pgraph.regs[NV_PGRAPH_CTX_USER], NV_PGRAPH_CTX_USER_CHID);

    bool valid = channel_valid && pgraph_channel_id == channel_id;
    if (!valid) {
        SET_MASK(d->pgraph.regs[NV_PGRAPH_TRAPPED_ADDR],
                 NV_PGRAPH_TRAPPED_ADDR_CHID, channel_id);

        NV2A_DPRINTF("pgraph switching to ch %d\n", channel_id);

        /* TODO: hardware context switching */
        assert(!(d->pgraph.regs[NV_PGRAPH_DEBUG_3]
                & NV_PGRAPH_DEBUG_3_HW_CONTEXT_SWITCH));

#if !USE_COROUTINES
        qemu_mutex_unlock(&d->pgraph.lock);
#endif
        qemu_mutex_lock_iothread();
        SDPRINTF("context switch setting interrupt\n");
        d->pgraph.pending_interrupts |= NV_PGRAPH_INTR_CONTEXT_SWITCH;
        update_irq(d);

#if !USE_COROUTINES
        qemu_mutex_lock(&d->pgraph.lock);
#endif
        qemu_mutex_unlock_iothread();

        // wait for the interrupt to be serviced
        while (d->pgraph.pending_interrupts & NV_PGRAPH_INTR_CONTEXT_SWITCH) {
#if USE_COROUTINES
            qemu_coroutine_yield();
#else
            qemu_cond_wait(&d->pgraph.interrupt_cond, &d->pgraph.lock);
#endif
        }
    }
}

static void pgraph_wait_fifo_access(NV2AState *d) {
    while (!(d->pgraph.regs[NV_PGRAPH_FIFO] & NV_PGRAPH_FIFO_ACCESS)) {
#if USE_COROUTINES
        // printf("waiting for fifo_access_cond\n");
        // while (!fifo_access_cond) {
            qemu_coroutine_yield();
        // }
#else
        qemu_cond_wait(&d->pgraph.fifo_access_cond, &d->pgraph.lock);
#endif
    }
}

// static const char* nv2a_method_names[] = {};

static void pgraph_method_log(unsigned int subchannel,
                              unsigned int graphics_class,
                              unsigned int method, uint32_t parameter) {
    static unsigned int last = 0;
    static unsigned int count = 0;
    if (last == 0x1800 && method != last) {
        NV2A_GL_DPRINTF(true, "pgraph method (%d) 0x%x * %d",
                     subchannel, last, count);
    }
    if (method != 0x1800) {
        // const char* method_name = NULL;
        // unsigned int nmethod = 0;
        // switch (graphics_class) {
        //     case NV_KELVIN_PRIMITIVE:
        //         nmethod = method | (0x5c << 16);
        //         break;
        //     case NV_CONTEXT_SURFACES_2D:
        //         nmethod = method | (0x6d << 16);
        //         break;
        //     case NV_CONTEXT_PATTERN:
        //         nmethod = method | (0x68 << 16);
        //         break;
        //     default:
        //         break;
        // }
        // if (nmethod != 0 && nmethod < ARRAY_SIZE(nv2a_method_names)) {
        //     method_name = nv2a_method_names[nmethod];
        // }
        // if (method_name) {
        //     NV2A_DPRINTF("pgraph method (%d): %s (0x%x)\n",
        //                  subchannel, method_name, parameter);
        // } else {
            NV2A_DPRINTF("pgraph method (%d): 0x%x -> 0x%04x (0x%x)\n",
                         subchannel, graphics_class, method, parameter);
        // }

    }
    if (method == last) { count++; }
    else {count = 0; }
    last = method;
}

static void pgraph_allocate_inline_buffer_vertices(PGRAPHState *pg,
                                                   unsigned int attr)
{
    int i;
    VertexAttribute *attribute = &pg->vertex_attributes[attr];

    if (attribute->inline_buffer || pg->inline_buffer_length == 0) {
        return;
    }

    /* Now upload the previous attribute value */
    attribute->inline_buffer = (float*)g_malloc(NV2A_MAX_BATCH_LENGTH
                                                  * sizeof(float) * 4);
    for (i = 0; i < pg->inline_buffer_length; i++) {
        memcpy(&attribute->inline_buffer[i * 4],
               attribute->inline_value,
               sizeof(float) * 4);
    }
}

static void pgraph_finish_inline_buffer_vertex(PGRAPHState *pg)
{
    int i;

    assert(pg->inline_buffer_length < NV2A_MAX_BATCH_LENGTH);

    for (i = 0; i < NV2A_VERTEXSHADER_ATTRIBUTES; i++) {
        VertexAttribute *attribute = &pg->vertex_attributes[i];
        if (attribute->inline_buffer) {
            memcpy(&attribute->inline_buffer[
                      pg->inline_buffer_length * 4],
                   attribute->inline_value,
                   sizeof(float) * 4);
        }
    }

    pg->inline_buffer_length++;
}

// Via https://rauwendaal.net/2014/06/14/rendering-a-screen-covering-triangle-in-opengl/
static const char *vert_shader_src =
    "#version 150 core\n"
    "void main()\n"
    "{\n"
    "    float x = -1.0 + float((gl_VertexID & 1) << 2);\n"
    "    float y = -1.0 + float((gl_VertexID & 2) << 1);\n"
    "    gl_Position = vec4(x, y, 0, 1);\n"
    "}\n";

static const char *frag_shader_src =
    "#version 150 core\n"
    "out vec4 out_Color;\n"
    "uniform sampler2D tex;\n"
    "uniform usampler2D utex;\n"
    "uniform int is_stencil;\n"
    "uniform int do_flip;\n"
    "void main()\n"
    "{\n"
        "vec2 texCoord = gl_FragCoord.xy/textureSize(tex,0).xy;\n"
        "if (do_flip > 0) texCoord.y = 1.0 - texCoord.y;\n"
        // "if (is_stencil > 0) {\n"
        // "  float val = float(texture(utex, texCoord).r)/255.0;\n"
        // "  out_Color.rgba = vec4(0,0,val,0);\n"
        // "}\n"
        // "else \n"
        // "{ out_Color.rgba = texture(tex, texCoord); }\n"
        "out_Color.rgba = texture(tex, texCoord);\n"
    "}\n";

GLuint texture_bound_location;
GLuint is_stencil_uni;
GLuint utex_loc;
GLuint do_flip;

#if RENDER_TO_TEXTURE
static void pgraph_setup_surface_to_texture(NV2AState *d)
{
#if !RENDER_TO_TEXTURE_COPY
    GLint status;
    char err_buf[512];
    struct PGRAPHState *pg = &d->pgraph;

    glGenVertexArrays(1, &pg->r2t.m_vao);
    glBindVertexArray(pg->r2t.m_vao);

    // Compile vertex shader
    pg->r2t.m_vert_shader = glCreateShader(GL_VERTEX_SHADER);
    glShaderSource(pg->r2t.m_vert_shader, 1, &vert_shader_src, NULL);
    glCompileShader(pg->r2t.m_vert_shader);
    glGetShaderiv(pg->r2t.m_vert_shader, GL_COMPILE_STATUS, &status);
    if (status != GL_TRUE) {
        glGetShaderInfoLog(pg->r2t.m_vert_shader, sizeof(err_buf), NULL, err_buf);
        err_buf[sizeof(err_buf)-1] = '\0';
        fprintf(stderr, "Vertex shader compilation failed: %s\n", err_buf);
        exit(1);
    }

    // Compile fragment shader
    pg->r2t.m_frag_shader = glCreateShader(GL_FRAGMENT_SHADER);
    glShaderSource(pg->r2t.m_frag_shader, 1, &frag_shader_src, NULL);
    // glShaderSource(pg->r2t.m_frag_shader, 1, &frag_txt, NULL);
    glCompileShader(pg->r2t.m_frag_shader);
    glGetShaderiv(pg->r2t.m_frag_shader, GL_COMPILE_STATUS, &status);
    if (status != GL_TRUE) {
        glGetShaderInfoLog(pg->r2t.m_frag_shader, sizeof(err_buf), NULL, err_buf);
        err_buf[sizeof(err_buf)-1] = '\0';
        fprintf(stderr, "Fragment shader compilation failed: %s\n", err_buf);
        exit(1);
    }

    // Link vertex and fragment shaders
    pg->r2t.m_shader_prog = glCreateProgram();
    glAttachShader(pg->r2t.m_shader_prog, pg->r2t.m_vert_shader);
    glAttachShader(pg->r2t.m_shader_prog, pg->r2t.m_frag_shader);
    glBindFragDataLocation(pg->r2t.m_shader_prog, 0, "out_Color");
    glLinkProgram(pg->r2t.m_shader_prog);
    glUseProgram(pg->r2t.m_shader_prog);

    texture_bound_location = glGetUniformLocation(pg->r2t.m_shader_prog, "tex");
    utex_loc = glGetUniformLocation(pg->r2t.m_shader_prog, "utex");
    is_stencil_uni = glGetUniformLocation(pg->r2t.m_shader_prog, "is_stencil");
    do_flip = glGetUniformLocation(pg->r2t.m_shader_prog, "do_flip");

    // Populate an empty vertex buffer
    glGenBuffers(1, &pg->r2t.m_vbo);
    glBindBuffer(GL_ARRAY_BUFFER, pg->r2t.m_vbo);
    glBufferData(GL_ARRAY_BUFFER, 0, NULL, GL_STATIC_DRAW);

    // Generate a framebuffer we can bind the texture to later
    glGenFramebuffers(1, &pg->r2t.copyFb);
#endif
}

static void pgraph_render_surface_to_texture(
    NV2AState *d, GLsync fence,
    GLuint src, GLenum src_format, GLenum src_target,
    GLuint dst, GLenum dst_format, GLenum dst_target,
    int width, int height, int src_zeta, int flip
    )
{
    ColorFormatInfo f = kelvin_color_format_map[dst_format];

#if !RENDER_TO_TEXTURE_COPY
    GLint m_viewport[4];
    GLboolean m_color_mask[4];
    GLboolean m_scissor_test;
    GLboolean m_stencil_test;
    GLboolean m_blend;
    GLboolean m_cull;
    GLboolean m_depth_test;

    // We come in with the final texture unit activated
    GLint m_final_texture_unit = 0;
    glGetIntegerv(GL_ACTIVE_TEXTURE, &m_final_texture_unit);

    glGetIntegerv(GL_VIEWPORT, m_viewport);
    glGetBooleanv(GL_COLOR_WRITEMASK, m_color_mask);
    m_scissor_test = glIsEnabled(GL_SCISSOR_TEST);
    m_stencil_test = glIsEnabled(GL_STENCIL_TEST);
    m_blend = glIsEnabled(GL_BLEND);
    m_cull = glIsEnabled(GL_CULL_FACE);
    m_depth_test = glIsEnabled(GL_DEPTH_TEST);


    // glWaitSync(fence, 0, GL_TIMEOUT_IGNORED);
    // glDeleteSync(fence);

    // Render the surface into the texture
    glBindFramebuffer(GL_FRAMEBUFFER, d->pgraph.r2t.copyFb);

    // Bind destination texture as framebuffer color attachment
    glBindTexture(dst_target, dst);
    // Reallocate space for new texture dimensions
    glTexParameteri(dst_target, GL_TEXTURE_BASE_LEVEL, 0);
    glTexParameteri(dst_target, GL_TEXTURE_MAX_LEVEL, 0);
    glTexParameteri(dst_target, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
    glTexImage2D(dst_target, 0, f.gl_internal_format,
        width, height, 0, f.gl_format, f.gl_type, NULL);
    // glTexParameteri(dst_target, GL_TEXTURE_MAG_FILTER, GL_LINEAR);

    // Attach the texture to the framebuffer
    glFramebufferTexture2D(GL_FRAMEBUFFER, GL_COLOR_ATTACHMENT0, dst_target, dst, 0);
    GLenum DrawBuffers[1] = {GL_COLOR_ATTACHMENT0};
    glDrawBuffers(1, DrawBuffers);

    assert(glCheckFramebufferStatus(GL_FRAMEBUFFER) == GL_FRAMEBUFFER_COMPLETE);

    // Bind the new framebuffer
    // glBindFramebuffer(GL_FRAMEBUFFER, d->pgraph.r2t.copyFb);

    // Set up viewport to prevent clipping
    glViewport(0, 0, width, height);

    glBindTexture(GL_TEXTURE_RECTANGLE, 0);

    // Bind surface as source texture, and a dummy vao for rendering the full screen triangle
    glBindTexture(GL_TEXTURE_2D, src);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_REPEAT);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_REPEAT);

    glBindVertexArray(d->pgraph.r2t.m_vao);
    glBindBuffer(GL_ARRAY_BUFFER, d->pgraph.r2t.m_vbo);
    glUseProgram(d->pgraph.r2t.m_shader_prog);

    // in case of zeta source, probably want the stencil component
    if (src_zeta) {
        glTexParameteri(GL_TEXTURE_2D, GL_DEPTH_STENCIL_TEXTURE_MODE, GL_STENCIL_INDEX);
        // GLenum swizzle_mask[4] = {GL_RED, GL_RED, GL_RED, GL_RED};
        // glTexParameteriv(GL_TEXTURE_2D, GL_TEXTURE_SWIZZLE_RGBA, (const GLint *)swizzle_mask);
    }

    // assign current texture unit
    glProgramUniform1i(d->pgraph.r2t.m_shader_prog, texture_bound_location, m_final_texture_unit-GL_TEXTURE0);
    // glProgramUniform1i(d->pgraph.r2t.m_shader_prog, utex_loc, m_final_texture_unit-GL_TEXTURE0);
    glProgramUniform1i(d->pgraph.r2t.m_shader_prog, is_stencil_uni, src_zeta);
    glProgramUniform1i(d->pgraph.r2t.m_shader_prog, do_flip, flip);

    // Render
    glColorMask(true, true, true, true);
    if (m_scissor_test) glDisable(GL_SCISSOR_TEST);
    if (m_blend)        glDisable(GL_BLEND);
    if (m_stencil_test) glDisable(GL_STENCIL_TEST);
    if (m_cull)         glDisable(GL_CULL_FACE);
    if (m_depth_test)   glDisable(GL_DEPTH_TEST);

    glClearColor(0.0f, 0.0f, 1.0f, 1.0f);
    glClear(GL_COLOR_BUFFER_BIT);
    glDrawArrays(GL_TRIANGLES, 0, 3);

    // Restore state saved on stack, framebuffer/vertex arrays/etc
    glBindFramebuffer(GL_FRAMEBUFFER, d->pgraph.gl_framebuffer);
    glBindVertexArray(d->pgraph.gl_vertex_array);
    if (d->pgraph.texture_binding[0]) {
        glBindTexture(d->pgraph.texture_binding[0]->gl_target,
                      d->pgraph.texture_binding[0]->gl_texture);
    }

#if 0
    glUseProgram(d->pgraph.shader_binding->gl_program);
#else
    glUseProgram(0); // FIXME: glUseProgram overrides pipeline so we can just turn it
                     // off here and the pipeline will supersede it again, but really
                     // should just rewrite this to use a separate pipeline
#endif
    // array buffers should be bound after this so no need to bind them here

    glViewport(m_viewport[0], m_viewport[1], m_viewport[2], m_viewport[3]);
    glColorMask(m_color_mask[0], m_color_mask[1], m_color_mask[2], m_color_mask[3]);
    if (m_scissor_test) glEnable(GL_SCISSOR_TEST);
    if (m_blend)        glEnable(GL_BLEND);
    if (m_stencil_test) glEnable(GL_STENCIL_TEST);
    if (m_cull)         glEnable(GL_CULL_FACE);
    if (m_depth_test)   glEnable(GL_DEPTH_TEST);

    // Bind dest texture to output unit
    glBindTexture(dst_target, dst);

#else
    // Reallocate space for new texture dimensions
    // glWaitSync(fence, 0, GL_TIMEOUT_IGNORED);
    // glDeleteSync(fence);

    glTexImage2D(dst_target, 0, f.gl_internal_format,
        width, height, 0, f.gl_format, f.gl_type, NULL);
    for (int i = 0; i < height; i++) {
        glCopyImageSubData(
            src, src_target, 0, 0, height-i-1, 0,
            dst, dst_target, 0, 0, i,          0,
            width, 1, 1);
    }
#endif
}
#endif



static void pgraph_init(NV2AState *d)
{
    int i;

#if PROFILE_METHODS
    signal(SIGUSR2, dump_stats);
#endif

    PGRAPHState *pg = &d->pgraph;

    fuck_fuck_fixme = d;

#if USE_COROUTINES
    qemu_spin_init(&pg->lock);
#else
    qemu_mutex_init(&pg->lock);
#endif
    qemu_cond_init(&pg->interrupt_cond);
    qemu_cond_init(&pg->fifo_access_cond);
    qemu_cond_init(&pg->flip_3d);

    /* fire up opengl */

    pg->gl_context = glo_context_create();
    assert(pg->gl_context);

#ifdef DEBUG_NV2A_GL
    gl_debug_initialize();
#endif

    /* DXT textures */
    assert(glo_check_extension("GL_EXT_texture_compression_s3tc"));
    /*  Internal RGB565 texture format */
    assert(glo_check_extension("GL_ARB_ES2_compatibility"));

    assert(glo_check_extension("GL_ARB_separate_shader_objects"));

    GLint max_vertex_attributes;
    glGetIntegerv(GL_MAX_VERTEX_ATTRIBS, &max_vertex_attributes);
    assert(max_vertex_attributes >= NV2A_VERTEXSHADER_ATTRIBUTES);


    glGenFramebuffers(1, &pg->gl_framebuffer);
    glBindFramebuffer(GL_FRAMEBUFFER, pg->gl_framebuffer);

    /* need a valid framebuffer to start with */
    glGenTextures(1, &pg->gl_color_buffer);
    glBindTexture(GL_TEXTURE_2D, pg->gl_color_buffer);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_BASE_LEVEL, 0);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAX_LEVEL, 0);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);

    glTexImage2D(GL_TEXTURE_2D, 0, GL_RGBA8,
                 640*RES_SCALE_FACTOR, 480*RES_SCALE_FACTOR,
                 0, GL_RGBA, GL_UNSIGNED_BYTE, NULL);
    glFramebufferTexture2D(GL_FRAMEBUFFER, GL_COLOR_ATTACHMENT0,
                           GL_TEXTURE_2D, pg->gl_color_buffer, 0);

    assert(glCheckFramebufferStatus(GL_FRAMEBUFFER)
            == GL_FRAMEBUFFER_COMPLETE);

    //glPolygonMode( GL_FRONT_AND_BACK, GL_LINE );

    // Initialize texture cache
    const size_t texture_cache_size = 512;
    lru_init(&pg->texture_cache,
        &texture_cache_entry_init,
        &texture_cache_entry_deinit,
        &texture_cache_entry_compare);
    pg->texture_cache_entries = malloc(texture_cache_size * sizeof(struct TextureKey));
    assert(pg->texture_cache_entries != NULL);
    for (i = 0; i < texture_cache_size; i++) {
        lru_add_free(&pg->texture_cache, &pg->texture_cache_entries[i].node);
    }

#if USE_TEXTURE_LOCATION_CACHE
    // Initialize texture location cache
    const size_t texture_location_cache_size = 512;
    lru_init(&pg->texture_location_cache,
        &texture_location_cache_entry_init,
        &texture_location_cache_entry_deinit,
        &texture_location_cache_entry_compare);
    pg->texture_location_cache_entries = malloc(texture_location_cache_size * sizeof(struct TextureLocationKey));
    assert(pg->texture_location_cache_entries != NULL);
    for (i = 0; i < texture_location_cache_size; i++) {
        lru_add_free(&pg->texture_location_cache, &pg->texture_location_cache_entries[i].node);
    }
#endif


#if USE_GEOMETRY_CACHE
    // Pre-allocate objects for geometry cache
    size_t gcache_size;

    // inline_array_cache
    gcache_size = 4096;
    lru_init(&pg->inline_array_cache, &gce_init, &gce_deinit, &gce_key_compare);
    pg->inline_array_cache_entries = malloc(gcache_size * sizeof(struct GeometryKey));
    assert(pg->inline_array_cache_entries);
    for (i = 0; i < gcache_size; i++) lru_add_free(&pg->inline_array_cache, &pg->inline_array_cache_entries[i].node);

    // inline_element_cache
    gcache_size = 4096;
    lru_init(&pg->inline_element_cache, &gce_init, &gce_deinit, &gce_key_compare);
    pg->inline_element_cache_entries = malloc(gcache_size * sizeof(struct GeometryKey));
    assert(pg->inline_element_cache_entries);
    for (i = 0; i < gcache_size; i++) lru_add_free(&pg->inline_element_cache, &pg->inline_element_cache_entries[i].node);

    // inline_attribute_buffer_cache
    gcache_size = 4096;
    lru_init(&pg->inline_attribute_buffer_cache, &gce_init, &gce_deinit, &gce_key_compare);
    pg->inline_attribute_buffer_cache_entries = malloc(gcache_size * sizeof(struct GeometryKey));
    assert(pg->inline_attribute_buffer_cache_entries);
    for (i = 0; i < gcache_size; i++) lru_add_free(&pg->inline_attribute_buffer_cache, &pg->inline_attribute_buffer_cache_entries[i].node);

    // converted_buffer_cache
    gcache_size = 4096;
    lru_init(&pg->converted_buffer_cache, &gce_init, &gce_deinit, &gce_key_compare);
    pg->converted_buffer_cache_entries = malloc(gcache_size * sizeof(struct GeometryKey));
    assert(pg->converted_buffer_cache_entries);
    for (i = 0; i < gcache_size; i++) lru_add_free(&pg->converted_buffer_cache, &pg->converted_buffer_cache_entries[i].node);
#endif

    pg->vertex_shader_cache = g_hash_table_new(vertex_shader_hash, vertex_shader_equal);
    pg->fragment_shader_cache = g_hash_table_new(fragment_shader_hash, fragment_shader_equal);

    for (i=0; i<NV2A_VERTEXSHADER_ATTRIBUTES; i++) {
        glGenBuffers(1, &pg->vertex_attributes[i].gl_converted_buffer);
        glGenBuffers(1, &pg->vertex_attributes[i].gl_inline_buffer);
    }
    glGenBuffers(1, &pg->gl_inline_array_buffer);
    glGenBuffers(1, &pg->gl_element_buffer);

    glGenBuffers(1, &pg->gl_memory_buffer);
    glBindBuffer(GL_ARRAY_BUFFER, pg->gl_memory_buffer);
    glBufferData(GL_ARRAY_BUFFER,
                 memory_region_size(d->vram),
                 NULL,
                 GL_DYNAMIC_DRAW);

    glGenVertexArrays(1, &pg->gl_vertex_array);
    glBindVertexArray(pg->gl_vertex_array);

    assert(glGetError() == GL_NO_ERROR);

#if USE_UBO
#if USE_UBO_CACHE
    // converted_buffer_cache
    size_t uboce_size = 128;
    lru_init(&pg->ubo_cache, &uboce_init, &uboce_deinit, &uboce_key_compare);
    pg->ubo_cache_entries = malloc(uboce_size * sizeof(struct UboCacheKey));
    assert(pg->ubo_cache_entries);
    for (i = 0; i < uboce_size; i++) lru_add_free(&pg->ubo_cache, &pg->ubo_cache_entries[i].node);
#else
    size_t len = 4*4*NV2A_VERTEXSHADER_CONSTANTS;
    glGenBuffers(1, &pg->gl_ubo_constants);
    glBindBuffer(GL_UNIFORM_BUFFER, pg->gl_ubo_constants);
    glBufferData(GL_UNIFORM_BUFFER, len, NULL, GL_DYNAMIC_DRAW);
    glBindBuffer(GL_UNIFORM_BUFFER, 0);
#endif
#endif

#if RENDER_TO_TEXTURE
    pgraph_setup_surface_to_texture(d);
#endif

    pg->pipe = 0;
    glGenProgramPipelines(1, &pg->pipe);
    glBindProgramPipeline(pg->pipe);

    glo_set_current(NULL);
}

static void pgraph_destroy(PGRAPHState *pg)
{
#if !USE_COROUTINES
    qemu_mutex_destroy(&pg->lock);
#endif
    qemu_cond_destroy(&pg->interrupt_cond);
    qemu_cond_destroy(&pg->fifo_access_cond);
    qemu_cond_destroy(&pg->flip_3d);

    glo_set_current(pg->gl_context);

    if (pg->gl_color_buffer) {
        glDeleteTextures(1, &pg->gl_color_buffer);
    }
    if (pg->gl_zeta_buffer) {
        glDeleteTextures(1, &pg->gl_zeta_buffer);
    }
    glDeleteFramebuffers(1, &pg->gl_framebuffer);

    // TODO: clear out shader cached

    // Clear out texture cache
    lru_flush(&pg->texture_cache);
    free(pg->texture_cache_entries);

    glo_set_current(NULL);

    glo_context_destroy(pg->gl_context);
}

static void pgraph_vert_shader_update_constants(PGRAPHState *pg,
                                           VertexShaderBinding *binding,
                                           bool binding_changed,
                                           bool vertex_program,
                                           bool fixed_function)
{
    int i, j;

    // if (binding->fog_color_loc != -1) {
    //     uint32_t fog_color = pg->regs[NV_PGRAPH_FOGCOLOR];
    //     glProgramUniform4f(binding->gl_frag_prog, binding->fog_color_loc,
    //                 GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_RED) / 255.0,
    //                 GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_GREEN) / 255.0,
    //                 GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_BLUE) / 255.0,
    //                 GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_ALPHA) / 255.0);
    // }
    if (binding->fog_param_loc[0] != -1) {
        glProgramUniform1f(binding->gl_vert_prog, binding->fog_param_loc[0],
                    *(float*)&pg->regs[NV_PGRAPH_FOGPARAM0]);
    }
    if (binding->fog_param_loc[1] != -1) {
        glProgramUniform1f(binding->gl_vert_prog, binding->fog_param_loc[1],
                    *(float*)&pg->regs[NV_PGRAPH_FOGPARAM1]);
    }

    float zclip_max = *(float*)&pg->regs[NV_PGRAPH_ZCLIPMAX];
    float zclip_min = *(float*)&pg->regs[NV_PGRAPH_ZCLIPMIN];

    if (fixed_function) {
        /* update lighting constants */
        struct {
            uint32_t* v;
            bool* dirty;
            GLint* locs;
            size_t len;
        } lighting_arrays[] = {
            {&pg->ltctxa[0][0], &pg->ltctxa_dirty[0], binding->ltctxa_loc, NV2A_LTCTXA_COUNT},
            {&pg->ltctxb[0][0], &pg->ltctxb_dirty[0], binding->ltctxb_loc, NV2A_LTCTXB_COUNT},
            {&pg->ltc1[0][0], &pg->ltc1_dirty[0], binding->ltc1_loc, NV2A_LTC1_COUNT},
        };

        for (i=0; i<ARRAY_SIZE(lighting_arrays); i++) {
            uint32_t *lighting_v = lighting_arrays[i].v;
            bool *lighting_dirty = lighting_arrays[i].dirty;
            GLint *lighting_locs = lighting_arrays[i].locs;
            size_t lighting_len = lighting_arrays[i].len;
            for (j=0; j<lighting_len; j++) {
                if (!lighting_dirty[j] && !binding_changed) continue;
                GLint loc = lighting_locs[j];
                if (loc != -1) {
                    glProgramUniform4fv(binding->gl_vert_prog, loc, 1, (const GLfloat*)&lighting_v[j*4]);
                }
                lighting_dirty[j] = false;
            }
        }


        for (i = 0; i < NV2A_MAX_LIGHTS; i++) {
            GLint loc;
            loc = binding->light_infinite_half_vector_loc[i];
            if (loc != -1) {
                glProgramUniform3fv(binding->gl_vert_prog, loc, 1, pg->light_infinite_half_vector[i]);
            }
            loc = binding->light_infinite_direction_loc[i];
            if (loc != -1) {
                glProgramUniform3fv(binding->gl_vert_prog, loc, 1, pg->light_infinite_direction[i]);
            }

            loc = binding->light_local_position_loc[i];
            if (loc != -1) {
                glProgramUniform3fv(binding->gl_vert_prog, loc, 1, pg->light_local_position[i]);
            }
            loc = binding->light_local_attenuation_loc[i];
            if (loc != -1) {
                glProgramUniform3fv(binding->gl_vert_prog, loc, 1, pg->light_local_attenuation[i]);
            }
        }

        /* estimate the viewport by assuming it matches the surface ... */
        //FIXME: Get surface dimensions?
        float m11 = 0.5 * pg->surface_shape.clip_width;
        float m22 = -0.5 * pg->surface_shape.clip_height;
        float m33 = zclip_max - zclip_min;
        //float m41 = m11;
        //float m42 = -m22;
        float m43 = zclip_min;
        //float m44 = 1.0;

        if (m33 == 0.0) {
            m33 = 1.0;
        }
        float invViewport[16] = {
            1.0/m11, 0, 0, 0,
            0, 1.0/m22, 0, 0,
            0, 0, 1.0/m33, 0,
            -1.0, 1.0, -m43/m33, 1.0
        };

        if (binding->inv_viewport_loc != -1) {
            glProgramUniformMatrix4fv(binding->gl_vert_prog, binding->inv_viewport_loc,
                               1, GL_FALSE, &invViewport[0]);
        }

    }

    // printf("--- shader constants # %016lx\n", fast_hash(pg->vsh_constants, 4*4*NV2A_VERTEXSHADER_CONSTANTS, 0));


    /* update vertex program constants */
    for (i=0; i<NV2A_VERTEXSHADER_CONSTANTS; i++) {
        if (!pg->vsh_constants_dirty[i] && !binding_changed) continue;

#if !USE_UBO
        GLint loc = binding->vsh_constant_loc[i];
        //assert(loc != -1);
        if (loc != -1) {
            // printf("Constant %d dirty, updated!\n", i);
            glProgramUniform4fv(binding->gl_vert_prog, loc, 1, (const GLfloat*)pg->vsh_constants[i]);
        }
#endif
        pg->vsh_constants_dirty[i] = false;
    }

    // glBindBuffer(GL_UNIFORM_BUFFER, pg->gl_ubo_constants);




#if USE_UBO

    FIXME

    size_t len = 4*4*NV2A_VERTEXSHADER_CONSTANTS;
    uint64_t ubo_hash = fast_hash((const unsigned char*)pg->vsh_constants, 4*4*NV2A_VERTEXSHADER_CONSTANTS, 0);

    UboCacheKey key_in = {
        .buffer_type = GL_UNIFORM_BUFFER,
        .buffer_length = len,
        .populated = 0
    };

    struct lru_node *found = lru_lookup(&pg->ubo_cache, ubo_hash, &key_in);
    UboCacheKey *key_out = container_of(found, struct UboCacheKey, node);
    assert(key_out != NULL);
    glBindBuffer(GL_UNIFORM_BUFFER, key_out->buffer_id);
    SDPRINTF("Uploading uniform buffer data %zd, # %016lx ", len, ubo_hash);
    if (!key_out->populated) {
        SDPRINTF("....uploading\n");
        glBindBufferRange(GL_UNIFORM_BUFFER, 0, key_out->buffer_id, 0, len);
        glBufferData(GL_UNIFORM_BUFFER, len, pg->vsh_constants, GL_DYNAMIC_DRAW);
        key_out->populated = 1;
    } else {
        SDPRINTF("Re-using buffer!\n");
        glBindBufferRange(GL_UNIFORM_BUFFER, 0, key_out->buffer_id, 0, len);
    }
#endif


    if (binding->surface_size_loc != -1) {
        glProgramUniform2f(binding->gl_vert_prog, binding->surface_size_loc, pg->surface_shape.clip_width,
                    pg->surface_shape.clip_height);
    }

    if (binding->clip_range_loc != -1) {
        glProgramUniform2f(binding->gl_vert_prog, binding->clip_range_loc, zclip_min, zclip_max);
    }
}

static void pgraph_frag_shader_update_constants(PGRAPHState *pg,
                                           FragmentShaderBinding *binding,
                                           bool binding_changed)
{
    int i, j;

    /* update combiner constants */
    for (i = 0; i < 9; i++) {
        uint32_t constant[2];
        if (i == 8) {
            /* final combiner */
            constant[0] = pg->regs[NV_PGRAPH_SPECFOGFACTOR0];
            constant[1] = pg->regs[NV_PGRAPH_SPECFOGFACTOR1];
        } else {
            constant[0] = pg->regs[NV_PGRAPH_COMBINEFACTOR0 + i * 4];
            constant[1] = pg->regs[NV_PGRAPH_COMBINEFACTOR1 + i * 4];
        }

        for (j = 0; j < 2; j++) {
            GLint loc = binding->psh_constant_loc[i][j];
            if (loc != -1) {
                float value[4];
                value[0] = (float) ((constant[j] >> 16) & 0xFF) / 255.0f;
                value[1] = (float) ((constant[j] >> 8) & 0xFF) / 255.0f;
                value[2] = (float) (constant[j] & 0xFF) / 255.0f;
                value[3] = (float) ((constant[j] >> 24) & 0xFF) / 255.0f;

                glProgramUniform4fv(binding->gl_frag_prog, loc, 1, value);
            }
        }
    }

    if (binding->alpha_ref_loc != -1) {
        float alpha_ref = GET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                                   NV_PGRAPH_CONTROL_0_ALPHAREF) / 255.0;
        glProgramUniform1f(binding->gl_frag_prog, binding->alpha_ref_loc, alpha_ref);
    }


    /* For each texture stage */
    for (i = 0; i < NV2A_MAX_TEXTURES; i++) {
        // char name[32];
        GLint loc;

        /* Bump luminance only during stages 1 - 3 */
        if (i > 0) {
            loc = binding->bump_mat_loc[i];
            if (loc != -1) {
                glProgramUniformMatrix2fv(binding->gl_frag_prog, loc, 1, GL_FALSE, pg->bump_env_matrix[i - 1]);
            }
            loc = binding->bump_scale_loc[i];
            if (loc != -1) {
                glProgramUniform1f(binding->gl_frag_prog, loc, *(float*)&pg->regs[
                                NV_PGRAPH_BUMPSCALE1 + (i - 1) * 4]);
            }
            loc = binding->bump_offset_loc[i];
            if (loc != -1) {
                glProgramUniform1f(binding->gl_frag_prog, loc, *(float*)&pg->regs[
                            NV_PGRAPH_BUMPOFFSET1 + (i - 1) * 4]);
            }
        }

    }

    if (binding->fog_color_loc != -1) {
        uint32_t fog_color = pg->regs[NV_PGRAPH_FOGCOLOR];
        glProgramUniform4f(binding->gl_frag_prog, binding->fog_color_loc,
                    GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_RED) / 255.0,
                    GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_GREEN) / 255.0,
                    GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_BLUE) / 255.0,
                    GET_MASK(fog_color, NV_PGRAPH_FOGCOLOR_ALPHA) / 255.0);
    }
}

















int shader_bindings;

static void pgraph_bind_shaders(PGRAPHState *pg)
{
    int i, j;

    bool vertex_program = GET_MASK(pg->regs[NV_PGRAPH_CSV0_D],
                                   NV_PGRAPH_CSV0_D_MODE) == 2;

    bool fixed_function = GET_MASK(pg->regs[NV_PGRAPH_CSV0_D],
                                   NV_PGRAPH_CSV0_D_MODE) == 0;

    int program_start = GET_MASK(pg->regs[NV_PGRAPH_CSV0_C],
                                 NV_PGRAPH_CSV0_C_CHEOPS_PROGRAM_START);

    NV2A_GL_DGROUP_BEGIN("%s (VP: %s FFP: %s)", __func__,
                         vertex_program ? "yes" : "no",
                         fixed_function ? "yes" : "no");

    VertexShaderBinding* old_vert_binding = pg->vertex_shader_binding;
    FragmentShaderBinding* old_frag_binding = pg->fragment_shader_binding;

    VertexShaderState state = {
        /* fixed function stuff */
        .skinning = (enum VshSkinning)GET_MASK(pg->regs[NV_PGRAPH_CSV0_D],
                                               NV_PGRAPH_CSV0_D_SKIN),
        .lighting = GET_MASK(pg->regs[NV_PGRAPH_CSV0_C],
                             NV_PGRAPH_CSV0_C_LIGHTING),
        .normalization = pg->regs[NV_PGRAPH_CSV0_C]
                           & NV_PGRAPH_CSV0_C_NORMALIZATION_ENABLE,

        .fixed_function = fixed_function,

        /* vertex program stuff */
        .vertex_program = vertex_program,
        .z_perspective = pg->regs[NV_PGRAPH_CONTROL_0]
                            & NV_PGRAPH_CONTROL_0_Z_PERSPECTIVE_ENABLE,

        /* geometry shader stuff */
        .primitive_mode = (enum ShaderPrimitiveMode)pg->primitive_mode,
        .polygon_front_mode = (enum ShaderPolygonMode)GET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                                                               NV_PGRAPH_SETUPRASTER_FRONTFACEMODE),
        .polygon_back_mode = (enum ShaderPolygonMode)GET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                                                              NV_PGRAPH_SETUPRASTER_BACKFACEMODE),
    };

    FragmentShaderState fstate = {
        .psh = (PshState){
            /* register combiner stuff */
            .window_clip_exclusive = pg->regs[NV_PGRAPH_SETUPRASTER]
                                       & NV_PGRAPH_SETUPRASTER_WINDOWCLIPTYPE,
            .combiner_control = pg->regs[NV_PGRAPH_COMBINECTL],
            .shader_stage_program = pg->regs[NV_PGRAPH_SHADERPROG],
            .other_stage_input = pg->regs[NV_PGRAPH_SHADERCTL],
            .final_inputs_0 = pg->regs[NV_PGRAPH_COMBINESPECFOG0],
            .final_inputs_1 = pg->regs[NV_PGRAPH_COMBINESPECFOG1],

            .alpha_test = pg->regs[NV_PGRAPH_CONTROL_0]
                            & NV_PGRAPH_CONTROL_0_ALPHATESTENABLE,
            .alpha_func = (enum PshAlphaFunc)GET_MASK(pg->regs[NV_PGRAPH_CONTROL_0],
                                   NV_PGRAPH_CONTROL_0_ALPHAFUNC),
        }
    };

    if (!fstate.psh.alpha_test) {
        fstate.psh.alpha_func = 0;
    }

    state.program_length = 0;
    memset(state.program_data, 0, sizeof(state.program_data));

    if (vertex_program) {
        // copy in vertex program tokens
        for (i = program_start; i < NV2A_MAX_TRANSFORM_PROGRAM_LENGTH; i++) {
            uint32_t *cur_token = (uint32_t*)&pg->program_data[i];
            memcpy(&state.program_data[state.program_length],
                   cur_token,
                   VSH_TOKEN_SIZE * sizeof(uint32_t));
            state.program_length++;

            if (vsh_get_field(cur_token, FLD_FINAL)) {
                break;
            }
        }
    }

    /* Texgen */
    for (i = 0; i < 4; i++) {
        unsigned int reg = (i < 2) ? NV_PGRAPH_CSV1_A : NV_PGRAPH_CSV1_B;
        for (j = 0; j < 4; j++) {
            unsigned int masks[] = {
                (i % 2) ? NV_PGRAPH_CSV1_A_T1_S : NV_PGRAPH_CSV1_A_T0_S,
                (i % 2) ? NV_PGRAPH_CSV1_A_T1_T : NV_PGRAPH_CSV1_A_T0_T,
                (i % 2) ? NV_PGRAPH_CSV1_A_T1_R : NV_PGRAPH_CSV1_A_T0_R,
                (i % 2) ? NV_PGRAPH_CSV1_A_T1_Q : NV_PGRAPH_CSV1_A_T0_Q
            };
            state.texgen[i][j] = (enum VshTexgen)GET_MASK_SLOW(pg->regs[reg], masks[j]);
        }
    }

    /* Fog */
    state.fog_enable = pg->regs[NV_PGRAPH_CONTROL_3]
                           & NV_PGRAPH_CONTROL_3_FOGENABLE;
    if (state.fog_enable) {
        /*FIXME: Use CSV0_D? */
        state.fog_mode = (enum VshFogMode)GET_MASK(pg->regs[NV_PGRAPH_CONTROL_3],
                                  NV_PGRAPH_CONTROL_3_FOG_MODE);
        state.foggen = (enum VshFoggen)GET_MASK(pg->regs[NV_PGRAPH_CSV0_D],
                                NV_PGRAPH_CSV0_D_FOGGENMODE);
    } else {
        /* FIXME: Do we still pass the fogmode? */
        state.fog_mode = (enum VshFogMode)0;
        state.foggen = (enum VshFoggen)0;
    }

    /* Texture matrices */
    for (i = 0; i < 4; i++) {
        state.texture_matrix_enable[i] = pg->texture_matrix_enable[i];
    }

    /* Lighting */
    if (state.lighting) {
        for (i = 0; i < NV2A_MAX_LIGHTS; i++) {
            state.light[i] = (enum VshLight)GET_MASK_SLOW(pg->regs[NV_PGRAPH_CSV0_D],
                                      NV_PGRAPH_CSV0_D_LIGHT0 << (i * 2));
        }
    }

    /* Window clip
     *
     * Optimization note: very quickly check to ignore any repeated or zero-size
     * clipping regions. Note that if region number 7 is valid, but the rest are
     * not, we will still add all of them. Clip regions seem to be typically
     * front-loaded (meaning the first one or two regions are populated, and the
     * following are zeroed-out), so let's avoid adding any more complicated
     * masking or copying logic here for now unless we discover a valid case.
     */
    assert(!fstate.psh.window_clip_exclusive); /* FIXME: Untested */
    fstate.psh.window_clip_count = 0;
    uint32_t last_x = 0, last_y = 0;

    for (i = 0; i < 8; i++) {
        const uint32_t x = pg->regs[NV_PGRAPH_WINDOWCLIPX0 + i * 4];
        const uint32_t y = pg->regs[NV_PGRAPH_WINDOWCLIPY0 + i * 4];
        const uint32_t x_min = GET_MASK(x, NV_PGRAPH_WINDOWCLIPX0_XMIN);
        const uint32_t x_max = GET_MASK(x, NV_PGRAPH_WINDOWCLIPX0_XMAX);
        const uint32_t y_min = GET_MASK(y, NV_PGRAPH_WINDOWCLIPY0_YMIN);
        const uint32_t y_max = GET_MASK(y, NV_PGRAPH_WINDOWCLIPY0_YMAX);

        /* Check for zero width or height clipping region */
        if ((x_min == x_max) || (y_min == y_max)) {
            continue;
        }

        /* Check for in-order duplicate regions */
        if ((x == last_x) && (y == last_y)) {
            continue;
        }

        NV2A_DPRINTF("Clipping Region %d: min=(%d, %d) max=(%d, %d)\n",
            i, x_min, y_min, x_max, y_max);

        fstate.psh.window_clip_count = i + 1;
        last_x = x;
        last_y = y;
    }

    for (i = 0; i < (fstate.psh.combiner_control & 0xFF); i++) {
        fstate.psh.rgb_inputs[i] = pg->regs[NV_PGRAPH_COMBINECOLORI0 + i * 4];
        fstate.psh.rgb_outputs[i] = pg->regs[NV_PGRAPH_COMBINECOLORO0 + i * 4];
        fstate.psh.alpha_inputs[i] = pg->regs[NV_PGRAPH_COMBINEALPHAI0 + i * 4];
        fstate.psh.alpha_outputs[i] = pg->regs[NV_PGRAPH_COMBINEALPHAO0 + i * 4];
        //constant_0[i] = pg->regs[NV_PGRAPH_COMBINEFACTOR0 + i * 4];
        //constant_1[i] = pg->regs[NV_PGRAPH_COMBINEFACTOR1 + i * 4];
    }

    for (i = 0; i < 4; i++) {
        fstate.psh.rect_tex[i] = false;
        bool enabled = pg->regs[NV_PGRAPH_TEXCTL0_0 + i*4]
                         & NV_PGRAPH_TEXCTL0_0_ENABLE;
        unsigned int color_format =
            GET_MASK(pg->regs[NV_PGRAPH_TEXFMT0 + i*4],
                     NV_PGRAPH_TEXFMT0_COLOR);

        if (enabled && kelvin_color_format_map[color_format].linear) {
            fstate.psh.rect_tex[i] = true;
        }

        for (j = 0; j < 4; j++) {
            fstate.psh.compare_mode[i][j] =
                (pg->regs[NV_PGRAPH_SHADERCLIPMODE] >> (4 * i + j)) & 1;
        }
        fstate.psh.alphakill[i] = pg->regs[NV_PGRAPH_TEXCTL0_0 + i*4]
                               & NV_PGRAPH_TEXCTL0_0_ALPHAKILLEN;
    }





    // printf("* Shader #%016lx cache ", fast_hash(&state, sizeof(state), 0));

    VertexShaderBinding* cached_shader = (VertexShaderBinding*)g_hash_table_lookup(pg->vertex_shader_cache, &state);
    if (cached_shader) {
        // printf("hit\n");
        pg->vertex_shader_binding = cached_shader;
    } else {
        // printf("miss\n");
        pg->vertex_shader_binding = generate_vertex_shader(state);

        /* cache it */
        VertexShaderState *cache_state = (VertexShaderState *)g_malloc(sizeof(*cache_state));
        memcpy(cache_state, &state, sizeof(*cache_state));
        g_hash_table_insert(pg->vertex_shader_cache, cache_state,
                            (gpointer)pg->vertex_shader_binding);
    }


    FragmentShaderBinding* cached_frag_shader = (FragmentShaderBinding*)g_hash_table_lookup(pg->fragment_shader_cache, &fstate);
    if (cached_frag_shader) {
        // printf("hit\n");
        pg->fragment_shader_binding = cached_frag_shader;
    } else {
        // printf("miss\n");
        pg->fragment_shader_binding = generate_fragment_shader(fstate);

        /* cache it */
        FragmentShaderState *cache_state = (FragmentShaderState *)g_malloc(sizeof(*cache_state));
        memcpy(cache_state, &fstate, sizeof(*cache_state));
        g_hash_table_insert(pg->fragment_shader_cache, cache_state,
                            (gpointer)pg->fragment_shader_binding);
    }

    bool vert_binding_changed = (pg->vertex_shader_binding != old_vert_binding);
    bool frag_binding_changed = (pg->fragment_shader_binding != old_frag_binding);
    bool binding_changed = vert_binding_changed | frag_binding_changed;

    if (binding_changed) {
        shader_bindings++;
    }

#if 0
    if ((old_vert_binding != NULL) && vert_binding_changed) {
        printf("binding changed!\n");

        // Compare what changed
        #define DO_COMP(field) do { \
            if (memcmp(&pg->vertex_shader_binding->state.field, &old_vert_binding->state.field, sizeof(pg->vertex_shader_binding->state.field)) != 0) { \
                printf(stringify(field) " changed!\n"); \
            }} while(0);

        // bool texture_matrix_enable[4];
        DO_COMP( texture_matrix_enable);

        // enum VshTexgen texgen[4][4];
        DO_COMP( texgen);

        // bool fog_enable;
        DO_COMP( fog_enable);

        // enum VshFoggen foggen;
        DO_COMP( foggen);

        // enum VshFogMode fog_mode;
        DO_COMP( fog_mode);

        // enum VshSkinning skinning;
        DO_COMP( skinning);

        // bool normalization;
        DO_COMP( normalization);

        // bool lighting;
        DO_COMP( lighting);

        // enum VshLight light[NV2A_MAX_LIGHTS];
        DO_COMP( light);

        // bool fixed_function;
        DO_COMP( fixed_function);

        // /* vertex program */
        // bool vertex_program;
        DO_COMP( vertex_program);

        // uint32_t program_data[NV2A_MAX_TRANSFORM_PROGRAM_LENGTH][VSH_TOKEN_SIZE];
        DO_COMP( program_data);

        // int program_length;
        DO_COMP( program_length);

        // bool z_perspective;
        // /* primitive format for geometry shader */
        // enum ShaderPolygonMode polygon_front_mode;
        DO_COMP( polygon_front_mode);

        // enum ShaderPolygonMode polygon_back_mode;
        DO_COMP( polygon_back_mode);

        // enum ShaderPrimitiveMode primitive_mode;
        DO_COMP( primitive_mode);
    }
    #undef DO_COMP
#endif
#if 0
    if ((old_frag_binding != NULL) && frag_binding_changed) {
        printf("binding changed!\n");

        // Compare what changed
        #define DO_COMP(field) do { \
            if (memcmp(&pg->fragment_shader_binding->state.field, &old_frag_binding->state.field, sizeof(pg->fragment_shader_binding->state.field)) != 0) { \
                printf(stringify(field) " changed!\n"); \
            }} while(0);

        DO_COMP(psh.combiner_control);
        DO_COMP(psh.shader_stage_program);
        DO_COMP(psh.other_stage_input);
        DO_COMP(psh.final_inputs_0);
        DO_COMP(psh.final_inputs_1);
        DO_COMP(psh.rgb_inputs);
        DO_COMP(psh.rgb_outputs);
        DO_COMP(psh.alpha_inputs);
        DO_COMP(psh.alpha_outputs);
        DO_COMP(psh.rect_tex);
        DO_COMP(psh.compare_mode);
        DO_COMP(psh.alphakill);
        DO_COMP(psh.alpha_test);
        DO_COMP(psh.alpha_func);
        DO_COMP(psh.window_clip_exclusive);
        DO_COMP(psh.window_clip_count);

    }
    #undef DO_COMP
#endif

    // glUseProgram(0);

// if ((old_binding == NULL) || binding_changed) {
    // glBindProgramPipeline(0);
    // glDeleteProgramPipelines(1, &pg->pipe);


    // glGenProgramPipelines(1, &pg->pipe);
    if (vert_binding_changed) {
    glUseProgramStages(pg->pipe, GL_GEOMETRY_SHADER_BIT, pg->vertex_shader_binding->gl_geom_prog);
    glUseProgramStages(pg->pipe, GL_VERTEX_SHADER_BIT,   pg->vertex_shader_binding->gl_vert_prog);
    }

    if (frag_binding_changed) {
    glUseProgramStages(pg->pipe, GL_FRAGMENT_SHADER_BIT, pg->fragment_shader_binding->gl_frag_prog);
    }

    if (binding_changed) {
    glValidateProgramPipeline(pg->pipe);
    }

    // glBindProgramPipeline(pg->pipe);
// }

    /* Clipping regions */
    for (i = 0; i < fstate.psh.window_clip_count; i++) {
        if (pg->fragment_shader_binding->clip_region_loc[i] == -1) {
            continue;
        }

        uint32_t x   = pg->regs[NV_PGRAPH_WINDOWCLIPX0 + i * 4];
        unsigned int x_min = GET_MASK(x, NV_PGRAPH_WINDOWCLIPX0_XMIN);
        unsigned int x_max = GET_MASK(x, NV_PGRAPH_WINDOWCLIPX0_XMAX);

        /* Adjust y-coordinates for the OpenGL viewport: translate coordinates
         * to have the origin at the bottom-left of the surface (as opposed to
         * top-left), and flip y-min and y-max accordingly.
         */
        uint32_t y   = pg->regs[NV_PGRAPH_WINDOWCLIPY0 + i * 4];
        unsigned int y_min = (pg->surface_shape.clip_height - 1) -
                             GET_MASK(y, NV_PGRAPH_WINDOWCLIPY0_YMAX);
        unsigned int y_max = (pg->surface_shape.clip_height - 1) -
                             GET_MASK(y, NV_PGRAPH_WINDOWCLIPY0_YMIN);

        pgraph_apply_anti_aliasing_factor(pg, &x_min, &y_min);
        pgraph_apply_anti_aliasing_factor(pg, &x_max, &y_max);

#if RES_SCALE_FACTOR != 1
        x_min *= RES_SCALE_FACTOR;
        y_min *= RES_SCALE_FACTOR;
        x_max *= RES_SCALE_FACTOR;
        y_max *= RES_SCALE_FACTOR;
#endif

        glProgramUniform4i(pg->fragment_shader_binding->gl_frag_prog,
                           pg->fragment_shader_binding->clip_region_loc[i],
                           x_min, y_min, x_max + 1, y_max + 1);
    }

    pgraph_vert_shader_update_constants(pg, pg->vertex_shader_binding, vert_binding_changed,
                                   vertex_program, fixed_function);

    pgraph_frag_shader_update_constants(pg, pg->fragment_shader_binding, frag_binding_changed);

    NV2A_GL_DGROUP_END();
}

static bool pgraph_framebuffer_dirty(PGRAPHState *pg)
{
    bool shape_changed = memcmp(&pg->surface_shape, &pg->last_surface_shape,
                                sizeof(SurfaceShape)) != 0;
    if (!shape_changed || (!pg->surface_shape.color_format
            && !pg->surface_shape.zeta_format)) {
        return false;
    }
    return true;
}

static bool pgraph_color_write_enabled(PGRAPHState *pg)
{
    return pg->regs[NV_PGRAPH_CONTROL_0] & (
        NV_PGRAPH_CONTROL_0_ALPHA_WRITE_ENABLE
        | NV_PGRAPH_CONTROL_0_RED_WRITE_ENABLE
        | NV_PGRAPH_CONTROL_0_GREEN_WRITE_ENABLE
        | NV_PGRAPH_CONTROL_0_BLUE_WRITE_ENABLE);
}

static bool pgraph_zeta_write_enabled(PGRAPHState *pg)
{
    return pg->regs[NV_PGRAPH_CONTROL_0] & (
        NV_PGRAPH_CONTROL_0_ZWRITEENABLE
        | NV_PGRAPH_CONTROL_0_STENCIL_WRITE_ENABLE);
}

static void pgraph_set_surface_dirty(PGRAPHState *pg, bool color, bool zeta)
{
    NV2A_DPRINTF("pgraph_set_surface_dirty(%d, %d) -- %d %d\n",
                 color, zeta,
                 pgraph_color_write_enabled(pg), pgraph_zeta_write_enabled(pg));
    /* FIXME: Does this apply to CLEARs too? */
    color = color && pgraph_color_write_enabled(pg);
    zeta = zeta && pgraph_zeta_write_enabled(pg);
    pg->surface_color.draw_dirty |= color;
    pg->surface_zeta.draw_dirty |= zeta;
}




static void pgraph_update_surface_part(NV2AState *d, bool upload, bool color) {
    PGRAPHState *pg = &d->pgraph;

    SDPRINTF("%s(, upload=%d, color=%d)\n", __func__, upload, color);

    unsigned int width, height;
    pgraph_get_surface_dimensions(pg, &width, &height);
    pgraph_apply_anti_aliasing_factor(pg, &width, &height);

    Surface *surface;
    hwaddr dma_address;
    GLuint *gl_buffer;
    unsigned int bytes_per_pixel;
    GLenum gl_internal_format, gl_format, gl_type, gl_attachment;

    hwaddr *cur_buffer_addr;

    if (color) {
        surface = &pg->surface_color;
        dma_address = pg->dma_color;
        gl_buffer = &pg->gl_color_buffer;

        cur_buffer_addr = &pg->gl_color_buffer_offset;

        assert(pg->surface_shape.color_format != 0);
        assert(pg->surface_shape.color_format
                < ARRAY_SIZE(kelvin_surface_color_format_map));
        SurfaceColorFormatInfo f =
            kelvin_surface_color_format_map[pg->surface_shape.color_format];
        if (f.bytes_per_pixel == 0) {
            fprintf(stderr, "nv2a: unimplemented color surface format 0x%x\n",
                    pg->surface_shape.color_format);
            abort();
        }

        bytes_per_pixel = f.bytes_per_pixel;
        gl_internal_format = f.gl_internal_format;
        gl_format = f.gl_format;
        gl_type = f.gl_type;
        gl_attachment = GL_COLOR_ATTACHMENT0;

    } else {
        surface = &pg->surface_zeta;
        dma_address = pg->dma_zeta;
        gl_buffer = &pg->gl_zeta_buffer;

        cur_buffer_addr = &pg->gl_zeta_buffer_offset;

        assert(pg->surface_shape.zeta_format != 0);
        switch (pg->surface_shape.zeta_format) {
        case NV097_SET_SURFACE_FORMAT_ZETA_Z16:
            bytes_per_pixel = 2;
            gl_format = GL_DEPTH_COMPONENT;
            gl_attachment = GL_DEPTH_ATTACHMENT;
            if (pg->surface_shape.z_format) {
                gl_type = GL_HALF_FLOAT;
                gl_internal_format = GL_DEPTH_COMPONENT32F;
            } else {
                gl_type = GL_UNSIGNED_SHORT;
                gl_internal_format = GL_DEPTH_COMPONENT16;
            }
            break;
        case NV097_SET_SURFACE_FORMAT_ZETA_Z24S8:
            bytes_per_pixel = 4;
            gl_format = GL_DEPTH_STENCIL;
            gl_attachment = GL_DEPTH_STENCIL_ATTACHMENT;
            if (pg->surface_shape.z_format) {
                assert(false);
                gl_type = GL_FLOAT_32_UNSIGNED_INT_24_8_REV;
                gl_internal_format = GL_DEPTH32F_STENCIL8;
            } else {
                gl_type = GL_UNSIGNED_INT_24_8;
                gl_internal_format = GL_DEPTH24_STENCIL8;
            }
            break;
        default:
            assert(false);
            break;
        }
    }


    DMAObject dma = nv_dma_load(d, dma_address);
    /* There's a bunch of bugs that could cause us to hit this function
     * at the wrong time and get a invalid dma object.
     * Check that it's sane. */
    assert(dma.dma_class == NV_DMA_IN_MEMORY_CLASS);

    assert(dma.address + surface->offset != 0);
    assert(surface->offset <= dma.limit);
    assert(surface->offset + surface->pitch * height <= dma.limit + 1);

    hwaddr data_len;
    uint8_t *data = (uint8_t*)nv_dma_map(d, dma_address, &data_len);

    /* TODO */
    // assert(pg->surface_clip_x == 0 && pg->surface_clip_y == 0);

    bool swizzle = (pg->surface_type == NV097_SET_SURFACE_FORMAT_TYPE_SWIZZLE);

    uint8_t *buf = data + surface->offset;
    if (swizzle) {
        buf = (uint8_t*)g_malloc(height * surface->pitch);
    }

    bool dirty = surface->buffer_dirty;
    if (color) {
        // dirty |= 1;
        // SDPRINTF("Testing %08lx... %d -> ", surface->offset, dirty);
#if !USE_SHARED_CONTEXT
        // memory_global_dirty_log_sync();
        dirty |= memory_region_test_and_clear_dirty(d->vram,
                                               dma.address + surface->offset,
                                               surface->pitch * height,
                                               DIRTY_MEMORY_NV2A);
#endif
        // SDPRINTF("%d (%s)\n", dirty, dirty ? "DIRTY" : "CLEAN");
    }
    if (upload && dirty) {
        /* surface modified (or moved) by the cpu.
         * copy it into the opengl renderbuffer */
        assert(!surface->draw_dirty);

        assert(surface->pitch % bytes_per_pixel == 0);


        if (!color) {
            /* need to clear the depth_stencil and depth attachment for zeta */
            glFramebufferTexture2D(GL_FRAMEBUFFER,
                                   GL_DEPTH_ATTACHMENT,
                                   GL_TEXTURE_2D,
                                   0, 0);
            glFramebufferTexture2D(GL_FRAMEBUFFER,
                                   GL_DEPTH_STENCIL_ATTACHMENT,
                                   GL_TEXTURE_2D,
                                   0, 0);
        }

        glFramebufferTexture2D(GL_FRAMEBUFFER,
                               gl_attachment,
                               GL_TEXTURE_2D,
                               0, 0);

        if (*gl_buffer) {
#if USE_SHARED_CONTEXT
            SDPRINTF("Would have released, but instead caching buffer %d\n", *gl_buffer);
            int index = surface_cache_store(*cur_buffer_addr);
            surface_cache[index].buf_id = *gl_buffer;
            // surface_cache[index].fence = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 ); // Should probably be moved below
            surface_cache[index].color = color;

            memcpy(&surface_cache[index].shape, &pg->last_surface_shape, sizeof(SurfaceShape));
#else
            SDPRINTF("Releasing buffer %d\n", *gl_buffer);
            glDeleteTextures(1, gl_buffer);
#endif

            *gl_buffer = 0;
        }

        // Store offset of new surface
        *cur_buffer_addr = dma.address + surface->offset;

        // Look in surface cache for this buffer
#if USE_SHARED_CONTEXT
        int index = surface_cache_find(surface->offset, color);

        // Verify surface shape
        if (index >= 0) {
            if ((memcmp(&pg->surface_shape, &surface_cache[index].shape, sizeof(SurfaceShape)) != 0) ) {
                SDPRINTF("Surface shape changed on us! buf_id = %d\n", surface_cache[index].buf_id);
                SDPRINTF("Deleting texture..\n");
                glDeleteTextures(1, &surface_cache[index].buf_id);
                surface_cache_retire(index);
                index = -1;
            } else {
                SDPRINTF("Shapes match!\n");
            }
        }

        // hmm
        if (index > 0) {
            if (color == surface_cache[index].color) {
                // Should be compatible
            } else {
                printf("Tried to load %s surface, wanted %s\n",
                    surface_cache[index].color ? "color" : "zeta",
                    color ? "color" : "zeta");
                glDeleteTextures(1, &surface_cache[index].buf_id);
                surface_cache_retire(index);
                index = -1;
            }
        }

        // FIXME: This code currently ignores the fact that the CPU may modify
        // the surface data!
        //
        // To do this in a way that allows the CPU to modify the surface at
        // any time we would need to protect the memory region containing the
        // surface and on CPU reads, download the surface into memory. Then
        // write protect the surface to catch writes into the surface. However,
        // it's not clear to me yet how often this really happens. For now,
        // let's be lazy and not worry about syncing.

#else
        int index = -1;
#endif
        if (index < 0) {
            SDPRINTF("Couldn't find buffer in cache for %08lx\n", surface->offset);

        if (swizzle) {
            unswizzle_rect(data + surface->offset,
                           width, height,
                           buf,
                           surface->pitch,
                           bytes_per_pixel);
        }

        glGenTextures(1, gl_buffer);
        SDPRINTF("Created buffer %d\n", *gl_buffer);
        glBindTexture(GL_TEXTURE_2D, *gl_buffer);

        NV2A_GL_DLABEL(GL_TEXTURE, *gl_buffer,
                       "%s format: %0X, width: %d, height: %d",
                       color ? "color" : "zeta",
                       color ? pg->surface_shape.color_format : pg->surface_shape.zeta_format,
                       width, height);

        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_BASE_LEVEL, 0);
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAX_LEVEL, 0);
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);

        /* This is VRAM so we can't do this inplace! */
#if !USE_SHARED_CONTEXT // FIXME: Just skipping all uploads of surfaces rn
        uint8_t *flipped_buf = (uint8_t*)g_malloc(width * height * bytes_per_pixel);
        unsigned int irow;
        for (irow = 0; irow < height; irow++) {
            memcpy(&flipped_buf[width * (height - irow - 1)
                                     * bytes_per_pixel],
                   &buf[surface->pitch * irow],
                   width * bytes_per_pixel);
        }

        SDPRINTF("Actually uploading...\n");
        glTexImage2D(GL_TEXTURE_2D, 0, gl_internal_format,
                     width, height,
                     0, gl_format, gl_type,
                     flipped_buf);
        g_free(flipped_buf);
#else
        SDPRINTF("Reserving space but skipping upload...\n");
        glTexImage2D(GL_TEXTURE_2D, 0, gl_internal_format,
#if RES_SCALE_FACTOR != 1
                     width*RES_SCALE_FACTOR, height*RES_SCALE_FACTOR,
#else
                     width, height,
#endif
                     0, gl_format, gl_type,
                     NULL); // skipping upload
#endif
        } else {
            SDPRINTF("Found buffer in cache for %08lx!\n", surface->offset);
            *gl_buffer = surface_cache[index].buf_id;
            SDPRINTF("shape reports %d x %d\n", surface_cache[index].shape.clip_width, surface_cache[index].shape.clip_height);
            surface_cache_retire(index);
            glBindTexture(GL_TEXTURE_2D, *gl_buffer);
        }

        SDPRINTF("Attaching buffer %d to framebuffer\n", *gl_buffer);

        #if PROFILE_SURFACES
        int w, h;
        int miplevel = 0;
        glGetTexLevelParameteriv(GL_TEXTURE_2D, miplevel, GL_TEXTURE_WIDTH, &w);
        glGetTexLevelParameteriv(GL_TEXTURE_2D, miplevel, GL_TEXTURE_HEIGHT, &h);
        SDPRINTF("%d x %d\n", w, h);
        SDPRINTF("want %d x %d\n", width, height);
        #endif

        glFramebufferTexture2D(GL_FRAMEBUFFER,
                               gl_attachment,
                               GL_TEXTURE_2D,
                               *gl_buffer, 0);

        assert(glCheckFramebufferStatus(GL_FRAMEBUFFER)
            == GL_FRAMEBUFFER_COMPLETE);

#if !USE_SHARED_CONTEXT
        if (color) {
            pgraph_update_memory_buffer(d, dma.address + surface->offset,
                                        surface->pitch * height, true);
        }
#endif
        surface->buffer_dirty = false;

#if PROFILE_SURFACES
        print_timestamp();
        uint64_t shash = fast_hash((const unsigned char *)buf, surface->pitch * height, 0);
        SDPRINTF("RAM->GPU (%c) %08lx - %4d x %4d # %016lx\n",
            color ? 'c' : 'z',
            surface->offset,
            surface->pitch,
            height,
            shash
            );
#endif

        NV2A_GL_DPRINTF(true, "upload_surface %s 0x%" HWADDR_PRIx " - 0x%" HWADDR_PRIx ", "
                      "(0x%" HWADDR_PRIx " - 0x%" HWADDR_PRIx ", "
                        "%d %d, %d %d, %d)",
            color ? "color" : "zeta",
            dma.address, dma.address + dma.limit,
            dma.address + surface->offset,
            dma.address + surface->pitch * height,
            pg->surface_shape.clip_x, pg->surface_shape.clip_y,
            pg->surface_shape.clip_width,
            pg->surface_shape.clip_height,
            surface->pitch);
    }

    if (!upload && surface->draw_dirty) {

        SDPRINTF("crtc_start_last[0] = %08lx\n", crtc_start_last[0]);
        SDPRINTF("crtc_start_last[1] = %08lx\n", crtc_start_last[1]);
        SDPRINTF("crtc_start_last[2] = %08lx\n", crtc_start_last[2]);

#if !USE_SHARED_CONTEXT
        // if (surface->offset == crtc_start_last[2]) { // hack to only copy fb to memory
        SDPRINTF("Actually downloading...\n");

        /* read the opengl framebuffer into the surface */
        glo_readpixels(gl_format, gl_type,
                       bytes_per_pixel, surface->pitch,
                       width, height,
                       buf);
        assert(glGetError() == GL_NO_ERROR);

#if 0
        uint8_t *flipped_buf = (uint8_t*)g_malloc(width * height * bytes_per_pixel);
        unsigned int irow;
        for (irow = 0; irow < height; irow++) {
            memcpy(&flipped_buf[width * (height - irow - 1)
                                     * bytes_per_pixel],
                   &buf[surface->pitch * irow],
                   width * bytes_per_pixel);
        }

        memcpy(buf, flipped_buf, width * height * bytes_per_pixel);
        free(flipped_buf);
#endif

        if (swizzle) {
            swizzle_rect(buf,
                         width, height,
                         data + surface->offset,
                         surface->pitch,
                         bytes_per_pixel);
        }

        memory_region_set_client_dirty(d->vram,
                                       dma.address + surface->offset,
                                       surface->pitch * height,
                                       DIRTY_MEMORY_VGA);

        // } // hack

        if (color) {
            pgraph_update_memory_buffer(d, dma.address + surface->offset,
                                        surface->pitch * height, true);
        }
#endif // !USE_SHARED_CONTEXT

        surface->draw_dirty = false;
        surface->write_enabled_cache = false;

#if PROFILE_SURFACES
        print_timestamp();
        uint64_t shash = fast_hash((const unsigned char *)buf, surface->pitch * height, 0);
        SDPRINTF("RAM<-GPU (%c) %08lx - %4d x %4d # %016lx\n",
            color ? 'c' : 'z',
            surface->offset,
            surface->pitch,
            height,
            shash
            );
#endif

        NV2A_GL_DPRINTF(true, "read_surface %d %s 0x%" HWADDR_PRIx " - 0x%" HWADDR_PRIx ", "
                      "(0x%" HWADDR_PRIx " - 0x%" HWADDR_PRIx ", "
                        "%d %d, %d %d, %d)", *gl_buffer,
            color ? "color" : "zeta",
            dma.address, dma.address + dma.limit,
            dma.address + surface->offset,
            dma.address + surface->pitch * pg->surface_shape.clip_height,
            pg->surface_shape.clip_x, pg->surface_shape.clip_y,
            pg->surface_shape.clip_width, pg->surface_shape.clip_height,
            surface->pitch);
    }

    if (swizzle) {
        g_free(buf);
    }
}

static void pgraph_update_surface(NV2AState *d, bool upload,
                                  bool color_write, bool zeta_write)
{
    PGRAPHState *pg = &d->pgraph;

    pg->surface_shape.z_format = GET_MASK(pg->regs[NV_PGRAPH_SETUPRASTER],
                                          NV_PGRAPH_SETUPRASTER_Z_FORMAT);

    /* FIXME: Does this apply to CLEARs too? */
    color_write = color_write && pgraph_color_write_enabled(pg);
    zeta_write = zeta_write && pgraph_zeta_write_enabled(pg);

    if (upload && pgraph_framebuffer_dirty(pg)) {
        assert(!pg->surface_color.draw_dirty);
        assert(!pg->surface_zeta.draw_dirty);

        pg->surface_color.buffer_dirty = true;
        pg->surface_zeta.buffer_dirty = true;

        glFramebufferTexture2D(GL_FRAMEBUFFER,
                               GL_COLOR_ATTACHMENT0,
                               GL_TEXTURE_2D,
                               0, 0);

        if (pg->gl_color_buffer) {
#if USE_SHARED_CONTEXT
            SDPRINTF("Would have released, but instead caching buffer %d\n", pg->gl_color_buffer);
            int index = surface_cache_store(pg->gl_color_buffer_offset);
            surface_cache[index].buf_id = pg->gl_color_buffer;
            surface_cache[index].shape = pg->last_surface_shape;
            // surface_cache[index].fence = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 ); // Should probably be moved below
            surface_cache[index].color = 1;
#else
            SDPRINTF("Releasing color buffer (%d)\n", pg->gl_color_buffer);
            glDeleteTextures(1, &pg->gl_color_buffer);
#endif
            pg->gl_color_buffer = 0;
        }

        glFramebufferTexture2D(GL_FRAMEBUFFER,
                               GL_DEPTH_ATTACHMENT,
                               GL_TEXTURE_2D,
                               0, 0);
        glFramebufferTexture2D(GL_FRAMEBUFFER,
                               GL_DEPTH_STENCIL_ATTACHMENT,
                               GL_TEXTURE_2D,
                               0, 0);

        if (pg->gl_zeta_buffer) {
#if USE_SHARED_CONTEXT
            SDPRINTF("Would have released, but instead caching buffer %d\n", pg->gl_zeta_buffer);
            int index = surface_cache_store(pg->gl_zeta_buffer_offset);
            surface_cache[index].buf_id = pg->gl_zeta_buffer;
            surface_cache[index].shape = pg->last_surface_shape;
            // surface_cache[index].fence = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 ); // Should probably be moved below
            surface_cache[index].color = 0;
#else
            SDPRINTF("Releasing zeta buffer (%d)\n", pg->gl_zeta_buffer);
            glDeleteTextures(1, &pg->gl_zeta_buffer);
#endif
            pg->gl_zeta_buffer = 0;
        }

        memcpy(&pg->last_surface_shape, &pg->surface_shape,
               sizeof(SurfaceShape));
    }

    if ((color_write || (!upload && pg->surface_color.write_enabled_cache))
        && (upload || pg->surface_color.draw_dirty)) {
        pgraph_update_surface_part(d, upload, true);
    }


    if ((zeta_write || (!upload && pg->surface_zeta.write_enabled_cache))
        && (upload || pg->surface_zeta.draw_dirty)) {
        pgraph_update_surface_part(d, upload, false);
    }
}

static void pgraph_bind_textures(NV2AState *d)
{
    int i;
    PGRAPHState *pg = &d->pgraph;

    NV2A_GL_DGROUP_BEGIN("%s", __func__);

    for (i=0; i<NV2A_MAX_TEXTURES; i++) {

        uint32_t ctl_0 = pg->regs[NV_PGRAPH_TEXCTL0_0 + i*4];
        uint32_t ctl_1 = pg->regs[NV_PGRAPH_TEXCTL1_0 + i*4];
        uint32_t fmt = pg->regs[NV_PGRAPH_TEXFMT0 + i*4];
        uint32_t filter = pg->regs[NV_PGRAPH_TEXFILTER0 + i*4];
        uint32_t address =  pg->regs[NV_PGRAPH_TEXADDRESS0 + i*4];
        uint32_t palette =  pg->regs[NV_PGRAPH_TEXPALETTE0 + i*4];

        bool enabled = GET_MASK(ctl_0, NV_PGRAPH_TEXCTL0_0_ENABLE);
        unsigned int min_mipmap_level =
            GET_MASK(ctl_0, NV_PGRAPH_TEXCTL0_0_MIN_LOD_CLAMP);
        unsigned int max_mipmap_level =
            GET_MASK(ctl_0, NV_PGRAPH_TEXCTL0_0_MAX_LOD_CLAMP);

        unsigned int pitch =
            GET_MASK(ctl_1, NV_PGRAPH_TEXCTL1_0_IMAGE_PITCH);

        unsigned int dma_select =
            GET_MASK(fmt, NV_PGRAPH_TEXFMT0_CONTEXT_DMA);
        bool cubemap =
            GET_MASK(fmt, NV_PGRAPH_TEXFMT0_CUBEMAPENABLE);
        unsigned int dimensionality =
            GET_MASK(fmt, NV_PGRAPH_TEXFMT0_DIMENSIONALITY);
        unsigned int color_format = GET_MASK(fmt, NV_PGRAPH_TEXFMT0_COLOR);
        unsigned int levels = GET_MASK(fmt, NV_PGRAPH_TEXFMT0_MIPMAP_LEVELS);
        unsigned int log_width = GET_MASK(fmt, NV_PGRAPH_TEXFMT0_BASE_SIZE_U);
        unsigned int log_height = GET_MASK(fmt, NV_PGRAPH_TEXFMT0_BASE_SIZE_V);
        unsigned int log_depth = GET_MASK(fmt, NV_PGRAPH_TEXFMT0_BASE_SIZE_P);

        unsigned int rect_width =
            GET_MASK(pg->regs[NV_PGRAPH_TEXIMAGERECT0 + i*4],
                     NV_PGRAPH_TEXIMAGERECT0_WIDTH);
        unsigned int rect_height =
            GET_MASK(pg->regs[NV_PGRAPH_TEXIMAGERECT0 + i*4],
                     NV_PGRAPH_TEXIMAGERECT0_HEIGHT);
#ifdef DEBUG_NV2A
        unsigned int lod_bias =
            GET_MASK(filter, NV_PGRAPH_TEXFILTER0_MIPMAP_LOD_BIAS);
#endif
        unsigned int min_filter = GET_MASK(filter, NV_PGRAPH_TEXFILTER0_MIN);
        unsigned int mag_filter = GET_MASK(filter, NV_PGRAPH_TEXFILTER0_MAG);

        unsigned int addru = GET_MASK(address, NV_PGRAPH_TEXADDRESS0_ADDRU);
        unsigned int addrv = GET_MASK(address, NV_PGRAPH_TEXADDRESS0_ADDRV);
        unsigned int addrp = GET_MASK(address, NV_PGRAPH_TEXADDRESS0_ADDRP);

        unsigned int border_source = GET_MASK(fmt,
                                              NV_PGRAPH_TEXFMT0_BORDER_SOURCE);
        uint32_t border_color = pg->regs[NV_PGRAPH_BORDERCOLOR0 + i*4];

        unsigned int offset = pg->regs[NV_PGRAPH_TEXOFFSET0 + i*4];

        bool palette_dma_select =
            GET_MASK(palette, NV_PGRAPH_TEXPALETTE0_CONTEXT_DMA);
        unsigned int palette_length_index =
            GET_MASK(palette, NV_PGRAPH_TEXPALETTE0_LENGTH);
        unsigned int palette_offset =
            palette & NV_PGRAPH_TEXPALETTE0_OFFSET;

        unsigned int palette_length = 0;
        switch (palette_length_index) {
        case NV_PGRAPH_TEXPALETTE0_LENGTH_256: palette_length = 256; break;
        case NV_PGRAPH_TEXPALETTE0_LENGTH_128: palette_length = 128; break;
        case NV_PGRAPH_TEXPALETTE0_LENGTH_64: palette_length = 64; break;
        case NV_PGRAPH_TEXPALETTE0_LENGTH_32: palette_length = 32; break;
        default: assert(false); break;
        }

        /* Check for unsupported features */
        assert(!(filter & NV_PGRAPH_TEXFILTER0_ASIGNED));
        assert(!(filter & NV_PGRAPH_TEXFILTER0_RSIGNED));
        assert(!(filter & NV_PGRAPH_TEXFILTER0_GSIGNED));
        assert(!(filter & NV_PGRAPH_TEXFILTER0_BSIGNED));

        glActiveTexture(GL_TEXTURE0 + i);
        if (!enabled) {
            glBindTexture(GL_TEXTURE_CUBE_MAP, 0);
            glBindTexture(GL_TEXTURE_RECTANGLE, 0);
            glBindTexture(GL_TEXTURE_1D, 0);
            glBindTexture(GL_TEXTURE_2D, 0);
            glBindTexture(GL_TEXTURE_3D, 0);
            continue;
        }

        if (!pg->texture_dirty[i] && pg->texture_binding[i]) {
            if (pg->fragment_shader_binding->tex_scale_loc[i] != -1) {
                glProgramUniform1f(pg->fragment_shader_binding->gl_frag_prog,
                                   pg->fragment_shader_binding->tex_scale_loc[i], pg->texture_binding[i]->scale);
            }
            glBindTexture(pg->texture_binding[i]->gl_target,
                          pg->texture_binding[i]->gl_texture);
            continue;
        }

        NV2A_DPRINTF(" texture %d is format 0x%x, off 0x%x (r %d, %d or %d, %d, %d; %d%s),"
                        " filter %x %x, levels %d-%d %d bias %d\n",
                     i, color_format, offset,
                     rect_width, rect_height,
                     1 << log_width, 1 << log_height, 1 << log_depth,
                     pitch,
                     cubemap ? "; cubemap" : "",
                     min_filter, mag_filter,
                     min_mipmap_level, max_mipmap_level, levels,
                     lod_bias);

        assert(color_format < ARRAY_SIZE(kelvin_color_format_map));
        ColorFormatInfo f = kelvin_color_format_map[color_format];
        if (f.bytes_per_pixel == 0) {
            fprintf(stderr, "nv2a: unimplemented texture color format 0x%x\n",
                    color_format);
            abort();
        }

        unsigned int width, height, depth;
        if (f.linear) {
            assert(dimensionality == 2);
            width = rect_width;
            height = rect_height;
            depth = 1;
        } else {
            width = 1 << log_width;
            height = 1 << log_height;
            depth = 1 << log_depth;

            /* FIXME: What about 3D mipmaps? */
            levels = MIN(levels, max_mipmap_level + 1);
            if (f.gl_format != 0) {
                /* Discard mipmap levels that would be smaller than 1x1.
                 * FIXME: Is this actually needed?
                 *
                 * >> Level 0: 32 x 4
                 *    Level 1: 16 x 2
                 *    Level 2: 8 x 1
                 *    Level 3: 4 x 1
                 *    Level 4: 2 x 1
                 *    Level 5: 1 x 1
                 */
                levels = MIN(levels, MAX(log_width, log_height) + 1);
            } else {
                /* OpenGL requires DXT textures to always have a width and
                 * height a multiple of 4. The Xbox and DirectX handles DXT
                 * textures smaller than 4 by padding the reset of the block.
                 *
                 * See:
                 * https://msdn.microsoft.com/en-us/library/windows/desktop/bb204843(v=vs.85).aspx
                 * https://msdn.microsoft.com/en-us/library/windows/desktop/bb694531%28v=vs.85%29.aspx#Virtual_Size
                 *
                 * Work around this for now by discarding mipmap levels that
                 * would result in too-small textures. A correct solution
                 * will be to decompress these levels manually, or add texture
                 * sampling logic.
                 *
                 * >> Level 0: 64 x 8
                 *    Level 1: 32 x 4
                 *    Level 2: 16 x 2 << Ignored
                 * >> Level 0: 16 x 16
                 *    Level 1: 8 x 8
                 *    Level 2: 4 x 4 << OK!
                 */
                if (log_width < 2 || log_height < 2) {
                    /* Base level is smaller than 4x4... */
                    levels = 1;
                } else {
                    levels = MIN(levels, MIN(log_width, log_height) - 1);
                }
            }
            assert(levels > 0);
        }

        unsigned int texture_vram_offset = 0;

        hwaddr dma_len;
        uint8_t *texture_data;
        if (dma_select) {
            texture_data = (uint8_t*)nv_dma_map(d, pg->dma_b, &dma_len);
            // texture_vram_offset = pg->dma_b.address;
        } else {
            texture_data = (uint8_t*)nv_dma_map(d, pg->dma_a, &dma_len);
            // texture_vram_offset = pg->dma_a.address;
        }
        assert(offset < dma_len);
        texture_data += offset;

        texture_vram_offset = texture_data - d->vram_ptr;

        hwaddr palette_dma_len;
        uint8_t *palette_data;
        if (palette_dma_select) {
            palette_data = (uint8_t*)nv_dma_map(d, pg->dma_b, &palette_dma_len);
        } else {
            palette_data = (uint8_t*)nv_dma_map(d, pg->dma_a, &palette_dma_len);
        }
        assert(palette_offset < palette_dma_len);
        palette_data += palette_offset;

        NV2A_DPRINTF(" - 0x%tx\n", texture_data - d->vram_ptr);

        size_t length = 0;
        if (f.linear) {
            assert(cubemap == false);
            assert(dimensionality == 2);
            length = height * pitch;
        } else {
            if (dimensionality >= 2) {
                unsigned int w = width, h = height;
                int level;
                if (f.gl_format != 0) {
                    for (level = 0; level < levels; level++) {
                        w = MAX(w, 1); h = MAX(h, 1);
                        length += w * h * f.bytes_per_pixel;
                        w /= 2;
                        h /= 2;
                    }
                } else {
                    /* Compressed textures are a bit different */
                    unsigned int block_size;
                    if (f.gl_internal_format ==
                            GL_COMPRESSED_RGBA_S3TC_DXT1_EXT) {
                        block_size = 8;
                    } else {
                        block_size = 16;
                    }

                    for (level = 0; level < levels; level++) {
                        w = MAX(w, 4); h = MAX(h, 4);
                        length += w/4 * h/4 * block_size;
                        w /= 2; h /= 2;
                    }
                }
                if (cubemap) {
                    assert(dimensionality == 2);
                    length *= 6;
                }
                if (dimensionality >= 3) {
                    length *= depth;
                }
            }
        }

        TextureShape state = {
            .cubemap = cubemap,
            .dimensionality = dimensionality,
            .color_format = color_format,
            .levels = levels,
            .width = width,
            .height = height,
            .depth = depth,
            .min_mipmap_level = min_mipmap_level,
            .max_mipmap_level = max_mipmap_level,
            .pitch = pitch,
        };

#ifdef USE_TEXTURE_CACHE


#if USE_TEXTURE_LOCATION_CACHE
        TextureLocationKey key = {
            .state = state,
            .texture_data = texture_data,
            .texture_len = length,
            .palette_data = palette_data,
            .palette_len = palette_length
        };

        uint64_t texture_hash = fast_hash(&key, sizeof(key), 5003);

        struct lru_node *found = lru_lookup(&pg->texture_location_cache, texture_hash, &key);
        TextureLocationKey *key_out = container_of(found, struct TextureLocationKey, node);
        assert((key_out != NULL) && (key_out->binding != NULL));

#else // USE_TEXTURE_LOCATION_CACHE
        uint64_t texture_hash = fast_hash(texture_data, length, 5003)
                              ^ fnv_hash(palette_data, palette_length);

        TextureKey key = {
            .state = state,
            .texture_data = texture_data,
            .palette_data = palette_data,
        };

        struct lru_node *found = lru_lookup(&pg->texture_cache, texture_hash, &key);
        TextureKey *key_out = container_of(found, struct TextureKey, node);
        assert((key_out != NULL) && (key_out->binding != NULL));
#endif // USE_TEXTURE_LOCATION_CACHE

        TextureBinding *binding = key_out->binding;

        binding->refcnt++;
#else
        TextureBinding *binding = generate_texture(state,
                                                   texture_data, palette_data);
#endif
        binding->scale = 1.0f;
        glBindTexture(binding->gl_target, binding->gl_texture);

#if RENDER_TO_TEXTURE
        // FIXME: Probably move this into generate_texture

        // printf("tex %d = %08x (%d x %d)\n", i, texture_vram_offset, state.width, state.height);

        int index = surface_cache_find(texture_vram_offset, 1);
        if (index >= 0) {
            // printf("found in cache\n");

            // Found a surface in the cache which matches the offset of this texture.
            // However, the cached surface may be stale and the address could just happen
            // to be the same. Sanity check...
            if (check_surface_to_texture_compatibility(surface_cache[index].shape.color_format, color_format)) {
                // Surface and texture format are compatible...
                // FIXME: Better checks on pixel formats
                // printf("ok!\n");

                pgraph_render_surface_to_texture(
                    d, surface_cache[index].fence,
                    surface_cache[index].buf_id, surface_cache[index].shape.color_format, GL_TEXTURE_2D,
                    binding->gl_texture, color_format, binding->gl_target,
#if RES_SCALE_FACTOR != 1
                    state.width*RES_SCALE_FACTOR, state.height*RES_SCALE_FACTOR
#else
                    state.width, state.height
#endif
                    , !surface_cache[index].color, 1
                    );

    #if RES_SCALE_FACTOR != 1
                // Only need to scale for unnormalized coords
                if (binding->gl_target == GL_TEXTURE_RECTANGLE) {
                    binding->scale = RES_SCALE_FACTOR * 1.0f;
                }
    #endif

                // printf("-> found match");

            } else {
                // printf("found match but bad color format\n");

                // FIXME: We should probably remove the stale surface from
                // cache. But what if the surface is actually used later?
                // Possibly add frame tick counter and evict old surfaces
                // after a period of time. For now, let's just retire the
                // surface.
                glDeleteTextures(1, &surface_cache[index].buf_id);
                surface_cache_retire(index);
            }
        }
#endif

    NV2A_GL_DLABEL(GL_TEXTURE, binding->gl_texture,
                   "format: 0x%02X%s, %d dimensions%s, width: %d, height: %d, depth: %d",
                   state.color_format, f.linear ? "" : " (SZ)",
                   state.dimensionality, state.cubemap ? " (Cubemap)" : "",
                   state.width, state.height, state.depth);

        if (pg->fragment_shader_binding->tex_scale_loc[i] != -1) {
            glProgramUniform1f(pg->fragment_shader_binding->gl_frag_prog,
                               pg->fragment_shader_binding->tex_scale_loc[i], binding->scale);
        }

        if (f.linear) {
            /* somtimes games try to set mipmap min filters on linear textures.
             * this could indicate a bug... */
            switch (min_filter) {
            case NV_PGRAPH_TEXFILTER0_MIN_BOX_NEARESTLOD:
            case NV_PGRAPH_TEXFILTER0_MIN_BOX_TENT_LOD:
                min_filter = NV_PGRAPH_TEXFILTER0_MIN_BOX_LOD0;
                break;
            case NV_PGRAPH_TEXFILTER0_MIN_TENT_NEARESTLOD:
            case NV_PGRAPH_TEXFILTER0_MIN_TENT_TENT_LOD:
                min_filter = NV_PGRAPH_TEXFILTER0_MIN_TENT_LOD0;
                break;
            }
        }

        glTexParameteri(binding->gl_target, GL_TEXTURE_MIN_FILTER,
            pgraph_texture_min_filter_map[min_filter]);
        glTexParameteri(binding->gl_target, GL_TEXTURE_MAG_FILTER,
            pgraph_texture_mag_filter_map[mag_filter]);

        /* Texture wrapping */
        assert(addru < ARRAY_SIZE(pgraph_texture_addr_map));
        glTexParameteri(binding->gl_target, GL_TEXTURE_WRAP_S,
            pgraph_texture_addr_map[addru]);
        if (dimensionality > 1) {
            assert(addrv < ARRAY_SIZE(pgraph_texture_addr_map));
        glTexParameteri(binding->gl_target, GL_TEXTURE_WRAP_T,
                pgraph_texture_addr_map[addrv]);
        }
        if (dimensionality > 2) {
            assert(addrp < ARRAY_SIZE(pgraph_texture_addr_map));
            glTexParameteri(binding->gl_target, GL_TEXTURE_WRAP_R,
                pgraph_texture_addr_map[addrp]);
        }

        /* FIXME: Only upload if necessary? [s, t or r = GL_CLAMP_TO_BORDER] */
        if (border_source == NV_PGRAPH_TEXFMT0_BORDER_SOURCE_COLOR) {
            GLfloat gl_border_color[] = {
                /* FIXME: Color channels might be wrong order */
                ((border_color >> 16) & 0xFF) / 255.0f, /* red */
                ((border_color >> 8) & 0xFF) / 255.0f,  /* green */
                (border_color & 0xFF) / 255.0f,         /* blue */
                ((border_color >> 24) & 0xFF) / 255.0f  /* alpha */
            };
            glTexParameterfv(binding->gl_target, GL_TEXTURE_BORDER_COLOR,
                gl_border_color);
        }

        if (pg->texture_binding[i]) {
            texture_binding_destroy(pg->texture_binding[i]);
        }
        pg->texture_binding[i] = binding;
        pg->texture_dirty[i] = false;
    }
    NV2A_GL_DGROUP_END();
}

static void pgraph_apply_anti_aliasing_factor(PGRAPHState *pg,
                                              unsigned int *width,
                                              unsigned int *height)
{
    switch (pg->surface_shape.anti_aliasing) {
    case NV097_SET_SURFACE_FORMAT_ANTI_ALIASING_CENTER_1:
        break;
    case NV097_SET_SURFACE_FORMAT_ANTI_ALIASING_CENTER_CORNER_2:
        if (width) { *width *= 2; }
        break;
    case NV097_SET_SURFACE_FORMAT_ANTI_ALIASING_SQUARE_OFFSET_4:
        if (width) { *width *= 2; }
        if (height) { *height *= 2; }
        break;
    default:
        assert(false);
        break;
    }
}

static void pgraph_get_surface_dimensions(PGRAPHState *pg,
                                          unsigned int *width,
                                          unsigned int *height)
{
    bool swizzle = (pg->surface_type == NV097_SET_SURFACE_FORMAT_TYPE_SWIZZLE);
    if (swizzle) {
        *width = 1 << pg->surface_shape.log_width;
        *height = 1 << pg->surface_shape.log_height;
    } else {
        *width = pg->surface_shape.clip_width;
        *height = pg->surface_shape.clip_height;
    }
}

#if TRACK_GEOMETRY_CACHE_STATS
int attr_cache_hit;
int attr_cache_miss;
int attr_cache_mem_upload;
int attr_cache_mem_upload2;
#endif

static void pgraph_update_memory_buffer(NV2AState *d, hwaddr addr, hwaddr size,
                                        bool f)
{
    glBindBuffer(GL_ARRAY_BUFFER, d->pgraph.gl_memory_buffer);

    hwaddr end = TARGET_PAGE_ALIGN(addr + size);
    addr &= TARGET_PAGE_MASK;
    assert(end < memory_region_size(d->vram));
    
    // qemu_mutex_lock_iothread();
    // memory_global_dirty_log_sync();
    // qemu_mutex_unlock_iothread();


    // Slow either way, better to use hashing?
#if 0
    // f = true;
#else
    // memory_global_dirty_log_sync();
#endif

    if (f || memory_region_test_and_clear_dirty(d->vram,
                                                addr,
                                                end - addr,
                                                DIRTY_MEMORY_NV2A)) {
#if TRACK_GEOMETRY_CACHE_STATS
        attr_cache_mem_upload++;
#endif
        SDPRINTF("....-> Actually uploading\n");
        glBufferSubData(GL_ARRAY_BUFFER, addr, end - addr, d->vram_ptr + addr);
        // glFinish();
    } else {
        SDPRINTF(" skipped!\n");
    }
}


static void pgraph_bind_vertex_attributes(NV2AState *d,
                                          unsigned int num_elements,
                                          bool inline_data,
                                          unsigned int inline_stride)
{
    int i, j;
    PGRAPHState *pg = &d->pgraph;

    if (inline_data) {
        NV2A_GL_DGROUP_BEGIN("%s (num_elements: %d inline stride: %d)",
                             __func__, num_elements, inline_stride);
    } else {
        NV2A_GL_DGROUP_BEGIN("%s (num_elements: %d)", __func__, num_elements);
    }

    for (i=0; i<NV2A_VERTEXSHADER_ATTRIBUTES; i++) {

        SDPRINTF("--VSHADER ATTR %d\n", i);

        VertexAttribute *attribute = &pg->vertex_attributes[i];
        if (attribute->count) {
            uint8_t *data;
            unsigned int in_stride;
            if (inline_data && attribute->needs_conversion) {
                data = (uint8_t*)pg->inline_array
                        + attribute->inline_array_offset;
                in_stride = inline_stride;
            } else {
                hwaddr dma_len;
                if (attribute->dma_select) {
                    data = (uint8_t*)nv_dma_map(d, pg->dma_vertex_b, &dma_len);
                } else {
                    data = (uint8_t*)nv_dma_map(d, pg->dma_vertex_a, &dma_len);
                }

                assert(attribute->offset < dma_len);
                data += attribute->offset;

                in_stride = attribute->stride;
            }

            if (attribute->needs_conversion) {
                NV2A_DPRINTF("converted %d\n", i);

                unsigned int out_stride = attribute->converted_size
                                        * attribute->converted_count;

                if (num_elements > attribute->converted_elements) {
                    attribute->converted_buffer = (uint8_t*)g_realloc(
                        attribute->converted_buffer,
                        num_elements * out_stride);
                }



#if USE_GEOMETRY_CACHE
                uint64_t geom_hash = fast_hash((const unsigned char *)data, num_elements * in_stride, 0);

                GeometryKey key_in = {
                    .buffer_type = GL_ARRAY_BUFFER,
                    .buffer_length = num_elements * out_stride,
                    .populated = 0
                };

                struct lru_node *found = lru_lookup(&pg->converted_buffer_cache, geom_hash, &key_in);
                GeometryKey *key_out = container_of(found, struct GeometryKey, node);
                assert(key_out != NULL);
                glBindBuffer(GL_ARRAY_BUFFER, key_out->buffer_id);
                SDPRINTF("Uploading inline elements %zd, # %016lx ", num_elements, geom_hash);
                if (!key_out->populated) {
                    SDPRINTF("....uploading\n");

#if TRACK_GEOMETRY_CACHE_STATS
                    attr_cache_miss++;
#endif

                    for (j=attribute->converted_elements; j<num_elements; j++) {
                        uint8_t *in = data + j * in_stride;
                        uint8_t *out = attribute->converted_buffer + j * out_stride;

                        switch (attribute->format) {
                        case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_CMP: {
                            uint32_t p = ldl_le_p((uint32_t*)in);
                            float *xyz = (float*)out;
                            xyz[0] = ((int32_t)(((p >>  0) & 0x7FF) << 21) >> 21)
                                                                          / 1023.0f;
                            xyz[1] = ((int32_t)(((p >> 11) & 0x7FF) << 21) >> 21)
                                                                          / 1023.0f;
                            xyz[2] = ((int32_t)(((p >> 22) & 0x3FF) << 22) >> 22)
                                                                           / 511.0f;
                            break;
                        }
                        default:
                            assert(false);
                            break;
                        }
                    }


                    glBufferData(GL_ARRAY_BUFFER,
                                 num_elements * out_stride,
                                 attribute->converted_buffer,
                                 GL_DYNAMIC_DRAW);
                    attribute->converted_elements = num_elements;
                    key_out->populated = 1;
                } else {
                    SDPRINTF("Re-using buffer!\n");
#if TRACK_GEOMETRY_CACHE_STATS
                    attr_cache_hit++;
#endif
                }
#else

                for (j=attribute->converted_elements; j<num_elements; j++) {
                    uint8_t *in = data + j * in_stride;
                    uint8_t *out = attribute->converted_buffer + j * out_stride;

                    switch (attribute->format) {
                    case NV097_SET_VERTEX_DATA_ARRAY_FORMAT_TYPE_CMP: {
                        uint32_t p = ldl_le_p((uint32_t*)in);
                        float *xyz = (float*)out;
                        xyz[0] = ((int32_t)(((p >>  0) & 0x7FF) << 21) >> 21)
                                                                      / 1023.0f;
                        xyz[1] = ((int32_t)(((p >> 11) & 0x7FF) << 21) >> 21)
                                                                      / 1023.0f;
                        xyz[2] = ((int32_t)(((p >> 22) & 0x3FF) << 22) >> 22)
                                                                       / 511.0f;
                        break;
                    }
                    default:
                        assert(false);
                        break;
                    }
                }

                SDPRINTF("Updating gl_converted_buffer\n");
                glBindBuffer(GL_ARRAY_BUFFER, attribute->gl_converted_buffer);
                if (num_elements != attribute->converted_elements) {
                    SDPRINTF(".....Uploading %d elements\n", num_elements);
                    glBufferData(GL_ARRAY_BUFFER,
                                 num_elements * out_stride,
                                 attribute->converted_buffer,
                                 GL_DYNAMIC_DRAW);
                    attribute->converted_elements = num_elements;
                }

#endif

                glVertexAttribPointer(i,
                    attribute->converted_count,
                    attribute->gl_type,
                    attribute->gl_normalize,
                    out_stride,
                    0);
            } else if (inline_data) {
                SDPRINTF("Binding gl_inline_array_buffer\n");
#if TRACK_GEOMETRY_CACHE_STATS
                attr_cache_mem_upload2++;
#endif
                glBindBuffer(GL_ARRAY_BUFFER, pg->gl_inline_array_buffer);
                glVertexAttribPointer(i,
                                      attribute->gl_count,
                                      attribute->gl_type,
                                      attribute->gl_normalize,
                                      inline_stride,
                                      (void*)(uintptr_t)attribute->inline_array_offset);
            } else {
                SDPRINTF("Updating memory buffer... %d * %d\n", num_elements, attribute->stride);
                // attr_cache_mem_upload++;
                hwaddr addr = data - d->vram_ptr;
                pgraph_update_memory_buffer(d, addr,
                                            num_elements * attribute->stride,
                                            false);
                glVertexAttribPointer(i,
                    attribute->gl_count,
                    attribute->gl_type,
                    attribute->gl_normalize,
                    attribute->stride,
                    (void*)(uint64_t)addr);
            }
            glEnableVertexAttribArray(i);
        } else {
            glDisableVertexAttribArray(i);

            glVertexAttrib4fv(i, attribute->inline_value);
        }
    }
    NV2A_GL_DGROUP_END();
}

static unsigned int pgraph_bind_inline_array(NV2AState *d)
{
    int i;

    PGRAPHState *pg = &d->pgraph;

    unsigned int offset = 0;
    for (i=0; i<NV2A_VERTEXSHADER_ATTRIBUTES; i++) {
        VertexAttribute *attribute = &pg->vertex_attributes[i];
        if (attribute->count) {
            attribute->inline_array_offset = offset;

            NV2A_DPRINTF("bind inline attribute %d size=%d, count=%d\n",
                i, attribute->size, attribute->count);
            offset += attribute->size * attribute->count;
            assert(offset % 4 == 0);
        }
    }

    unsigned int vertex_size = offset;


    unsigned int index_count = pg->inline_array_length*4 / vertex_size;

    NV2A_DPRINTF("draw inline array %d, %d\n", vertex_size, index_count);

#if USE_GEOMETRY_CACHE
    uint64_t geom_hash = fast_hash((const unsigned char *)pg->inline_array, pg->inline_array_length*4, 2020);

    GeometryKey key_in = {
        .buffer_type = GL_ARRAY_BUFFER,
        .buffer_length = pg->inline_array_length*4,
        .populated = 0
    };

    struct lru_node *found = lru_lookup(&pg->inline_array_cache, geom_hash, &key_in);
    GeometryKey *key_out = container_of(found, struct GeometryKey, node);
    assert(key_out != NULL);
    SDPRINTF("binding buffer %d\n", key_out->buffer_id);
    glBindBuffer(GL_ARRAY_BUFFER, key_out->buffer_id);
    pg->gl_inline_array_buffer = key_out->buffer_id;
    SDPRINTF("Uploading inline elements %zd, # %016lx ", pg->inline_array_length, geom_hash);
    if (!key_out->populated) {
        SDPRINTF("....uploading\n");
        glBufferData(GL_ARRAY_BUFFER,
                     pg->inline_array_length*4,
                     pg->inline_array,
                     GL_DYNAMIC_DRAW);
        SDPRINTF("done\n");
        key_out->populated = 1;
    } else {
        SDPRINTF("Re-using buffer!\n");
    }
#else
    glBindBuffer(GL_ARRAY_BUFFER, pg->gl_inline_array_buffer);
    glBufferData(GL_ARRAY_BUFFER, pg->inline_array_length*4, pg->inline_array,
                 GL_DYNAMIC_DRAW);
#endif
    SDPRINTF("binding vattrs\n");
    pgraph_bind_vertex_attributes(d, index_count, true, vertex_size);
    SDPRINTF("ok\n");

    return index_count;
}

/* 16 bit to [0.0, F16_MAX = 511.9375] */
static float convert_f16_to_float(uint16_t f16) {
    if (f16 == 0x0000) { return 0.0; }
    uint32_t i = (f16 << 11) + 0x3C000000;
    return *(float*)&i;
}

/* 24 bit to [0.0, F24_MAX] */
static float convert_f24_to_float(uint32_t f24) {
    assert(!(f24 >> 24));
    f24 &= 0xFFFFFF;
    if (f24 == 0x000000) { return 0.0; }
    uint32_t i = f24 << 7;
    return *(float*)&i;
}

static uint8_t cliptobyte(int x)
{
    return (uint8_t)((x < 0) ? 0 : ((x > 255) ? 255 : x));
}

static void convert_yuy2_to_rgb(const uint8_t *line, unsigned int ix,
                                uint8_t *r, uint8_t *g, uint8_t* b) {
    int c, d, e;
    c = (int)line[ix * 2] - 16;
    if (ix % 2) {
        d = (int)line[ix * 2 - 1] - 128;
        e = (int)line[ix * 2 + 1] - 128;
    } else {
        d = (int)line[ix * 2 + 1] - 128;
        e = (int)line[ix * 2 + 3] - 128;
    }
    *r = cliptobyte((298 * c + 409 * e + 128) >> 8);
    *g = cliptobyte((298 * c - 100 * d - 208 * e + 128) >> 8);
    *b = cliptobyte((298 * c + 516 * d + 128) >> 8);
}

static uint8_t* convert_texture_data(const TextureShape s,
                                     const uint8_t *data,
                                     const uint8_t *palette_data,
                                     unsigned int width,
                                     unsigned int height,
                                     unsigned int depth,
                                     unsigned int row_pitch,
                                     unsigned int slice_pitch)
{
    if (s.color_format == NV097_SET_TEXTURE_FORMAT_COLOR_SZ_I8_A8R8G8B8) {
        assert(depth == 1); /* FIXME */
        uint8_t* converted_data = (uint8_t*)g_malloc(width * height * 4);
        int x, y;
        for (y = 0; y < height; y++) {
            for (x = 0; x < width; x++) {
                uint8_t index = data[y * row_pitch + x];
                uint32_t color = *(uint32_t*)(palette_data + index * 4);
                *(uint32_t*)(converted_data + y * width * 4 + x * 4) = color;
            }
        }
        return converted_data;
    } else if (s.color_format
                   == NV097_SET_TEXTURE_FORMAT_COLOR_LC_IMAGE_CR8YB8CB8YA8) {
        assert(depth == 1); /* FIXME */
        uint8_t* converted_data = (uint8_t*)g_malloc(width * height * 4);
        int x, y;
        for (y = 0; y < height; y++) {
            const uint8_t* line = &data[y * s.width * 2];
            for (x = 0; x < width; x++) {
                uint8_t* pixel = &converted_data[(y * s.width + x) * 4];
                /* FIXME: Actually needs uyvy? */
                convert_yuy2_to_rgb(line, x, &pixel[0], &pixel[1], &pixel[2]);
                pixel[3] = 255;
          }
        }
        return converted_data;
    } else if (s.color_format
                   == NV097_SET_TEXTURE_FORMAT_COLOR_SZ_R6G5B5) {
        assert(depth == 1); /* FIXME */
        uint8_t *converted_data = (uint8_t*)g_malloc(width * height * 3);
        int x, y;
        for (y = 0; y < height; y++) {
            for (x = 0; x < width; x++) {
                uint16_t rgb655 = *(uint16_t*)(data + y * row_pitch + x * 2);
                int8_t *pixel = (int8_t*)&converted_data[(y * width + x) * 3];
                /* Maps 5 bit G and B signed value range to 8 bit
                 * signed values. R is probably unsigned.
                 */
                rgb655 ^= (1 << 9) | (1 << 4);
                pixel[0] = ((rgb655 & 0xFC00) >> 10) * 0x7F / 0x3F;
                pixel[1] = ((rgb655 & 0x03E0) >> 5) * 0xFF / 0x1F - 0x80;
                pixel[2] = (rgb655 & 0x001F) * 0xFF / 0x1F - 0x80;
            }
        }
        return converted_data;
    } else {
        return NULL;
    }
}

static void upload_gl_texture(GLenum gl_target,
                              const TextureShape s,
                              const uint8_t *texture_data,
                              const uint8_t *palette_data)
{
    ColorFormatInfo f = kelvin_color_format_map[s.color_format];

    // printf("tex upload\n");

    switch(gl_target) {
    case GL_TEXTURE_1D:
        assert(false);
        break;
    case GL_TEXTURE_RECTANGLE: {
        /* Can't handle strides unaligned to pixels */
        assert(s.pitch % f.bytes_per_pixel == 0);
        glPixelStorei(GL_UNPACK_ROW_LENGTH,
                      s.pitch / f.bytes_per_pixel);

        uint8_t *converted = convert_texture_data(s, texture_data,
                                                  palette_data,
                                                  s.width, s.height, 1,
                                                  s.pitch, 0);

        glTexImage2D(gl_target, 0, f.gl_internal_format,
                     s.width, s.height, 0,
                     f.gl_format, f.gl_type,
                     converted ? converted : texture_data);

        if (converted) {
          g_free(converted);
        }

        glPixelStorei(GL_UNPACK_ROW_LENGTH, 0);
        break;
    }
    case GL_TEXTURE_2D:
    case GL_TEXTURE_CUBE_MAP_POSITIVE_X:
    case GL_TEXTURE_CUBE_MAP_NEGATIVE_X:
    case GL_TEXTURE_CUBE_MAP_POSITIVE_Y:
    case GL_TEXTURE_CUBE_MAP_NEGATIVE_Y:
    case GL_TEXTURE_CUBE_MAP_POSITIVE_Z:
    case GL_TEXTURE_CUBE_MAP_NEGATIVE_Z: {

        unsigned int width = s.width, height = s.height;

#if PROFILE_TEXTURES
        const void *hashme = texture_data;
        size_t hashme_len = 0;
#endif

        int level;
        for (level = 0; level < s.levels; level++) {
            if (f.gl_format == 0) { /* compressed */

                width = MAX(width, 4); height = MAX(height, 4);

                unsigned int block_size;
                if (f.gl_internal_format == GL_COMPRESSED_RGBA_S3TC_DXT1_EXT) {
                    block_size = 8;
                } else {
                    block_size = 16;
                }


                TDPRINTF("Uploading compressed texture %d x %d, lev=%d...\n", width, height, level);

                glCompressedTexImage2D(gl_target, level, f.gl_internal_format,
                                       width, height, 0,
                                       width/4 * height/4 * block_size,
                                       texture_data);

                texture_data += width/4 * height/4 * block_size;

#if PROFILE_TEXTURES
                hashme_len += width/4 * height/4 * block_size;
#endif
            } else {

                width = MAX(width, 1); height = MAX(height, 1);

                unsigned int pitch = width * f.bytes_per_pixel;
                uint8_t *unswizzled = (uint8_t*)g_malloc(height * pitch);
                unswizzle_rect(texture_data, width, height,
                               unswizzled, pitch, f.bytes_per_pixel);

                uint8_t *converted = convert_texture_data(s, unswizzled,
                                                          palette_data,
                                                          width, height, 1,
                                                          pitch, 0);

                glTexImage2D(gl_target, level, f.gl_internal_format,
                             width, height, 0,
                             f.gl_format, f.gl_type,
                             converted ? converted : unswizzled);

                if (converted) {
                    g_free(converted);
                }
                g_free(unswizzled);

                texture_data += width * height * f.bytes_per_pixel;
            }

            width /= 2;
            height /= 2;
        }

#if PROFILE_TEXTURES
        if (hashme_len > 0) {
            printf("---> %016lx\n", fast_hash((const unsigned char *)hashme, hashme_len, 0));
        }
#endif

        break;
    }
    case GL_TEXTURE_3D: {

        unsigned int width = s.width, height = s.height, depth = s.depth;

        assert(f.gl_format != 0); /* FIXME: compressed not supported yet */
        assert(f.linear == false);

        int level;
        for (level = 0; level < s.levels; level++) {

            unsigned int row_pitch = width * f.bytes_per_pixel;
            unsigned int slice_pitch = row_pitch * height;
            uint8_t *unswizzled = (uint8_t*)g_malloc(slice_pitch * depth);
            unswizzle_box(texture_data, width, height, depth, unswizzled,
                           row_pitch, slice_pitch, f.bytes_per_pixel);

            uint8_t *converted = convert_texture_data(s, unswizzled,
                                                      palette_data,
                                                      width, height, depth,
                                                      row_pitch, slice_pitch);

            glTexImage3D(gl_target, level, f.gl_internal_format,
                         width, height, depth, 0,
                         f.gl_format, f.gl_type,
                         converted ? converted : unswizzled);

            if (converted) {
                g_free(converted);
            }
            g_free(unswizzled);

            texture_data += width * height * depth * f.bytes_per_pixel;

            width /= 2;
            height /= 2;
            depth /= 2;
        }
        break;
    }
    default:
        assert(false);
        break;
    }
}



static void generate_texture_upload(
    const TextureShape s,
    const uint8_t *texture_data,
    const uint8_t *palette_data,
    TextureBinding *binding)
{
    ColorFormatInfo f = kelvin_color_format_map[s.color_format];

    /* Create a new opengl texture */
    GLuint gl_texture = binding->gl_texture;
    GLenum gl_target = binding->gl_target;

    glBindTexture(gl_target, gl_texture);

    if (gl_target == GL_TEXTURE_CUBE_MAP) {

        ColorFormatInfo f = kelvin_color_format_map[s.color_format];
        unsigned int block_size;
        if (f.gl_internal_format == GL_COMPRESSED_RGBA_S3TC_DXT1_EXT) {
            block_size = 8;
        } else {
            block_size = 16;
        }

        size_t length = 0;
        unsigned int w = s.width, h = s.height;
        int level;
        for (level = 0; level < s.levels; level++) {
            if (f.gl_format == 0) {
                length += w/4 * h/4 * block_size;
            } else {
                length += w * h * f.bytes_per_pixel;
            }

            w /= 2;
            h /= 2;
        }

#if 0
        if ((f.gl_format == 0) && (s.width == s.height)) {
            // length += block_size; // 2x2
            // length += block_size; // 1x1
            // length += block_size * 2;
            length = (length + 127) & ~127;
        } else {
            // Ensure 64b alignment
            length = (length + 63) & ~63;
        }
#endif

        // FIXME: Addresses https://github.com/xqemu/xqemu/issues/145, needs PR
        length = (length + 127) & ~127;

        upload_gl_texture(GL_TEXTURE_CUBE_MAP_POSITIVE_X,
                          s, texture_data + 0 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_NEGATIVE_X,
                          s, texture_data + 1 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_POSITIVE_Y,
                          s, texture_data + 2 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_NEGATIVE_Y,
                          s, texture_data + 3 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_POSITIVE_Z,
                          s, texture_data + 4 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_NEGATIVE_Z,
                          s, texture_data + 5 * length, palette_data);
    } else {
        upload_gl_texture(gl_target, s, texture_data, palette_data);
    }

    /* Linear textures don't support mipmapping */
    if (!f.linear) {
        glTexParameteri(gl_target, GL_TEXTURE_BASE_LEVEL,
            s.min_mipmap_level);
        glTexParameteri(gl_target, GL_TEXTURE_MAX_LEVEL,
            s.levels - 1);
    }

    if (f.gl_swizzle_mask[0] != 0 || f.gl_swizzle_mask[1] != 0
        || f.gl_swizzle_mask[2] != 0 || f.gl_swizzle_mask[3] != 0) {
        glTexParameteriv(gl_target, GL_TEXTURE_SWIZZLE_RGBA,
                         (const GLint *)f.gl_swizzle_mask);
    }
}






static TextureBinding* generate_texture(const TextureShape s,
                                        const uint8_t *texture_data,
                                        const uint8_t *palette_data)
{
    ColorFormatInfo f = kelvin_color_format_map[s.color_format];

    /* Create a new opengl texture */
    GLuint gl_texture;
    glGenTextures(1, &gl_texture);

    TDPRINTF("Generated texture %d\n", gl_texture);

    GLenum gl_target;
    if (s.cubemap) {
        assert(f.linear == false);
        assert(s.dimensionality == 2);
        gl_target = GL_TEXTURE_CUBE_MAP;
    } else {
        if (f.linear) {
            /* linear textures use unnormalised texcoords.
             * GL_TEXTURE_RECTANGLE_ARB conveniently also does, but
             * does not allow repeat and mirror wrap modes.
             *  (or mipmapping, but xbox d3d says 'Non swizzled and non
             *   compressed textures cannot be mip mapped.')
             * Not sure if that'll be an issue. */

            /* FIXME: GLSL 330 provides us with textureSize()! Use that? */
            gl_target = GL_TEXTURE_RECTANGLE;
            assert(s.dimensionality == 2);
        } else {
            switch(s.dimensionality) {
            case 1: gl_target = GL_TEXTURE_1D; break;
            case 2: gl_target = GL_TEXTURE_2D; break;
            case 3: gl_target = GL_TEXTURE_3D; break;
            default:
                assert(false);
                break;
            }
        }
    }

#if 0
    glBindTexture(gl_target, gl_texture);

    NV2A_GL_DLABEL(GL_TEXTURE, gl_texture,
                   "format: 0x%02X%s, %d dimensions%s, width: %d, height: %d, depth: %d",
                   s.color_format, f.linear ? "" : " (SZ)",
                   s.dimensionality, s.cubemap ? " (Cubemap)" : "",
                   s.width, s.height, s.depth);

    if (gl_target == GL_TEXTURE_CUBE_MAP) {

        ColorFormatInfo f = kelvin_color_format_map[s.color_format];
        unsigned int block_size;
        if (f.gl_internal_format == GL_COMPRESSED_RGBA_S3TC_DXT1_EXT) {
            block_size = 8;
        } else {
            block_size = 16;
        }

        size_t length = 0;
        unsigned int w = s.width, h = s.height;
        int level;
        for (level = 0; level < s.levels; level++) {
            if (f.gl_format == 0) {
                length += w/4 * h/4 * block_size;
            } else {
                length += w * h * f.bytes_per_pixel;
            }

            w /= 2;
            h /= 2;
        }

#if 0
        if ((f.gl_format == 0) && (s.width == s.height)) {
            // length += block_size; // 2x2
            // length += block_size; // 1x1
            // length += block_size * 2;
            length = (length + 127) & ~127;
        } else {
            // Ensure 64b alignment
            length = (length + 63) & ~63;
        }
#endif

        // FIXME: Addresses https://github.com/xqemu/xqemu/issues/145, needs PR
        length = (length + 127) & ~127;

        upload_gl_texture(GL_TEXTURE_CUBE_MAP_POSITIVE_X,
                          s, texture_data + 0 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_NEGATIVE_X,
                          s, texture_data + 1 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_POSITIVE_Y,
                          s, texture_data + 2 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_NEGATIVE_Y,
                          s, texture_data + 3 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_POSITIVE_Z,
                          s, texture_data + 4 * length, palette_data);
        upload_gl_texture(GL_TEXTURE_CUBE_MAP_NEGATIVE_Z,
                          s, texture_data + 5 * length, palette_data);
    } else {
        upload_gl_texture(gl_target, s, texture_data, palette_data);
    }

    /* Linear textures don't support mipmapping */
    if (!f.linear) {
        glTexParameteri(gl_target, GL_TEXTURE_BASE_LEVEL,
            s.min_mipmap_level);
        glTexParameteri(gl_target, GL_TEXTURE_MAX_LEVEL,
            s.levels - 1);
    }

    if (f.gl_swizzle_mask[0] != 0 || f.gl_swizzle_mask[1] != 0
        || f.gl_swizzle_mask[2] != 0 || f.gl_swizzle_mask[3] != 0) {
        glTexParameteriv(gl_target, GL_TEXTURE_SWIZZLE_RGBA,
                         (const GLint *)f.gl_swizzle_mask);
    }
#endif

    TextureBinding* ret = (TextureBinding *)g_malloc(sizeof(TextureBinding));
    ret->gl_target = gl_target;
    ret->gl_texture = gl_texture;
    ret->refcnt = 1;

    generate_texture_upload(s, texture_data, palette_data, ret);

    return ret;
}

#if TRACK_GEOMETRY_CACHE_STATS
int geo_cache_hit;
int geo_cache_miss;
int geo_cache_retire;
#endif

#if USE_GEOMETRY_CACHE

// New geometry cache
struct lru_node *gce_init(struct lru_node *obj, void *key)
{
    struct GeometryKey *k_out = container_of(obj, struct GeometryKey, node);
    struct GeometryKey *k_in = (struct GeometryKey *)key;
    memcpy(k_out, k_in, sizeof(struct GeometryKey));
    glGenBuffers(1, &k_out->buffer_id);

#if TRACK_GEOMETRY_CACHE_STATS
    geo_cache_miss++;
#endif

    return obj;
}

struct lru_node *gce_deinit(struct lru_node *obj)
{
    struct GeometryKey *a = container_of(obj, struct GeometryKey, node);
    SDPRINTF("Evicting from geometry cache!\n");
    glDeleteBuffers(1, &a->buffer_id);

#if TRACK_GEOMETRY_CACHE_STATS
    geo_cache_retire++;
#endif

    return obj;
}

int gce_key_compare(struct lru_node *obj, void *key)
{
    struct GeometryKey *a = container_of(obj, struct GeometryKey, node);
    struct GeometryKey *b = (struct GeometryKey *)key;
    if ((a->buffer_type != b->buffer_type) ||
           (a->buffer_length != b->buffer_length)) {
        return 1;
    }

#if TRACK_GEOMETRY_CACHE_STATS
    geo_cache_hit++;
#endif

    return 0;
}

#endif


#if USE_UBO

// This is shit

struct lru_node *uboce_init(struct lru_node *obj, void *key)
{
    struct UboCacheKey *k_out = container_of(obj, struct UboCacheKey, node);
    struct UboCacheKey *k_in = (struct UboCacheKey *)key;
    memcpy(k_out, k_in, sizeof(struct UboCacheKey));

    // Align constants for faster copy
    size_t len = 4*4*NV2A_VERTEXSHADER_CONSTANTS;
    glGenBuffers(1, &k_out->buffer_id);
    // glBindBuffer(GL_UNIFORM_BUFFER, k_out->buffer_id);
    // glBufferData(GL_UNIFORM_BUFFER, len, 0, GL_DYNAMIC_DRAW);
    // glBindBufferRange(GL_UNIFORM_BUFFER, 0, k_out->buffer_id, 0, len);

    return obj;
}

struct lru_node *uboce_deinit(struct lru_node *obj)
{
    struct UboCacheKey *a = container_of(obj, struct UboCacheKey, node);
    glDeleteBuffers(1, &a->buffer_id);
    return obj;
}

int uboce_key_compare(struct lru_node *obj, void *key)
{
    struct UboCacheKey *a = container_of(obj, struct UboCacheKey, node);
    struct UboCacheKey *b = (struct UboCacheKey *)key;
    if ((a->buffer_type != b->buffer_type) ||
           (a->buffer_length != b->buffer_length)) {
        return 1;
    }

    return 0;
}

#endif

static void texture_binding_destroy(gpointer data)
{
    TextureBinding *binding = (TextureBinding *)data;
    assert(binding->refcnt > 0);
    binding->refcnt--;
    if (binding->refcnt == 0) {
        glDeleteTextures(1, &binding->gl_texture);
        g_free(binding);
    }
}

#if TRACK_LOCATION_CACHE_STATS
int loc_cache_hit = 0;
int loc_cache_miss = 0;
int loc_cache_false_dirty = 0;
// QemuSpin loc_cache_stats_lock;
#endif

#if USE_TEXTURE_LOCATION_CACHE
static struct lru_node *texture_location_cache_entry_init(struct lru_node *obj, void *key)
{
    // Initializing a new location cache entry
    struct TextureLocationKey *k_out = container_of(obj, struct TextureLocationKey, node);
    struct TextureLocationKey *k_in = (struct TextureLocationKey *)key;

    memcpy(k_out, k_in, sizeof(struct TextureLocationKey));

    // Lookup in the texture data cache... we may have stored it before
    // If not, the texture will be created and bound to this location

    TextureKey tc_key = {
        .state = k_in->state,
        .texture_data = k_in->texture_data,
        .palette_data = k_in->palette_data,
    };

    uint64_t hash = fast_hash(k_in->texture_data, k_in->texture_len, 5003)
                  ^ fast_hash(k_in->palette_data, k_in->palette_len, 5002);

    struct lru_node *found = lru_lookup(&fuck_fuck_fixme->pgraph.texture_cache, hash, &tc_key);
    TextureKey *tc_k_out = container_of(found, struct TextureKey, node);
    assert((tc_k_out != NULL) && (tc_k_out->binding != NULL));
    k_out->binding = tc_k_out->binding;
    k_out->hash = hash;


#if TRACK_LOCATION_CACHE_STATS
        loc_cache_miss++;
#endif

    return obj;
}

static struct lru_node *texture_location_cache_entry_deinit(struct lru_node *obj)
{
    // struct TextureLocationKey *a = container_of(obj, struct TextureLocationKey, node);
    // fixme: dec refcount on binding?

    return obj;
}


static int texture_location_cache_entry_compare(struct lru_node *obj, void *key)
{

#define CHECK_FALSE_CLEAN 0 // Check for false clean reports (keep this off for perf)
    // FIXME: This happens, but pretty rarely and it's not noticable... why? Are
    // we clearing this range somewhere else? -- Prob due to dirty bmp sync

    struct TextureLocationKey *a = container_of(obj, struct TextureLocationKey, node);
    struct TextureLocationKey *b = (struct TextureLocationKey *)key;

    if (a->texture_data != b->texture_data) return 1;
    if (a->texture_len  != b->texture_len) return 1;
    if (a->palette_data != b->palette_data) return 1;
    if (a->palette_len  != b->palette_len) return 1;

    int state_equ = memcmp(&a->state, &b->state, sizeof(a->state));
    if (state_equ) {
        // State not equal
        return state_equ;
    }

#if CHECK_FALSE_CLEAN
        uint64_t hash = fast_hash(a->texture_data, a->texture_len, 5003)
                      ^ fast_hash(a->palette_data, a->palette_len, 5002);
#endif

    // State equal... check to see if the memory was touched
    bool texture_dirty = memory_region_test_and_clear_dirty(fuck_fuck_fixme->vram,
        a->texture_data - fuck_fuck_fixme->vram_ptr, a->texture_len, DIRTY_MEMORY_VGA);

    if (a->palette_len > 0) {
        // Check palette too
        texture_dirty |= memory_region_test_and_clear_dirty(fuck_fuck_fixme->vram,
            a->palette_data - fuck_fuck_fixme->vram_ptr, a->palette_len, DIRTY_MEMORY_VGA);
    }

    if (!texture_dirty) {
        // printf("location cache success!\n");

#if CHECK_FALSE_CLEAN // Check for false clean reports (keep this off for perf)
        if (hash != a->hash) {
            printf("FALSE REPORT OF CLEAN!\n");
        }
#endif

#if TRACK_LOCATION_CACHE_STATS
        loc_cache_hit++;
#endif

        return 0;
    }

    // printf("location cache dirty! hashing texture data...\n");

    // CPU has touched the texture data at this location!
    // Check texture *data* cache if we have previous data

    TextureKey tc_key = {
        .state = a->state,
        .texture_data = a->texture_data,
        .palette_data = a->palette_data,
    };

#if !CHECK_FALSE_CLEAN
    uint64_t hash = fast_hash(a->texture_data, a->texture_len, 5003)
                  ^ fast_hash(a->palette_data, a->palette_len, 5002);
#endif

    struct lru_node *found = lru_lookup(&fuck_fuck_fixme->pgraph.texture_cache, hash, &tc_key);
    TextureKey *key_out = container_of(found, struct TextureKey, node);
    assert((key_out != NULL) && (key_out->binding != NULL));

    if (a->binding == key_out->binding) {
        // FALSE REPORT OF DIRTY! (something else on shared page?)
#if TRACK_LOCATION_CACHE_STATS
        loc_cache_false_dirty++;
#endif
        // printf("--> memory region reported dirty but texture is the same!\n");
    } else {
        // Update texture location key with current binding
        a->binding = key_out->binding;
        a->hash = hash;
    }
    // fixme: dec refcount on old binding?

    return 0;
}
#endif






/* functions for texture LRU cache */
static struct lru_node *texture_cache_entry_init(struct lru_node *obj, void *key)
{
    struct TextureKey *k_out = container_of(obj, struct TextureKey, node);
    struct TextureKey *k_in = (struct TextureKey *)key;
    memcpy(k_out, k_in, sizeof(struct TextureKey));
    k_out->binding = generate_texture(k_in->state,
                                      k_in->texture_data,
                                      k_in->palette_data);

    // k_out->data_hash = fast_hash(k_in->texture_data, k_in->texture_len, 5003)
    //                  ^ fnv_hash(k_in->palette_data, k_in->palette_len);
    return obj;
}

static struct lru_node *texture_cache_entry_deinit(struct lru_node *obj)
{
    struct TextureKey *a = container_of(obj, struct TextureKey, node);
    texture_binding_destroy(a->binding);
    return obj;
}

static int texture_cache_entry_compare(struct lru_node *obj, void *key)
{
    struct TextureKey *a = container_of(obj, struct TextureKey, node);
    struct TextureKey *b = (struct TextureKey *)key;
    return memcmp(&a->state, &b->state, sizeof(a->state));

#if 0
    if (a->texture_data_offset != b->texture_data_offset) return 1;
    if (a->texture_len         != b->texture_len) return 1;
    if (a->palette_data_offset != b->palette_data_offset) return 1;
    if (a->palette_len         != b->palette_len) return 1;

    int state_equ = memcmp(&a->state, &b->state, sizeof(a->state));
    if (state_equ) {
        // State not equal
        return state_equ;
    }

    // State equal... check to see if the memory was touched
    bool texture_dirty = memory_region_test_and_clear_dirty(fuck_fuck_fixme->vram,
        a->texture_data_offset, a->texture_len, DIRTY_MEMORY_VGA);

    if (a->palette_len > 0) {
        // Check palette too
        texture_dirty |= memory_region_test_and_clear_dirty(fuck_fuck_fixme->vram,
            a->palette_data_offset, a->palette_len, DIRTY_MEMORY_VGA);
    }


    uint64_t hash = fast_hash(a->texture_data, a->texture_len, 5003)
                     ^ fnv_hash(a->palette_data, a->palette_len);

    if (!texture_dirty) {
        // if (a->data_hash != hash) {
        //     printf("hash does not agree with memory! %016x v %016x\n", a->data_hash, hash);
        // }

        return 0;
    }

        // CPU touched the texture, re-upload it
        // [or return 1 here and let the LRU just create a new entry ---
        // cpu could ping-pong and we'd be better off hashing in that case as a fallback]
        #if 1
        printf("re-uploading!\n");
        generate_texture_upload(a->state, a->texture_data, a->palette_data, a->binding);
        #else
        printf("dirty.. should create!\n");
        return 1;
        #endif

    return 0;
#endif
}

/* hash and equality for shader cache hash table */
static guint vertex_shader_hash(gconstpointer key)
{
    return fnv_hash((const uint8_t *)key, sizeof(VertexShaderState));
}
static gboolean vertex_shader_equal(gconstpointer a, gconstpointer b)
{
    const VertexShaderState *as = (const VertexShaderState *)a, *bs = (const VertexShaderState *)b;
    return memcmp(as, bs, sizeof(VertexShaderState)) == 0;
}

/* hash and equality for shader cache hash table */
static guint fragment_shader_hash(gconstpointer key)
{
    return fnv_hash((const uint8_t *)key, sizeof(FragmentShaderState));
}
static gboolean fragment_shader_equal(gconstpointer a, gconstpointer b)
{
    const FragmentShaderState *as = (const FragmentShaderState *)a, *bs = (const FragmentShaderState *)b;
    return memcmp(as, bs, sizeof(FragmentShaderState)) == 0;
}

static unsigned int kelvin_map_stencil_op(uint32_t parameter)
{
    unsigned int op;
    switch (parameter) {
    case NV097_SET_STENCIL_OP_V_KEEP:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_KEEP; break;
    case NV097_SET_STENCIL_OP_V_ZERO:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_ZERO; break;
    case NV097_SET_STENCIL_OP_V_REPLACE:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_REPLACE; break;
    case NV097_SET_STENCIL_OP_V_INCRSAT:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_INCRSAT; break;
    case NV097_SET_STENCIL_OP_V_DECRSAT:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_DECRSAT; break;
    case NV097_SET_STENCIL_OP_V_INVERT:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_INVERT; break;
    case NV097_SET_STENCIL_OP_V_INCR:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_INCR; break;
    case NV097_SET_STENCIL_OP_V_DECR:
        op = NV_PGRAPH_CONTROL_2_STENCIL_OP_V_DECR; break;
    default:
        assert(false);
        break;
    }
    return op;
}

static unsigned int kelvin_map_polygon_mode(uint32_t parameter)
{
    unsigned int mode;
    switch (parameter) {
    case NV097_SET_FRONT_POLYGON_MODE_V_POINT:
        mode = NV_PGRAPH_SETUPRASTER_FRONTFACEMODE_POINT; break;
    case NV097_SET_FRONT_POLYGON_MODE_V_LINE:
        mode = NV_PGRAPH_SETUPRASTER_FRONTFACEMODE_LINE; break;
    case NV097_SET_FRONT_POLYGON_MODE_V_FILL:
        mode = NV_PGRAPH_SETUPRASTER_FRONTFACEMODE_FILL; break;
    default:
        assert(false);
        break;
    }
    return mode;
}

static unsigned int kelvin_map_texgen(uint32_t parameter, unsigned int channel)
{
    assert(channel < 4);
    unsigned int texgen;
    switch (parameter) {
    case NV097_SET_TEXGEN_S_DISABLE:
        texgen = NV_PGRAPH_CSV1_A_T0_S_DISABLE; break;
    case NV097_SET_TEXGEN_S_EYE_LINEAR:
        texgen = NV_PGRAPH_CSV1_A_T0_S_EYE_LINEAR; break;
    case NV097_SET_TEXGEN_S_OBJECT_LINEAR:
        texgen = NV_PGRAPH_CSV1_A_T0_S_OBJECT_LINEAR; break;
    case NV097_SET_TEXGEN_S_SPHERE_MAP:
        assert(channel < 2);
        texgen = NV_PGRAPH_CSV1_A_T0_S_SPHERE_MAP; break;
    case NV097_SET_TEXGEN_S_REFLECTION_MAP:
        assert(channel < 3);
        texgen = NV_PGRAPH_CSV1_A_T0_S_REFLECTION_MAP; break;
    case NV097_SET_TEXGEN_S_NORMAL_MAP:
        assert(channel < 3);
        texgen = NV_PGRAPH_CSV1_A_T0_S_NORMAL_MAP; break;
    default:
        assert(false);
        break;
    }
    return texgen;
}

static uint64_t fnv_hash(const uint8_t *data, size_t len)
{
    return XXH64(data, len, 0);
}

static uint64_t fast_hash(const uint8_t *data, size_t len, unsigned int samples)
{
    return XXH64(data, len, 0);;
}

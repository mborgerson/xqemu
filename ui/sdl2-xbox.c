/*
 * QEMU SDL display driver -- opengl support
 *
 * Copyright (c) 2018 Matt Borgerson
 *
 * Based on sdl2-gl.c
 *
 * Copyright (c) 2014 Red Hat
 *
 * Authors:
 *     Gerd Hoffmann <kraxel@redhat.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "qemu/osdep.h"
#include "qemu-common.h"
#include "ui/console.h"
#include "ui/input.h"
#include "ui/sdl2.h"
#include "sysemu/sysemu.h"

#include "qemu/thread.h"
#include "hw/xbox/nv2a/perf_config.h"

// Created in early startup
extern SDL_GLContext m_context;

// Framebuffer copy
GLuint m_vao;
GLuint m_vbo;
GLuint m_vert_shader;
GLuint m_frag_shader;
GLuint m_shader_prog;


GLuint flip_sync;

static void init_shaders(void);

// Framebuffer sync
QemuSpin avail_spinner;
extern volatile int available;
extern volatile GLuint fb_tex;
extern volatile GLsync fb_sync;

// FPS counter
static struct timeval tv_last;
static int tv_last_valid;
static int frames, updates;
static int second_elapse(void);

#if !USE_SHARED_CONTEXT
static void xb_surface_gl_create_texture(DisplaySurface *surface);
static void xb_surface_gl_update_texture(DisplaySurface *surface, int x, int y, int w, int h);
static void xb_surface_gl_destroy_texture(DisplaySurface *surface);
#endif

#if 0
static void sdl2_set_scanout_mode(struct sdl2_console *scon, bool scanout)
{
    if (scon->scanout_mode == scanout) {
        return;
    }

    scon->scanout_mode = scanout;
    if (!scon->scanout_mode) {
        egl_fb_destroy(&scon->guest_fb);
        if (scon->surface) {
            surface_gl_destroy_texture(scon->gls, scon->surface);
            surface_gl_create_texture(scon->gls, scon->surface);
        }
    }
}
#endif

extern int loc_cache_hit;
extern int loc_cache_miss;
extern int loc_cache_false_dirty;

static void sdl2_gl_render_surface(struct sdl2_console *scon)
{
    int ww, wh;

    SDL_GL_MakeCurrent(scon->real_window, scon->winctx);
    // sdl2_set_scanout_mode(scon, false);

    SDL_GetWindowSize(scon->real_window, &ww, &wh);
    // surface_gl_setup_viewport(scon->gls, scon->surface, ww, wh);

    // surface_gl_render_texture(scon->gls, scon->surface);
    // SDL_GL_SwapWindow(scon->real_window);

#if USE_SHARED_CONTEXT
    //
    // Read surface data directly from NV2A PGRAPH GL context
    //
    if (available) {
        GLuint display_tex;
        GLsync fence;


        qemu_spin_lock(&avail_spinner);
        display_tex = fb_tex;
        fence = fb_sync;
        // available = 0; // <--
        qemu_spin_unlock(&avail_spinner);

        if (display_tex) {

            // Make sure to wait for rendering to finish
            glWaitSync(fence, 0, GL_TIMEOUT_IGNORED);

            // Render the surface to this fbo
            glViewport(0, 0, ww, wh);
            glBindTexture(GL_TEXTURE_2D, display_tex);
            glClear(GL_COLOR_BUFFER_BIT);
            glDrawArrays(GL_TRIANGLES, 0, 3);

            // flip_sync = glFenceSync( GL_SYNC_GPU_COMMANDS_COMPLETE, 0 );
#if 0
        qemu_spin_lock(&avail_spinner);
        // display_tex = fb_tex;
        // fence = fb_sync;
        available = 0; // <--
        qemu_spin_unlock(&avail_spinner);
#endif
            // glBindTexture(GL_TEXTURE_2D, 0);

            SDL_GL_SwapWindow(scon->real_window);




#if 0
            if (second_elapse()) {
                printf("FPS: %d, UPDATES: %d\n", frames, updates);

                if (frames > 0) {
                    extern int shader_bindings;
                    // printf("Shader Bindings / frame: %d\n", shader_bindings/frames);
                    shader_bindings = 0;
                }
                

                if (frames > 0) {
                    extern int num_methods_executed;
                    // printf("[pgraph methods] #%d (avg = %d)\n", num_methods_executed, num_methods_executed/frames);
                    num_methods_executed = 0;
                }
                
                frames = 0;
                updates = 0;


#if TRACK_LOCATION_CACHE_STATS
                int total = loc_cache_hit + loc_cache_miss;

                // printf("[Loc Cache] Hit:%d, Miss:%d, False Miss:%d --> %.2f%%\n",
                //     loc_cache_hit,
                //     loc_cache_miss,
                //     loc_cache_false_dirty,
                //     total > 0 ? (float)(loc_cache_hit-loc_cache_false_dirty)/(float)total*100.0 : 0.0
                //     );
                loc_cache_hit = 0;
                loc_cache_miss = 0;
                loc_cache_false_dirty = 0;
#endif


#if TRACK_GEOMETRY_CACHE_STATS
                {
                extern int geo_cache_hit;
                extern int geo_cache_miss;
                extern int geo_cache_retire;
                int total = geo_cache_hit + geo_cache_miss;

                // printf("[geometry Cache] Hit:%d, Miss:%d, Retired:%d --> %.2f%%\n",
                //     geo_cache_hit,
                //     geo_cache_miss,
                //     geo_cache_retire,
                //     total > 0 ? (float)(geo_cache_hit)/(float)total*100.0 : 0.0
                //     );
                geo_cache_hit = 0;
                geo_cache_miss = 0;
                geo_cache_retire = 0;
                }
#endif

#if TRACK_GEOMETRY_CACHE_STATS
                {
                extern int attr_cache_hit;
                extern int attr_cache_miss;
                extern int attr_cache_mem_upload;
                extern int attr_cache_mem_upload2;
                int total = attr_cache_hit + attr_cache_miss;

                // printf("[attr Cache] Hit:%d, Miss:%d, Mem Upload: %d, %d --> %.2f%%\n",
                //     attr_cache_hit,
                //     attr_cache_miss,
                //     attr_cache_mem_upload,
                //     attr_cache_mem_upload2,
                //     total > 0 ? (float)(attr_cache_hit)/(float)total*100.0 : 0.0
                //     );
                attr_cache_hit = 0;
                attr_cache_miss = 0;
                attr_cache_mem_upload = 0;
                attr_cache_mem_upload2 = 0;
                }
#endif




            }
            frames++;
#endif
        }

#if 1
        qemu_spin_lock(&avail_spinner);
        // FIXME: Probably a timing bug in here...
        available = 0;
        qemu_spin_unlock(&avail_spinner);
#endif
    }

#else

    //
    // Surface data has been read from memory already into the bound texture
    //
    glViewport(0, 0, ww, wh);
    glClear(GL_COLOR_BUFFER_BIT);
    glDrawArrays(GL_TRIANGLES, 0, 3);
    SDL_GL_SwapWindow(scon->real_window);

#endif // USE_SHARED_CONTEXT

    updates++;
}

void sdl2_gl_update(DisplayChangeListener *dcl,
                    int x, int y, int w, int h)
{
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);

    assert(scon->opengl);

#if !USE_SHARED_CONTEXT
    SDL_GL_MakeCurrent(scon->real_window, scon->winctx);
    xb_surface_gl_update_texture(scon->surface, x, y, w, h);
#endif
    scon->updates++;
}

void sdl2_gl_switch(DisplayChangeListener *dcl,
                    DisplaySurface *new_surface)
{
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);
    DisplaySurface *old_surface = scon->surface;

    assert(scon->opengl);

    SDL_GL_MakeCurrent(scon->real_window, scon->winctx);

#if !USE_SHARED_CONTEXT
    xb_surface_gl_destroy_texture(scon->surface);
#endif

    scon->surface = new_surface;

    if (!new_surface) {
        // qemu_gl_fini_shader(scon->gls);
        // scon->gls = NULL;
#if !USE_SHARED_CONTEXT
        xb_surface_gl_destroy_texture(scon->surface);
#endif
        sdl2_window_destroy(scon);
        return;
    }

    if (!scon->real_window) {
        sdl2_window_create(scon);

        /* Make sure we are creating a new context */
        scon->winctx = dpy_gl_ctx_create(scon->dcl.con, NULL);

        // scon->gls = qemu_gl_init_shader();
    } else if (old_surface &&
               ((surface_width(old_surface)  != surface_width(new_surface)) ||
                (surface_height(old_surface) != surface_height(new_surface)))) {
#if !USE_SHARED_CONTEXT
        sdl2_window_resize(scon);
#endif
    }

#if !USE_SHARED_CONTEXT
    xb_surface_gl_create_texture(scon->surface);
#endif
}

void sdl2_gl_refresh(DisplayChangeListener *dcl)
{
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);

    assert(scon->opengl);

#if USE_SHARED_CONTEXT
    // Force redraw
    sdl2_gl_render_surface(scon);
#else
    if (scon->updates && scon->surface) {
        scon->updates = 0;
        sdl2_gl_render_surface(scon);
    }
#endif
    
    // trigger int after we grab the frame
    graphic_hw_update(dcl->con);

    sdl2_poll_events(scon);
}

void sdl2_gl_redraw(struct sdl2_console *scon)
{
    assert(scon->opengl);

    if (scon->surface) {
        // sdl2_gl_render_surface(scon);
    }
}

QEMUGLContext sdl2_gl_create_context(DisplayChangeListener *dcl,
                                     QEMUGLParams *params)
{
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);
    SDL_GLContext ctx;

    assert(scon->opengl);

    scon->winctx = m_context;

    SDL_GL_MakeCurrent(scon->real_window, scon->winctx);

#if 0
    SDL_GL_SetAttribute(SDL_GL_SHARE_WITH_CURRENT_CONTEXT, 1);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_PROFILE_MASK,
                        SDL_GL_CONTEXT_PROFILE_CORE);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MAJOR_VERSION, params->major_ver);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MINOR_VERSION, params->minor_ver);

    ctx = SDL_GL_CreateContext(scon->real_window);
#endif

    // Use context created at startup
    ctx = scon->winctx;

    init_shaders();
    glClearColor(0.0f, 0.0f, 0.0f, 1.0f);

    qemu_spin_init(&avail_spinner);

    return (QEMUGLContext)ctx;
}

void sdl2_gl_destroy_context(DisplayChangeListener *dcl, QEMUGLContext ctx)
{
    SDL_GLContext sdlctx = (SDL_GLContext)ctx;

    glUseProgram(0);
    glDisableVertexAttribArray(0);
    glDetachShader(m_shader_prog, m_vert_shader);
    glDetachShader(m_shader_prog, m_frag_shader);
    glDeleteProgram(m_shader_prog);
    glDeleteShader(m_vert_shader);
    glDeleteShader(m_frag_shader);
    // glDeleteTextures(1, &m_tex);
    glDeleteBuffers(1, &m_vbo);
    glDeleteVertexArrays(1, &m_vao);
    SDL_GL_DeleteContext(sdlctx);
}

int sdl2_gl_make_context_current(DisplayChangeListener *dcl,
                                 QEMUGLContext ctx)
{
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);
    SDL_GLContext sdlctx = (SDL_GLContext)ctx;

    assert(scon->opengl);

    return SDL_GL_MakeCurrent(scon->real_window, sdlctx);
}

QEMUGLContext sdl2_gl_get_current_context(DisplayChangeListener *dcl)
{
    SDL_GLContext sdlctx;

    sdlctx = SDL_GL_GetCurrentContext();
    return (QEMUGLContext)sdlctx;
}

void sdl2_gl_scanout_disable(DisplayChangeListener *dcl)
{
#if 0
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);

    assert(scon->opengl);
    scon->w = 0;
    scon->h = 0;
    sdl2_set_scanout_mode(scon, false);
#endif
}

void sdl2_gl_scanout_texture(DisplayChangeListener *dcl,
                             uint32_t backing_id,
                             bool backing_y_0_top,
                             uint32_t backing_width,
                             uint32_t backing_height,
                             uint32_t x, uint32_t y,
                             uint32_t w, uint32_t h)
{
#if 0
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);

    assert(scon->opengl);
    scon->x = x;
    scon->y = y;
    scon->w = w;
    scon->h = h;
    scon->y0_top = backing_y_0_top;

    SDL_GL_MakeCurrent(scon->real_window, scon->winctx);

    sdl2_set_scanout_mode(scon, true);
    egl_fb_setup_for_tex(&scon->guest_fb, backing_width, backing_height,
                         backing_id, false);
#endif
}

void sdl2_gl_scanout_flush(DisplayChangeListener *dcl,
                           uint32_t x, uint32_t y, uint32_t w, uint32_t h)
{
#if 0
    struct sdl2_console *scon = container_of(dcl, struct sdl2_console, dcl);
    int ww, wh;

    assert(scon->opengl);
    if (!scon->scanout_mode) {
        return;
    }
    if (!scon->guest_fb.framebuffer) {
        return;
    }

    SDL_GL_MakeCurrent(scon->real_window, scon->winctx);

    SDL_GetWindowSize(scon->real_window, &ww, &wh);
    egl_fb_setup_default(&scon->win_fb, ww, wh);
    egl_fb_blit(&scon->win_fb, &scon->guest_fb, !scon->y0_top);

    SDL_GL_SwapWindow(scon->real_window);
#endif
}

/**********************************************************/

//
// This vert+frag program renders one large triangle that
// covers the screen to display the bound texture.
//

// Via https://rauwendaal.net/2014/06/14/rendering-a-screen-covering-triangle-in-opengl/
static const char *vert_shader_src =
    "#version 150 core\n"
    "out vec2 texCoord;\n"
    "void main()\n"
    "{\n"
    "    float x = -1.0 + float((gl_VertexID & 1) << 2);\n"
    "    float y = -1.0 + float((gl_VertexID & 2) << 1);\n"
    "    texCoord.x = (x+1.0)*0.5;\n"
#if USE_SHARED_CONTEXT
    "    texCoord.y = (y+1.0)*0.5;\n"
#else
    // We flip the surface upside-down when writing out to memory
    "    texCoord.y = 1.0-(y+1.0)*0.5;\n"
#endif
    "    gl_Position = vec4(x, y, 0, 1);\n"
    "}\n";

static const char *frag_shader_src =
    "#version 150 core\n"
    "in vec2 texCoord;\n"
    "out vec4 out_Color;\n"
    "uniform sampler2D tex;\n"
    "void main()\n"
    "{\n"
        "out_Color.rgb = texture(tex, texCoord).rgb;\n"
        "out_Color.a = 1.0;\n"
    "}\n";

static void init_shaders(void)
{
    GLint status;
    char err_buf[512];

    glGenVertexArrays(1, &m_vao);
    glBindVertexArray(m_vao);

    // Compile vertex shader
    m_vert_shader = glCreateShader(GL_VERTEX_SHADER);
    glShaderSource(m_vert_shader, 1, &vert_shader_src, NULL);
    glCompileShader(m_vert_shader);
    glGetShaderiv(m_vert_shader, GL_COMPILE_STATUS, &status);
    if (status != GL_TRUE) {
        glGetShaderInfoLog(m_vert_shader, sizeof(err_buf), NULL, err_buf);
        err_buf[sizeof(err_buf)-1] = '\0';
        fprintf(stderr, "Vertex shader compilation failed: %s\n", err_buf);
        assert(false);
    }

    // Load fragment shader from source file
#if 0
    FILE *fd = fopen("frag.txt", "rb");
    if (fd == NULL) exit(1);
    fseek(fd, 0, SEEK_END);
    size_t len = ftell(fd);
    char *frag_txt = malloc(len+1);
    assert(frag_txt != NULL);
    fseek(fd, 0, SEEK_SET);
    fread(frag_txt, 1, len, fd);
    frag_txt[len] = '\x00';
#endif

    // Compile fragment shader
    m_frag_shader = glCreateShader(GL_FRAGMENT_SHADER);
    glShaderSource(m_frag_shader, 1, &frag_shader_src, NULL);
    // glShaderSource(m_frag_shader, 1, &frag_txt, NULL);
    glCompileShader(m_frag_shader);
    glGetShaderiv(m_frag_shader, GL_COMPILE_STATUS, &status);
    if (status != GL_TRUE) {
        glGetShaderInfoLog(m_frag_shader, sizeof(err_buf), NULL, err_buf);
        err_buf[sizeof(err_buf)-1] = '\0';
        fprintf(stderr, "Fragment shader compilation failed: %s\n", err_buf);
        assert(false);
    }

    // Link vertex and fragment shaders
    m_shader_prog = glCreateProgram();
    glAttachShader(m_shader_prog, m_vert_shader);
    glAttachShader(m_shader_prog, m_frag_shader);
    glBindFragDataLocation(m_shader_prog, 0, "out_Color");
    glLinkProgram(m_shader_prog);
    glUseProgram(m_shader_prog);

    // Create an empty vertex buffer
    glGenBuffers(1, &m_vbo);
    glBindBuffer(GL_ARRAY_BUFFER, m_vbo);
    glBufferData(GL_ARRAY_BUFFER, 0, NULL, GL_STATIC_DRAW);
}

//
// These functions originate from console-gl.c and are used
// when loading the framebuffer from memory into a texture.
//

#if !USE_SHARED_CONTEXT
static void xb_surface_gl_create_texture(DisplaySurface *surface)
{
    // assert(gls);

    assert(QEMU_IS_ALIGNED(surface_stride(surface), surface_bytes_per_pixel(surface)));

    switch (surface->format) {
    case PIXMAN_BE_b8g8r8x8:
    case PIXMAN_BE_b8g8r8a8:
        surface->glformat = GL_BGRA_EXT;
        surface->gltype = GL_UNSIGNED_BYTE;
        break;
    case PIXMAN_BE_x8r8g8b8:
    case PIXMAN_BE_a8r8g8b8:
        surface->glformat = GL_RGBA;
        surface->gltype = GL_UNSIGNED_BYTE;
        break;
    case PIXMAN_r5g6b5:
        surface->glformat = GL_RGB;
        surface->gltype = GL_UNSIGNED_SHORT_5_6_5;
        break;
    default:
        g_assert_not_reached();
    }

    glGenTextures(1, &surface->texture);
    glEnable(GL_TEXTURE_2D);
    glBindTexture(GL_TEXTURE_2D, surface->texture);
    glPixelStorei(GL_UNPACK_ROW_LENGTH_EXT,
                  surface_stride(surface) / surface_bytes_per_pixel(surface));
    glTexImage2D(GL_TEXTURE_2D, 0, GL_RGB,
                 surface_width(surface),
                 surface_height(surface),
                 0, surface->glformat, surface->gltype,
                 surface_data(surface));
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR);
    glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
}

static void xb_surface_gl_update_texture(DisplaySurface *surface, int x, int y, int w, int h)
{
    uint8_t *data = (void *)surface_data(surface);

    // assert(gls);

    glPixelStorei(GL_UNPACK_ROW_LENGTH_EXT,
                  surface_stride(surface) / surface_bytes_per_pixel(surface));
    glTexSubImage2D(GL_TEXTURE_2D, 0,
                    x, y, w, h,
                    surface->glformat, surface->gltype,
                    data + surface_stride(surface) * y
                    + surface_bytes_per_pixel(surface) * x);
}

static void xb_surface_gl_destroy_texture(DisplaySurface *surface)
{
    if (!surface || !surface->texture) {
        return;
    }
    glDeleteTextures(1, &surface->texture);
}

#endif // !USE_SHARED_CONTEXT

//
// Helper to determine if we should print some stats on a 1 second interval
//
#if 0
static int second_elapse(void)
{
    struct timeval tv_now, tv_since_last;

    gettimeofday(&tv_now, NULL);

    if (!tv_last_valid) {
        tv_last = tv_now;
        tv_last_valid = 1;
    }

    timersub(&tv_now, &tv_last, &tv_since_last);

    if (tv_since_last.tv_sec >= 1) {
        tv_last = tv_now;
        return 1;
    }
    return 0;
}
#endif
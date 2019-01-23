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

volatile int puller_cond;
volatile int pusher_cond;
volatile int fifo_access_cond;

typedef struct RAMHTEntry {
    uint32_t handle;
    hwaddr instance;
    enum FIFOEngine engine;
    unsigned int channel_id : 5;
    bool valid;
} RAMHTEntry;

static void pfifo_run_pusher(NV2AState *d);
static uint32_t ramht_hash(NV2AState *d, uint32_t handle);
static RAMHTEntry ramht_lookup(NV2AState *d, uint32_t handle);

/* PFIFO - MMIO and DMA FIFO submission to PGRAPH and VPE */
uint64_t pfifo_read(void *opaque, hwaddr addr, unsigned int size)
{
    NV2AState *d = (NV2AState *)opaque;

#if !USE_COROUTINES
    qemu_mutex_lock(&d->pfifo.lock);
#endif

    uint64_t r = 0;
    switch (addr) {
    case NV_PFIFO_INTR_0:
        r = d->pfifo.pending_interrupts;
        break;
    case NV_PFIFO_INTR_EN_0:
        r = d->pfifo.enabled_interrupts;
        break;
    case NV_PFIFO_RUNOUT_STATUS:
        r = NV_PFIFO_RUNOUT_STATUS_LOW_MARK; /* low mark empty */
        break;
    default:
        r = d->pfifo.regs[addr];
        break;
    }

#if !USE_COROUTINES
    qemu_mutex_unlock(&d->pfifo.lock);
#endif

    reg_log_read(NV_PFIFO, addr, r);
    return r;
}

void pfifo_write(void *opaque, hwaddr addr, uint64_t val, unsigned int size)
{
    NV2AState *d = (NV2AState *)opaque;

    reg_log_write(NV_PFIFO, addr, val);

#if !USE_COROUTINES
    qemu_mutex_lock(&d->pfifo.lock);
#endif

    switch (addr) {
    case NV_PFIFO_INTR_0:
        d->pfifo.pending_interrupts &= ~val;
        CRPRINTF("updating irq %s\n", __func__);
        update_irq(d);
        break;
    case NV_PFIFO_INTR_EN_0:
        d->pfifo.enabled_interrupts = val;
        CRPRINTF("updating irq %s\n", __func__);
        update_irq(d);
        break;
    default:
        d->pfifo.regs[addr] = val;
        break;
    }

#if USE_COROUTINES
    CRPRINTF("Signaling pusher and puller!\n");
    qemu_spin_lock(&d->pfifo.lock);
    pusher_cond = 1;
    puller_cond = 1;
    qemu_spin_unlock(&d->pfifo.lock);
#else
    qemu_cond_broadcast(&d->pfifo.pusher_cond);
    qemu_cond_broadcast(&d->pfifo.puller_cond);
#endif

#if !USE_COROUTINES
    qemu_mutex_unlock(&d->pfifo.lock);
#endif
}

static void pfifo_run_puller(NV2AState *d)
{
    uint32_t *pull0 = &d->pfifo.regs[NV_PFIFO_CACHE1_PULL0];
    uint32_t *pull1 = &d->pfifo.regs[NV_PFIFO_CACHE1_PULL1];
    uint32_t *engine_reg = &d->pfifo.regs[NV_PFIFO_CACHE1_ENGINE];

    uint32_t *status = &d->pfifo.regs[NV_PFIFO_CACHE1_STATUS];
    uint32_t *get_reg = &d->pfifo.regs[NV_PFIFO_CACHE1_GET];
    uint32_t *put_reg = &d->pfifo.regs[NV_PFIFO_CACHE1_PUT];

    // TODO
    // CacheEntry working_cache[NV2A_CACHE1_SIZE];
    // int working_cache_size = 0;
    // pull everything into our own queue

    // TODO think more about locking

    while (true) {
        if (!GET_MASK(*pull0, NV_PFIFO_CACHE1_PULL0_ACCESS)) return;

        /* empty cache1 */
        if (*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) break;

        uint32_t get = *get_reg;
        uint32_t put = *put_reg;

        assert(get < 128*4 && (get % 4) == 0);
        uint32_t method_entry = d->pfifo.regs[NV_PFIFO_CACHE1_METHOD + get*2];
        uint32_t parameter = d->pfifo.regs[NV_PFIFO_CACHE1_DATA + get*2];

        uint32_t new_get = (get+4) & 0x1fc;
        *get_reg = new_get;

        if (new_get == put) {
            // set low mark
            *status |= NV_PFIFO_CACHE1_STATUS_LOW_MARK;
        }
        if (*status & NV_PFIFO_CACHE1_STATUS_HIGH_MARK) {
            // unset high mark
            *status &= ~NV_PFIFO_CACHE1_STATUS_HIGH_MARK;
            // signal pusher
#if USE_COROUTINES
            CRPRINTF("puller is signaling pusher!\n");
            pusher_cond = 1;
            qemu_coroutine_yield();
#else
            qemu_cond_signal(&d->pfifo.pusher_cond);
#endif
        }


        uint32_t method = method_entry & 0x1FFC;
        uint32_t subchannel = GET_MASK(method_entry, NV_PFIFO_CACHE1_METHOD_SUBCHANNEL);

        // NV2A_DPRINTF("pull %d 0x%x 0x%x - subch %d\n", get/4, method_entry, parameter, subchannel);

        if (method == 0) {
            RAMHTEntry entry = ramht_lookup(d, parameter);
            assert(entry.valid);

            // assert(entry.channel_id == state->channel_id);

            assert(entry.engine == ENGINE_GRAPHICS);


            /* the engine is bound to the subchannel */
            assert(subchannel < 8);
            SET_MASK_SLOW(*engine_reg, 3 << (4*subchannel), entry.engine);
            SET_MASK_SLOW(*pull1, NV_PFIFO_CACHE1_PULL1_ENGINE, entry.engine);
            // NV2A_DPRINTF("engine_reg1 %d 0x%x\n", subchannel, *engine_reg);

#if !USE_COROUTINES
            // TODO: this is fucked
            qemu_mutex_lock(&d->pgraph.lock);
            //make pgraph busy
            qemu_mutex_unlock(&d->pfifo.lock);
#endif

            pgraph_context_switch(d, entry.channel_id);
            pgraph_wait_fifo_access(d);

            pgraph_method(d, subchannel, 0, entry.instance);

#if !USE_COROUTINES
            // make pgraph not busy
            qemu_mutex_unlock(&d->pgraph.lock);
            qemu_mutex_lock(&d->pfifo.lock);
#endif
        } else if (method >= 0x100) {
            // method passed to engine

            /* methods that take objects.
             * TODO: Check this range is correct for the nv2a */
            if (method >= 0x180 && method < 0x200) {
                //qemu_mutex_lock_iothread();
                RAMHTEntry entry = ramht_lookup(d, parameter);
                assert(entry.valid);
                // assert(entry.channel_id == state->channel_id);
                parameter = entry.instance;
                //qemu_mutex_unlock_iothread();
            }

            enum FIFOEngine engine = GET_MASK_SLOW(*engine_reg, 3 << (4*subchannel));
            // NV2A_DPRINTF("engine_reg2 %d 0x%x\n", subchannel, *engine_reg);
            assert(engine == ENGINE_GRAPHICS);
            SET_MASK(*pull1, NV_PFIFO_CACHE1_PULL1_ENGINE, engine);

#if !USE_COROUTINES
            // TODO: this is fucked
            qemu_mutex_lock(&d->pgraph.lock);
            //make pgraph busy
            qemu_mutex_unlock(&d->pfifo.lock);
#endif

            pgraph_wait_fifo_access(d);
            CRPRINTF("running method\n");
            pgraph_method(d, subchannel, method, parameter);

#if !USE_COROUTINES
            // make pgraph not busy
            qemu_mutex_unlock(&d->pgraph.lock);
            qemu_mutex_lock(&d->pfifo.lock);
#endif
        } else {
            assert(false);
        }

    }
}

#if USE_COROUTINES
static void coroutine_fn pfifo_puller_thread(void *arg)
#else
static void *pfifo_puller_thread(void *arg)
#endif
{
    NV2AState *d = (NV2AState *)arg;

    glo_set_current(d->pgraph.gl_context);

#if !USE_COROUTINES
    qemu_mutex_lock(&d->pfifo.lock);
#endif

    while (true) {
#if USE_COROUTINES
        CRPRINTF("running puller!\n");
        pfifo_run_puller(d);
        // while (!puller_cond && !d->exiting) {
        while (1) {
            int should_break = 0;

            qemu_spin_lock(&d->pfifo.lock);
            if (puller_cond) {
                should_break = 1;
                puller_cond = 0;
            }
            qemu_spin_unlock(&d->pfifo.lock);

            if (should_break) {
                CRPRINTF("puller got signal\n");
                break;
            } else {
                // CRPRINTF("puller is waiting!\n");
                qemu_coroutine_yield();
            }
        }
#else
        pfifo_run_puller(d);
        qemu_cond_wait(&d->pfifo.puller_cond, &d->pfifo.lock);
#endif
        if (d->exiting) {
            break;
        }
    }
#if !USE_COROUTINES
    qemu_mutex_unlock(&d->pfifo.lock);
    return NULL;
#endif
}

static void pfifo_run_pusher(NV2AState *d)
{
    uint32_t *push0 = &d->pfifo.regs[NV_PFIFO_CACHE1_PUSH0];
    uint32_t *push1 = &d->pfifo.regs[NV_PFIFO_CACHE1_PUSH1];
    uint32_t *dma_subroutine = &d->pfifo.regs[NV_PFIFO_CACHE1_DMA_SUBROUTINE];
    uint32_t *dma_state = &d->pfifo.regs[NV_PFIFO_CACHE1_DMA_STATE];
    uint32_t *dma_push = &d->pfifo.regs[NV_PFIFO_CACHE1_DMA_PUSH];
    uint32_t *dma_get = &d->pfifo.regs[NV_PFIFO_CACHE1_DMA_GET];
    uint32_t *dma_put = &d->pfifo.regs[NV_PFIFO_CACHE1_DMA_PUT];
    uint32_t *dma_dcount = &d->pfifo.regs[NV_PFIFO_CACHE1_DMA_DCOUNT];
    uint32_t *status = &d->pfifo.regs[NV_PFIFO_CACHE1_STATUS];
    uint32_t *get_reg = &d->pfifo.regs[NV_PFIFO_CACHE1_GET];
    uint32_t *put_reg = &d->pfifo.regs[NV_PFIFO_CACHE1_PUT];

    if (!GET_MASK(*push0, NV_PFIFO_CACHE1_PUSH0_ACCESS)) return;
    if (!GET_MASK(*dma_push, NV_PFIFO_CACHE1_DMA_PUSH_ACCESS)) return;

    /* suspended */
    if (GET_MASK(*dma_push, NV_PFIFO_CACHE1_DMA_PUSH_STATUS)) return;

    // TODO: should we become busy here??
    // NV_PFIFO_CACHE1_DMA_PUSH_STATE _BUSY

    unsigned int channel_id = GET_MASK(*push1,
                                       NV_PFIFO_CACHE1_PUSH1_CHID);


    /* Channel running DMA mode */
    uint32_t channel_modes = d->pfifo.regs[NV_PFIFO_MODE];
    assert(channel_modes & (1 << channel_id));

    assert(GET_MASK(*push1, NV_PFIFO_CACHE1_PUSH1_MODE)
            == NV_PFIFO_CACHE1_PUSH1_MODE_DMA);

    /* We're running so there should be no pending errors... */
    assert(GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR)
            == NV_PFIFO_CACHE1_DMA_STATE_ERROR_NONE);

    hwaddr dma_instance =
        GET_MASK(d->pfifo.regs[NV_PFIFO_CACHE1_DMA_INSTANCE],
                 NV_PFIFO_CACHE1_DMA_INSTANCE_ADDRESS) << 4;

    hwaddr dma_len;
    uint8_t *dma = nv_dma_map(d, dma_instance, &dma_len);

    while (true) {
        uint32_t dma_get_v = *dma_get;
        uint32_t dma_put_v = *dma_put;
        if (dma_get_v == dma_put_v) break;
        if (dma_get_v >= dma_len) {
            assert(false);
            SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR,
                     NV_PFIFO_CACHE1_DMA_STATE_ERROR_PROTECTION);
            break;
        }

        uint32_t word = ldl_le_p((uint32_t*)(dma + dma_get_v));
        dma_get_v += 4;

        uint32_t method_type =
            GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_TYPE);
        uint32_t method_subchannel =
            GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_SUBCHANNEL);
        uint32_t method =
            GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD) << 2;
        uint32_t method_count =
            GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_COUNT);

        uint32_t subroutine_state =
            GET_MASK(*dma_subroutine, NV_PFIFO_CACHE1_DMA_SUBROUTINE_STATE);

        if (method_count) {
            /* full */
            if (*status & NV_PFIFO_CACHE1_STATUS_HIGH_MARK) return;


            /* data word of methods command */
            d->pfifo.regs[NV_PFIFO_CACHE1_DMA_DATA_SHADOW] = word;

            uint32_t put = *put_reg;
            uint32_t get = *get_reg;

            assert((method & 3) == 0);
            uint32_t method_entry = 0;
            SET_MASK(method_entry, NV_PFIFO_CACHE1_METHOD_ADDRESS, method >> 2);
            SET_MASK(method_entry, NV_PFIFO_CACHE1_METHOD_TYPE, method_type);
            SET_MASK(method_entry, NV_PFIFO_CACHE1_METHOD_SUBCHANNEL, method_subchannel);

            // NV2A_DPRINTF("push %d 0x%x 0x%x - subch %d\n", put/4, method_entry, word, method_subchannel);

            assert(put < 128*4 && (put%4) == 0);
            d->pfifo.regs[NV_PFIFO_CACHE1_METHOD + put*2] = method_entry;
            d->pfifo.regs[NV_PFIFO_CACHE1_DATA + put*2] = word;

            uint32_t new_put = (put+4) & 0x1fc;
            *put_reg = new_put;
            if (new_put == get) {
                // set high mark
                *status |= NV_PFIFO_CACHE1_STATUS_HIGH_MARK;
            }
            if (*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) {
                // unset low mark
                *status &= ~NV_PFIFO_CACHE1_STATUS_LOW_MARK;
                // signal puller
#if USE_COROUTINES
                CRPRINTF("pusher signaling puller!\n");
                puller_cond = 1;
                qemu_coroutine_yield();
#else
                qemu_cond_signal(&d->pfifo.puller_cond);
#endif
            }

            if (method_type == NV_PFIFO_CACHE1_DMA_STATE_METHOD_TYPE_INC) {
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD,
                         (method + 4) >> 2);
            }
            SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_COUNT,
                     method_count - 1);
            (*dma_dcount)++;
        } else {
            /* no command active - this is the first word of a new one */
            d->pfifo.regs[NV_PFIFO_CACHE1_DMA_RSVD_SHADOW] = word;

            /* match all forms */
            if ((word & 0xe0000003) == 0x20000000) {
                /* old jump */
                d->pfifo.regs[NV_PFIFO_CACHE1_DMA_GET_JMP_SHADOW] =
                    dma_get_v;
                dma_get_v = word & 0x1fffffff;
                NV2A_DPRINTF("pb OLD_JMP 0x%x\n", dma_get_v);
            } else if ((word & 3) == 1) {
                /* jump */
                d->pfifo.regs[NV_PFIFO_CACHE1_DMA_GET_JMP_SHADOW] =
                    dma_get_v;
                dma_get_v = word & 0xfffffffc;
                NV2A_DPRINTF("pb JMP 0x%x\n", dma_get_v);
            } else if ((word & 3) == 2) {
                /* call */
                if (subroutine_state) {
                    SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR,
                             NV_PFIFO_CACHE1_DMA_STATE_ERROR_CALL);
                    break;
                } else {
                    *dma_subroutine = dma_get_v;
                    SET_MASK(*dma_subroutine,
                             NV_PFIFO_CACHE1_DMA_SUBROUTINE_STATE, 1);
                    dma_get_v = word & 0xfffffffc;
                    NV2A_DPRINTF("pb CALL 0x%x\n", dma_get_v);
                }
            } else if (word == 0x00020000) {
                /* return */
                if (!subroutine_state) {
                    SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR,
                             NV_PFIFO_CACHE1_DMA_STATE_ERROR_RETURN);
                    // break;
                } else {
                    dma_get_v = *dma_subroutine & 0xfffffffc;
                    SET_MASK(*dma_subroutine,
                             NV_PFIFO_CACHE1_DMA_SUBROUTINE_STATE, 0);
                    NV2A_DPRINTF("pb RET 0x%x\n", dma_get_v);
                }
            } else if ((word & 0xe0030003) == 0) {

#if FIFO_SHORTCUT
                //
                // Hacky shortcut to fast set transform constants:
                //
                const int hack_method = (word & 0x1fff);
                struct PGRAPHState *pg = &d->pgraph;
                if (hack_method == NV097_SET_TRANSFORM_CONSTANT) {

                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }
                    
                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    // Bulk upload these constants
                    int slot;
                    for (slot = 0; slot < hack_count; slot++) {
                        uint32_t param = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                        int const_load = GET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                                                  NV_PGRAPH_CHEOPS_OFFSET_CONST_LD_PTR);
                        assert(const_load < NV2A_VERTEXSHADER_CONSTANTS);
                        // VertexShaderConstant *constant = &pg->constants[const_load];
                        pg->vsh_constants_dirty[const_load] |=
                            (param != pg->vsh_constants[const_load][slot%4]);
                        pg->vsh_constants[const_load][slot%4] = param;
                        if (slot % 4 == 3) {
                            SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                                     NV_PGRAPH_CHEOPS_OFFSET_CONST_LD_PTR, const_load+1);
                        }
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }




                else if ((hack_method >= NV097_SET_VERTEX_DATA2F_M) && (hack_method <= (NV097_SET_VERTEX_DATA2F_M + 0x7c))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }
                    
                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_VERTEX_DATA2F_M)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t param = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                        unsigned int part = slot % 2;
                        slot /= 2;
                        VertexAttribute *attribute = &pg->vertex_attributes[slot];
                        pgraph_allocate_inline_buffer_vertices(pg, slot);
                        attribute->inline_value[part] = *(float*)&param;
                        /* FIXME: Should these really be set to 0.0 and 1.0 ? Conditions? */
                        attribute->inline_value[2] = 0.0;
                        attribute->inline_value[3] = 1.0;
                        if ((slot == 0) && (part == 1)) {
                            pgraph_finish_inline_buffer_vertex(pg);
                        }
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }




                else if ((hack_method >= NV097_SET_VERTEX_DATA4F_M) && (hack_method <= (NV097_SET_VERTEX_DATA4F_M + 0xfc))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }
                    
                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_VERTEX_DATA4F_M)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t param = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                        unsigned int part = slot % 4;
                        slot /= 4;
                        VertexAttribute *attribute = &pg->vertex_attributes[slot];
                        pgraph_allocate_inline_buffer_vertices(pg, slot);
                        attribute->inline_value[part] = *(float*)&param;
                        if ((slot == 0) && (part == 3)) {
                            pgraph_finish_inline_buffer_vertex(pg);
                        }
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }

                else if ((hack_method >= NV097_SET_VERTEX_DATA4UB) && (hack_method <= (NV097_SET_VERTEX_DATA4UB + 0x3c))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }
                    
                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_VERTEX_DATA4UB)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t parameter = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                    // case NV097_SET_VERTEX_DATA4UB ...
                    //         NV097_SET_VERTEX_DATA4UB + 0x3c: {
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
                    //     break;
                    // }
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }



                else if (hack_method == NV097_SET_TRANSFORM_CONSTANT_LOAD) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }

                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    uint32_t param = ldl_le_p((uint32_t*)(dma + dma_get_v));
                    //////////////////////////////////////////////////////////////////
                    // Copy-paste from pgraph.c
                    assert(param < NV2A_VERTEXSHADER_CONSTANTS);
                    SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                             NV_PGRAPH_CHEOPS_OFFSET_CONST_LD_PTR, param);
                    NV2A_DPRINTF("load to %d\n", param);
                    //////////////////////////////////////////////////////////////////
                    dma_get_v += 4;
                }

#if 0 // Assertion `program_load < NV2A_MAX_TRANSFORM_PROGRAM_LENGTH' failed.
                else if ((hack_method >= NV097_SET_TRANSFORM_PROGRAM) && (hack_method <= (NV097_SET_TRANSFORM_PROGRAM + 0x7c))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }

                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_TRANSFORM_PROGRAM)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t parameter = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                        int program_load = GET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                                                    NV_PGRAPH_CHEOPS_OFFSET_PROG_LD_PTR);

                        assert(program_load < NV2A_MAX_TRANSFORM_PROGRAM_LENGTH);
                        pg->program_data[program_load][slot%4] = parameter;

                        if (slot % 4 == 3) {
                            SET_MASK(pg->regs[NV_PGRAPH_CHEOPS_OFFSET],
                                     NV_PGRAPH_CHEOPS_OFFSET_PROG_LD_PTR, program_load+1);
                        }
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }
#endif



                else if ((hack_method >= NV097_SET_MODEL_VIEW_MATRIX) && (hack_method <= (NV097_SET_MODEL_VIEW_MATRIX + 0xfc))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }

                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_MODEL_VIEW_MATRIX)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t parameter = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                        unsigned int matnum = slot / 16;
                        unsigned int entry = slot % 16;
                        unsigned int row = NV_IGRAPH_XF_XFCTX_MMAT0 + matnum*8 + entry/4;
                        pg->vsh_constants[row][entry % 4] = parameter;
                        pg->vsh_constants_dirty[row] = true;
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }





                else if ((hack_method >= NV097_SET_COMPOSITE_MATRIX) && (hack_method <= (NV097_SET_COMPOSITE_MATRIX + 0x3c))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }

                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_COMPOSITE_MATRIX)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t parameter = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                        unsigned int row = NV_IGRAPH_XF_XFCTX_CMAT0 + slot/4;
                        pg->vsh_constants[row][slot%4] = parameter;
                        pg->vsh_constants_dirty[row] = true;
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }


                else if ((hack_method >= NV097_SET_VERTEX_DATA_ARRAY_OFFSET) && (hack_method <= (NV097_SET_VERTEX_DATA_ARRAY_OFFSET + 0x3c))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }

                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_VERTEX_DATA_ARRAY_OFFSET)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t parameter = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c
                    // case NV097_SET_VERTEX_DATA_ARRAY_OFFSET ...
                    //         NV097_SET_VERTEX_DATA_ARRAY_OFFSET + 0x3c:

                        // slot = (method - NV097_SET_VERTEX_DATA_ARRAY_OFFSET) / 4;

                        pg->vertex_attributes[slot].dma_select =
                            parameter & 0x80000000;
                        pg->vertex_attributes[slot].offset =
                            parameter & 0x7fffffff;

                        pg->vertex_attributes[slot].converted_elements = 0;
                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }

                else if ((hack_method >= NV097_SET_VERTEX_DATA_ARRAY_FORMAT) && (hack_method <= (NV097_SET_VERTEX_DATA_ARRAY_FORMAT + 0x3c))) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }

                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    int slot_base = (hack_method - NV097_SET_VERTEX_DATA_ARRAY_FORMAT)/4;

                    // Bulk upload these constants
                    int islot;
                    for (islot = 0; islot < hack_count; islot++) {
                        int slot = islot + slot_base;
                        uint32_t parameter = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        //////////////////////////////////////////////////////////////////
                        // Copy-paste from pgraph.c


                        // slot = (method - NV097_SET_VERTEX_DATA_ARRAY_FORMAT) / 4;
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

                        //////////////////////////////////////////////////////////////////
                        dma_get_v += 4;
                    }
                }


                else {
#endif

                /* increasing methods */
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD,
                         (word & 0x1fff) >> 2 );
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_SUBCHANNEL,
                         (word >> 13) & 7);
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_COUNT,
                         (word >> 18) & 0x7ff);
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_TYPE,
                         NV_PFIFO_CACHE1_DMA_STATE_METHOD_TYPE_INC);

#if FIFO_SHORTCUT
                }
#endif

                *dma_dcount = 0;
            } else if ((word & 0xe0030003) == 0x40000000) {
#if FIFO_SHORTCUT
                //
                // Hacky shortcut to fast-upload inline indices:
                //
                //   This is a very hot path! Let's bypass the fifo state machines
                //   for now and copy these elements over directly. Yields massive
                //   performance improvement in dashboard / Halo:CE.
                //
                const int hack_method = (word & 0x1fff);
                struct PGRAPHState *pg = &d->pgraph;
                if (hack_method == NV097_ARRAY_ELEMENT16) {
                    // Make sure all queued methods have been executed
                    while ((*status & NV_PFIFO_CACHE1_STATUS_LOW_MARK) == 0) {
                        // printf("waiting for puller to finish\n");
                        puller_cond = 1;
                        qemu_coroutine_yield();
                    }
                    uint32_t graphics_class = GET_MASK(pg->regs[NV_PGRAPH_CTX_SWITCH1],
                                                       NV_PGRAPH_CTX_SWITCH1_GRCLASS);
                    int hack_count = (word >> 18) & 0x7ff;
                    assert(graphics_class == NV_KELVIN_PRIMITIVE);
                    assert((dma_put_v-dma_get_v) >= (hack_count*4));

                    // printf("element 16 x %d!\n", hack_count);


                    // Bulk upload these inline indices
                    int count_index;
                    assert((pg->inline_elements_length+hack_count*2) < NV2A_MAX_BATCH_LENGTH);
                    for (count_index = 0; count_index < hack_count; count_index++) {
                        uint32_t param = ldl_le_p((uint32_t*)(dma + dma_get_v));
                        pg->inline_elements[pg->inline_elements_length++] = param & 0xFFFF;
                        pg->inline_elements[pg->inline_elements_length++] = param >> 16;
                        dma_get_v += 4;
                    }
                }
                else {
#endif
                /* non-increasing methods */
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD,
                         (word & 0x1fff) >> 2 );
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_SUBCHANNEL,
                         (word >> 13) & 7);
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_COUNT,
                         (word >> 18) & 0x7ff);
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_METHOD_TYPE,
                         NV_PFIFO_CACHE1_DMA_STATE_METHOD_TYPE_NON_INC);
#if FIFO_SHORTCUT
                }
#endif
                *dma_dcount = 0;
            } else {
                NV2A_DPRINTF("pb reserved cmd 0x%x - 0x%x\n",
                             dma_get_v, word);
                SET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR,
                         NV_PFIFO_CACHE1_DMA_STATE_ERROR_RESERVED_CMD);
                // break;
                assert(false);
            }
        }

        *dma_get = dma_get_v;

        if (GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR)) {
            break;
        }
    }

    // NV2A_DPRINTF("DMA pusher done: max 0x%" HWADDR_PRIx ", 0x%" HWADDR_PRIx " - 0x%" HWADDR_PRIx "\n",
    //      dma_len, control->dma_get, control->dma_put);

    uint32_t error = GET_MASK(*dma_state, NV_PFIFO_CACHE1_DMA_STATE_ERROR);
    if (error) {
        NV2A_DPRINTF("pb error: %d\n", error);
        assert(false);

        SET_MASK(*dma_push, NV_PFIFO_CACHE1_DMA_PUSH_STATUS, 1); /* suspended */

        // d->pfifo.pending_interrupts |= NV_PFIFO_INTR_0_DMA_PUSHER;
        // update_irq(d);
    }
}

#if USE_COROUTINES
static void coroutine_fn pfifo_pusher_thread(void *arg)
#else
static void *pfifo_pusher_thread(void *arg)
#endif
{
    NV2AState *d = (NV2AState *)arg;

#if !USE_COROUTINES
    qemu_mutex_lock(&d->pfifo.lock);
#endif
    while (true) {
#if USE_COROUTINES
        CRPRINTF("running pusher!\n");
        pfifo_run_pusher(d);
        while (1) {
            int should_break = 0;

            qemu_spin_lock(&d->pfifo.lock);
            if (pusher_cond) {
                should_break = 1;
                pusher_cond = 0;
            }
            qemu_spin_unlock(&d->pfifo.lock);

            if (should_break) {
                CRPRINTF("pusher got signal\n");
                break;
            } else {
                // CRPRINTF("pusher is waiting!\n");
                qemu_coroutine_yield();
            }
        }
#else
        pfifo_run_pusher(d);
        qemu_cond_wait(&d->pfifo.pusher_cond, &d->pfifo.lock);
#endif
        if (d->exiting) {
            break;
        }
    }
#if !USE_COROUTINES
    qemu_mutex_unlock(&d->pfifo.lock);
    return NULL;
#endif
}

#if USE_COROUTINES
static void* render_thread(void *arg)
{
    NV2AState *d = (NV2AState *)arg;

    Coroutine *puller, *pusher;

    pusher = qemu_coroutine_create(pfifo_pusher_thread, d);
    puller = qemu_coroutine_create(pfifo_puller_thread, d);

    // qemu_mutex_lock(&d->pfifo.lock);
    while (!d->exiting) {
        // CRPRINTF("LOOP %d\n", i++);
        // qemu_mutex_lock_iothread();
        qemu_coroutine_enter(pusher);
        qemu_coroutine_enter(puller);
        // qemu_mutex_unlock_iothread();
        // qemu_cond_wait(&d->pfifo.pusher_cond, &d->pfifo.lock);
    }

    // qemu_mutex_unlock(&d->pfifo.lock);
    return NULL;
}
#endif

static uint32_t ramht_hash(NV2AState *d, uint32_t handle)
{
    unsigned int ramht_size =
        1 << (GET_MASK(d->pfifo.regs[NV_PFIFO_RAMHT], NV_PFIFO_RAMHT_SIZE)+12);

    /* XXX: Think this is different to what nouveau calculates... */
    unsigned int bits = ctz32(ramht_size)-1;

    uint32_t hash = 0;
    while (handle) {
        hash ^= (handle & ((1 << bits) - 1));
        handle >>= bits;
    }

    unsigned int channel_id = GET_MASK(d->pfifo.regs[NV_PFIFO_CACHE1_PUSH1],
                                       NV_PFIFO_CACHE1_PUSH1_CHID);
    hash ^= channel_id << (bits - 4);

    return hash;
}


static RAMHTEntry ramht_lookup(NV2AState *d, uint32_t handle)
{
    hwaddr ramht_size =
        1 << (GET_MASK(d->pfifo.regs[NV_PFIFO_RAMHT], NV_PFIFO_RAMHT_SIZE)+12);

    uint32_t hash = ramht_hash(d, handle);
    assert(hash * 8 < ramht_size);

    hwaddr ramht_address =
        GET_MASK(d->pfifo.regs[NV_PFIFO_RAMHT],
                 NV_PFIFO_RAMHT_BASE_ADDRESS) << 12;

    assert(ramht_address + hash * 8 < memory_region_size(&d->ramin));

    uint8_t *entry_ptr = d->ramin_ptr + ramht_address + hash * 8;

    uint32_t entry_handle = ldl_le_p((uint32_t*)entry_ptr);
    uint32_t entry_context = ldl_le_p((uint32_t*)(entry_ptr + 4));

    return (RAMHTEntry){
        .handle = entry_handle,
        .instance = (entry_context & NV_RAMHT_INSTANCE) << 4,
        .engine = (entry_context & NV_RAMHT_ENGINE) >> 16,
        .channel_id = (entry_context & NV_RAMHT_CHID) >> 24,
        .valid = entry_context & NV_RAMHT_STATUS,
    };
}

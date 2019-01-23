XQEMU: perf-wip branch
======================

You are looking at my `perf-wip` branch. This branch has highly-experimental
work focused on dramatically improving the performance of XQEMU. Many of these
changes are not complete and may cause unexpected behavior, potentially
*crashing your system*. This branch is largely targeted at early adopters to
help in measuring impact of these changes in terms of performance gains and
overall application stability. Eventually these changes will grow up to become
PRs and integrated into XQEMU master, so *your* testing feedback is welcome
and encouraged. Reach out on Discord or IRC. Appveyor builds can be found
[here](https://ci.appveyor.com/project/mborgerson/xqemu).

Performance related changes:
- Elementary surface caching based on address/shape
- Surface-to-texture transfer
- SDL2-GL renderer, display using cached surfaces on flip
- Naive geometry buffer caching (re-using texture LRU cache)
- Coroutines to mitigate thread sync latency
- Split frag/vert/geom shaders into separable programs, reducing shader binds
- Improved (but still buggy) texture caching by using dirty bits
- Bulk uploads via fifo bypassing state machine
- Eliminate most runtime calls to ctz (still more to tackle)
- Other changes

Many of these changes can be tweaked in hw/xbox/nv2a/perf_config.h.

Non-performance related changes (fun tangents):
- 4x resolution scaling
- A couple of gfx bug fixes (cube maps, VERTEX_DATA2S)
- Final shaders to be run when displaying the framebuffer to support
  things like FXAA (currently disabled)
- Fix surface blit bug to display Xbox logo correctly

My primary testing games are Halo:CE and THPS2x, which both have seen
remarkable performance improvements with these changes (32+ FPS). Other games
might not fair so well, and some might be broken entirely. Please let me
know if this works well for you or if it horribly breaks things (but don't
blame me if your computer crashes!).

---

XQEMU is an open-source emulator to play original Xbox games on Windows, macOS,
and Linux. Please visit [xqemu.com](http://xqemu.com) to learn more.

Build Status
------------
| Windows | Linux | macOS |
| ------- | ----- | ----- |
| [![Build status](https://ci.appveyor.com/api/projects/status/8rbaimmbp6k44rab?svg=true)](https://ci.appveyor.com/project/mborgerson/xqemu-c5j6o) | [![Travis-CI Status](https://travis-ci.org/xqemu/xqemu.svg?branch=master)](https://travis-ci.org/xqemu/xqemu) | [![Travis-CI Status](https://travis-ci.org/xqemu/xqemu.svg?branch=master)](https://travis-ci.org/xqemu/xqemu) |

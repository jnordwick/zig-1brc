const std = @import("std");
const lin = std.os.linux;
const hm = std.hash_map;
const Allocator = std.mem.Allocator;

pub const cache_line_size = 64;
pub const nthreads = 12;
pub const ht_capacity = 10000;
pub const buf_writer_size = 1024 * 200;

pub fn map_file(fname: []const u8) ![]u8 {
    const file = try std.fs.cwd().openFile(fname, .{});
    const stat = try file.stat();
    const len = stat.size;
    const int_addr = lin.mmap(null, stat.size, lin.PROT.READ, .{ .TYPE = .PRIVATE, .POPULATE = true }, file.handle, 0);
    const addr: [*]u8 = @ptrFromInt(int_addr);
    const advret = lin.madvise(addr, stat.size, lin.MADV.WILLNEED | lin.MADV.HUGEPAGE);
    if (advret != 0)
        return error.MadvRet;
    return addr[0..len];
}

pub const Map = hm.HashMapUnmanaged([]const u8, Stat, Context, 50);

pub const PerThread = struct {
    const data_size = @sizeOf(Map) + 4 * ht_capacity;
    const _pad_size = b: {
        const t = cache_line_size - (data_size % cache_line_size);
        break :b if (t == 0) 0 else t;
    };

    t: std.Thread = undefined,
    m: Map = Map{},
    _pad: [_pad_size]u8 = [_]u8{0} ** _pad_size,

    pub fn init(this: @This()) void {
        for (0..ht_capacity) |n| {
            this.i[n] = n;
        }
    }
};

pub const Stat = struct {
    sum: i32 = 0,
    n: u32 = 0,
};

const Context = struct {
    pub fn hash(self: @This(), s: []const u8) u64 {
        _ = self;
        var h: u64 = 43029;
        for (s) |c| {
            h = (h * 65) ^ c;
        }
        return h;
    }
    pub fn eql(self: @This(), a: []const u8, b: []const u8) bool {
        _ = self;
        return std.mem.eql(u8, a, b);
    }
};

pub fn make_buffered_writer(under: anytype) std.io.BufferedWriter(buf_writer_size, @TypeOf(under)) {
    return .{ .unbuffered_writer = under };
}

pub fn parse_num(str: []const u8) i32 {
    var sign: i32 = 10;
    var p: usize = 0;
    var num: i32 = 0;
    if (str[0] == '-') {
        sign = -10;
        p = 1;
    }
    while (p < str.len) : (p += 1) {
        if (str[p] == '.')
            continue;
        const dig = str[p] - '0';
        num = num * 10 + dig;
    }
    return num * sign;
}

pub const ParsedLine = struct {
    name: []const u8,
    num: i32,
};

pub fn parse_line(span: []const u8, pos: u64, ret: *ParsedLine) u64 {
    const semi = std.mem.indexOfScalarPos(u8, span, pos, ';').?;
    const nl = std.mem.indexOfScalarPos(u8, span, semi + 1, '\n').?;

    ret.name = span[pos..semi];
    const dec = span[semi + 1 .. nl];
    ret.num = parse_num(dec);
    return nl + 1;
}

pub fn parse_span(span: []const u8, pt: *PerThread) void {
    var parse: ParsedLine = undefined;
    var p: u64 = 0;
    while (p < span.len) {
        p = parse_line(span, p, &parse);
        var r = pt.m.getOrPutAssumeCapacity(parse.name);
        if (!r.found_existing) {
            r.value_ptr.sum = parse.num;
            r.value_ptr.n = 1;
        } else {
            r.value_ptr.sum += parse.num;
            r.value_ptr.n += 1;
        }
    }
}

pub fn next_span(span: []const u8, offset: u64, len: u64) []const u8 {
    const end = offset + len;
    if (end >= span.len)
        return span[offset..span.len];
    var pos = end;
    while (span[pos] != '\n') : (pos += 1) {}
    pos += 1;
    return span[offset..pos];
}

pub fn spin_thread(pt: *PerThread, span: []const u8, alloc: Allocator) !void {
    try pt.m.ensureTotalCapacity(alloc, ht_capacity);
    pt.t = try std.Thread.spawn(.{}, parse_span, .{ span, pt });
}

// BUG: you can have such a small thread and too many threads where
// the same line gets indexed by two threads if they are given the same
// line to start on. the fix is to have spin_out find the new line after
// the offset calculation and only pass in offsets that start the next
// line. right now each threads find their own new line prior to the offset.
pub fn spin_out(comptime N: comptime_int, span: []const u8, alloc: Allocator) !Map {
    var maps = [_]PerThread{.{}} ** (N + 1);
    const size: u64 = 1 + span.len / N;
    var offset: u64 = 0;
    for (0..N) |n| {
        const nspan = next_span(span, offset, size);
        offset += nspan.len;
        try spin_thread(&maps[n], nspan, alloc);
    }
    std.debug.assert(offset == span.len);

    maps[0].t.join();
    var tlmap = &maps[0].m;

    for (1..N) |n| {
        maps[n].t.join();
        var it = maps[n].m.iterator();
        while (it.next()) |e| {
            var r = tlmap.getOrPutAssumeCapacity(e.key_ptr.*);
            if (!r.found_existing) {
                r.value_ptr.* = e.value_ptr.*;
            } else {
                r.value_ptr.sum += e.value_ptr.sum;
                r.value_ptr.n += e.value_ptr.n;
            }
        }
    }

    return maps[0].m;
}

pub fn slice_compare(ctx: u32, l: []const u8, r: []const u8) bool {
    _ = ctx;
    return std.mem.lessThan(u8, l, r);
}

// This function is ugly af, I know. Its 23:15, and I'm tired.
pub fn print_num(cnum: i32, buf: *[32]u8) ![]u8 {
    const is_neg = cnum < 0;
    const num: u32 = @intCast(if (is_neg) -cnum else cnum);
    var n: u32 = (num / 10) + @intFromBool(num % 10 >= 5);
    if (n == 0) {
        buf[29] = '0';
        buf[30] = '.';
        buf[31] = '0';
        return buf[29..];
    }
    var pos: u32 = buf.len - 1;
    buf[pos] = '0' + @as(u8, @intCast(n % 10));
    n /= 10;
    buf[pos - 1] = '.';
    if (n == 0) {
        buf[pos - 2] = '0';
        return buf[pos - 2 ..];
    }
    pos -= 2;
    while (n > 0) {
        buf[pos] = '0' + @as(u8, @intCast(n % 10));
        n /= 10;
        pos -= 1;
    }
    if (is_neg) {
        buf[pos] = '-';
    } else {
        pos += 1;
    }
    return buf[pos..];
}

pub fn print_stats(m: *const Map, ks: [][]const u8) !void {
    const stdout = std.io.getStdOut();
    var bout = make_buffered_writer(stdout.writer());
    var num_buf: [32]u8 = undefined;

    for (ks) |k| {
        const stat = m.get(k).?;
        const mean = @divTrunc(stat.sum, @as(i32, @intCast(stat.n)));
        const num = try print_num(mean, &num_buf);
        _ = try bout.write(k);
        _ = try bout.write(";");
        _ = try bout.write(num);
        _ = try bout.write("\n");
    }
    try bout.flush();
}

pub fn sort_map(m: *const Map, ks: *[ht_capacity][]const u8) u32 {
    var it = m.keyIterator();
    const sz = m.size;
    var i: u64 = 0;
    while (it.next()) |e| {
        ks[i] = e.*;
        i += 1;
    }
    const sl = ks[0..sz];
    std.mem.sort([]const u8, sl, @as(u32, 0), slice_compare);
    return sz;
}

pub fn main() !void {
    const ffname = "data/measurements.txt";
    var ks: [ht_capacity][]u8 = undefined;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const alloc = arena.allocator();

    const span = try map_file(ffname[0..]);
    const map = try spin_out(nthreads, span, alloc);
    const sz = sort_map(&map, &ks);
    try print_stats(&map, ks[0..sz]);
}

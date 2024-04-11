const std = @import("std");
const tt = std.testing;
const b = @import("main.zig");
const pp = std.debug.print;

test "mmap" {
    const ffname = "/home/jason/devel/zig-1brc/data/measurements.txt";
    const span = try b.map_file(ffname[0..]);
    pp("mapped {s} for {d} bytes\n", .{ ffname, span.len });
}

test "parse num" {
    try tt.expectEqual(@as(i32, 1230), b.parse_num(@as([]const u8, "12.3"[0..])));
    try tt.expectEqual(@as(i32, -1230), b.parse_num(@as([]const u8, "-12.3"[0..])));
    try tt.expectEqual(@as(i32, 200), b.parse_num(@as([]const u8, "2.0"[0..])));
    try tt.expectEqual(@as(i32, -230), b.parse_num(@as([]const u8, "-2.3"[0..])));
}

test "parseline" {
    const text: []const u8 = "one;3.4\ntwo;-12.3\nthree;0.1\n";
    var ret: b.ParsedLine = undefined;

    var p = b.parse_line(text, 0, &ret);
    try tt.expectEqualSlices(u8, @as([]const u8, "one"[0..]), ret.name);
    try tt.expectEqual(@as(i32, 340), ret.num);
    try tt.expectEqual(@as(u64, 8), p);

    p = b.parse_line(text, p, &ret);
    try tt.expectEqualSlices(u8, @as([]const u8, "two"[0..]), ret.name);
    try tt.expectEqual(@as(i32, -1230), ret.num);
    try tt.expectEqual(@as(u64, 18), p);

    p = b.parse_line(text, p, &ret);
    try tt.expectEqualSlices(u8, @as([]const u8, "three"[0..]), ret.name);
    try tt.expectEqual(@as(i32, 10), ret.num);
    try tt.expectEqual(@as(u64, text.len), p);
}

test "parse range" {
    const text: []const u8 = "zero;9.9\none;3.4\ntwo;-12.3\none;-0.1\n";
    var m = b.PerThread{};
    try m.m.ensureTotalCapacity(tt.allocator, b.ht_capacity);
    defer m.m.deinit(tt.allocator);
    b.parse_span(text, &m);
    const k1: []const u8 = "one";
    const k2: []const u8 = "two";
    const v1 = m.m.get(k1);
    const v2 = m.m.get(k2);
    try tt.expectEqual(b.Stat{ .sum = 330, .n = 2 }, v1);
    try tt.expectEqual(b.Stat{ .sum = -1230, .n = 1 }, v2);
}

test "spin_out" {
    const text: []const u8 = "one;1.2\nzero;9.9\none;3.4\ntwo;-12.3\none;-0.1\ntwo;-1.2\n";
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const alloc = arena.allocator();
    const m = try b.spin_out(2, text, alloc);

    const k0: []const u8 = "zero";
    const k1: []const u8 = "one";
    const k2: []const u8 = "two";
    const v0 = m.get(k0).?;
    const v1 = m.get(k1).?;
    const v2 = m.get(k2).?;
    try tt.expectEqual(b.Stat{ .sum = 990, .n = 1 }, v0);
    try tt.expectEqual(b.Stat{ .sum = 450, .n = 3 }, v1);
    try tt.expectEqual(b.Stat{ .sum = -1350, .n = 2 }, v2);
}

test "map and spin" {
    const file = "/home/jason/devel/zig-1brc/data/mill.txt";
    const text = try b.map_file(file[0..]);
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const alloc = arena.allocator();
    const m = try b.spin_out(16, text, alloc);

    var sum: u32 = 0;
    var it = m.iterator();
    while (it.next()) |e| {
        sum += e.value_ptr.n;
    }
    try tt.expectEqual(@as(u32, 1000000), sum);
}

test "sort map" {
    const text: []const u8 = "zero;9.9\none;3.4\ntwo;-12.3\none;-0.1\n";
    var ks: [b.ht_capacity][]u8 = undefined;
    var m = b.PerThread{};
    try m.m.ensureTotalCapacity(tt.allocator, b.ht_capacity);
    defer m.m.deinit(tt.allocator);
    b.parse_span(text, &m);
    const sz = b.sort_map(&m.m, &ks);
    const rs: [3][]const u8 = .{ "one", "two", "zero" };
    try tt.expectEqual(3, sz);
    for (0..rs.len) |i| {
        try tt.expectEqualSlices(u8, rs[i], ks[i]);
    }
}

test "print num" {
    var buf: [32]u8 = undefined;
    try tt.expectEqualSlices(u8, "12.3", try b.print_num(1234, &buf));
    try tt.expectEqualSlices(u8, "0.0", try b.print_num(3, &buf));
    try tt.expectEqualSlices(u8, "0.1", try b.print_num(5, &buf));
    try tt.expectEqualSlices(u8, "-8.3", try b.print_num(-825, &buf));
}

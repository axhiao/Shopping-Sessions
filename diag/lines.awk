# Input: `addr2line -a -i -f -C` output for every instruction address of the
# binary. Output: "<hex address> <key>" where key is the line code of the
# innermost frame in our sources (main.rs line; 10000 + pq.rs line; 20000 +
# diag.rs line; 30000 + prof.rs line) or, failing that, a short function name.
function flush() {
    if (addr != "") {
        if (code != "") print addr, code
        else print addr, name
    }
}
/^0x/ {
    flush()
    addr = substr($0, 3); sub(/^0+/, "", addr); if (addr == "") addr = "0"
    code = ""; name = ""; st = 0
    next
}
{
    if (st == 0) { fn = $0; st = 1; next }
    st = 0
    if (name == "") {
        n = fn
        gsub(/[<>]/, "", n); sub(/\(.*/, "", n)
        k = split(n, parts, "::")
        n = (k >= 2) ? parts[k - 1] "." parts[k] : parts[k]
        gsub(/[^A-Za-z0-9_.]/, "", n)
        name = substr(n, 1, 18)
        if (name == "") name = "x"
        if (name ~ /^[0-9]+$/) name = "f" name
    }
    if (code == "") {
        if (match($0, /src\/main\.rs:[0-9]+/)) code = substr($0, RSTART + 12, RLENGTH - 12) + 0
        else if (match($0, /src\/pq\.rs:[0-9]+/)) code = substr($0, RSTART + 10, RLENGTH - 10) + 10000
        else if (match($0, /src\/diag\.rs:[0-9]+/)) code = substr($0, RSTART + 12, RLENGTH - 12) + 20000
        else if (match($0, /src\/prof\.rs:[0-9]+/)) code = substr($0, RSTART + 12, RLENGTH - 12) + 30000
    }
}
END { flush() }
